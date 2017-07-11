package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.repo.client.WorkerResponse;

/**
 *
 * @author pdowler
 */
public class ObservationValidator extends Harvester
{

    private static Logger log = Logger.getLogger(ObservationValidator.class);

    private boolean service = false;

    private RepoClient srcObservationService;
    private DatabaseObservationDAO srcObservationDAO;
    private DatabaseObservationDAO destObservationDAO;

    private Date maxDate;
    HarvestSkipURIDAO harvestSkip = null;

    public ObservationValidator(String resourceId, String collection, int nthreads, String[] dest, Integer batchSize,
            boolean full, boolean dryrun) throws IOException, URISyntaxException
    {
        super(Observation.class, null, dest, batchSize, full, dryrun);
        init(resourceId, collection, nthreads);
    }

    public ObservationValidator(String[] src, String[] dest, Integer batchSize, boolean full, boolean dryrun)
            throws IOException, URISyntaxException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        init();
    }

    public void setInteractive(boolean enabled)
    {
    }

    public void setMaxDate(Date maxDate)
    {
        this.maxDate = maxDate;
    }

    public void setDoCollisionCheck(boolean doCollisionCheck)
    {
    }

    private void init() throws IOException, URISyntaxException
    {
        Map<String, Object> config1 = getConfigDAO(src);
        Map<String, Object> config2 = getConfigDAO(dest);
        this.srcObservationDAO = new DatabaseObservationDAO();
        srcObservationDAO.setConfig(config1);
        this.destObservationDAO = new DatabaseObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    private void init(String uri, String collection, int threads) throws IOException, URISyntaxException
    {
        this.collection = collection;
        this.resourceId = uri;
        this.service = true;
        Map<String, Object> config2 = getConfigDAO(dest);
        this.srcObservationService = new RepoClient(new URI(uri), threads);
        this.destObservationDAO = new DatabaseObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    @Override
    public void run()
    {
        log.info("START VALIDATION: " + Observation.class.getSimpleName());

        boolean go = true;
        while (go)
        {
            Progress num = doit();

            if (num.found > 0)
                log.info("***************** finished batch: " + num + " *******************");

            if (num.abort)
                log.error("batched aborted");
            go = (num.found > 0 && !num.abort && !num.done);
            if (batchSize != null && num.found < batchSize.intValue() / 2)
                go = false;
            full = false; // do not start at beginning again
            if (dryrun)
                go = false; // no state update -> infinite loop
        }

        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    private static class Progress
    {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int ingested = 0;
        int failed = 0;

        @Override
        public String toString()
        {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private Date startDate;

    private Date curLastModified = null;

    private Progress doit()
    {
        Progress ret = new Progress();

        long t = System.currentTimeMillis();
        long tState = -1;
        long tQuery = -1;
        long tTransaction = -1;

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null)
            expectedNum = batchSize.intValue();
        try
        {
            System.gc(); // hint
            t = System.currentTimeMillis();

            log.info("**************** state = " + curLastModified + " source = " + source + " )");
            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            startDate = curLastModified;

            Date end = maxDate;
            List<SkippedWrapperURI<ObservationError>> entityListSrc = null;
            List<SkippedWrapperURI<Observation>> entityListSrcComplete = null;

            Date fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60000L); // 5
                                                                                 // minutes
                                                                                 // ago;
            if (end == null)
                end = fiveMinAgo;
            else
            {
                log.info("harvest limit: min( " + format(fiveMinAgo) + " " + format(end) + " )");
                if (end.getTime() > fiveMinAgo.getTime())
                    end = fiveMinAgo;
            }

            log.info("harvest window: " + format(startDate) + " :: " + format(end) + " [" + batchSize + "]");

            List<ObservationState> tmpSrcState = null;
            List<ObservationState> tmpDstState = null;
            List<Observation> tmpSrc = null;
            List<Observation> tmpDst = null;

            tmpDstState = destObservationDAO.getObservationList(collection, startDate, end, batchSize + 1);
            tmpDst = destObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);

            if (!this.service)
            {
                tmpSrcState = srcObservationDAO.getObservationList(collection, startDate, end, batchSize + 1);
                tmpSrc = srcObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);
            }
            else
            {
                tmpSrcState = srcObservationService.getObservationList(collection, startDate, end, batchSize + 1);

                List<WorkerResponse> aux = srcObservationService.getList(collection, startDate, end, batchSize + 1);
                tmpSrc = new ArrayList<Observation>();
                for (WorkerResponse wr : aux)
                {
                    if (wr.getObservation() == null)
                        continue;
                    tmpSrc.add(wr.getObservation());
                }

            }

            List<Observation> copy = new ArrayList<Observation>();
            copy.addAll(tmpSrc);

            List<ObservationError> errlist = calculateErroneousObservations(tmpSrcState, tmpDstState, tmpSrc, tmpDst);
            // log.info("************************** errlistState.size() = " +
            // errlistState.size());
            log.info("************************** errlist.size() = " + errlist.size());
            // log.info("************************** tmpSrcState.size() = " +
            // tmpSrcState.size());
            // log.info("************************** tmpDstState.size() = " +
            // tmpDstState.size());
            // log.info("************************** tmpSrc.size() = " +
            // tmpSrc.size());
            // log.info("************************** tmpDst.size() = " +
            // tmpDst.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            entityListSrc = wrap(errlist);

            entityListSrcComplete = wrapObservation(copy);
            if (entityListSrcComplete.size() > 0)
            {
                curLastModified = entityListSrcComplete.get(entityListSrcComplete.size() - 1).entity
                        .getMaxLastModified();
            }

            ret.found = copy.size();
            log.info("found: " + copy.size());

            ListIterator<SkippedWrapperURI<ObservationError>> iter = entityListSrc.listIterator();
            while (iter.hasNext())
            {
                SkippedWrapperURI<ObservationError> ow = iter.next();
                ObservationError o = ow.entity;
                iter.remove(); // allow garbage collection during loop

                String skipMsg = null;

                try
                {
                    // o could be null in skip mode cleanup
                    if (!dryrun)
                    {
                        if (o != null)
                        {
                            skipMsg = o.toString();// + ": " + o.getError();
                            try
                            {
                                log.debug("starting HarvestSkipURI transaction");
                                boolean putSkip = true;
                                HarvestSkipURI skip = harvestSkip.get(source, cname, o.getObs().getURI().getURI());
                                if (skip == null)
                                {
                                    skip = new HarvestSkipURI(source, cname, o.getObs().getURI().getURI(), skipMsg);
                                }
                                else
                                {
                                    if (skipMsg != null && !skipMsg.equals(skip.errorMessage))
                                    {
                                        skip.errorMessage = skipMsg; // possible
                                                                     // update
                                    }
                                    else
                                    {
                                        putSkip = false; // avoid timestamp
                                                         // update
                                    }
                                }

                                if (destObservationDAO.getTransactionManager().isOpen())
                                    throw new RuntimeException(
                                            "BUG: found open trasnaction at start of next observation");
                                log.debug("starting transaction");
                                destObservationDAO.getTransactionManager().startTransaction();

                                // track the fail
                                if (putSkip)
                                {
                                    log.info("put: " + skip);
                                    harvestSkip.put(skip);
                                }

                            }
                            catch (Throwable oops)
                            {
                                log.warn("failed to insert HarvestSkipURI", oops);
                                destObservationDAO.getTransactionManager().rollbackTransaction();
                                log.warn("rollback HarvestSkipURI: OK");
                                ret.abort = true;
                            }
                            ret.failed++;
                        }

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ret.ingested++;
                }
                catch (Throwable oops)
                {
                    String str = oops.toString();
                    if (oops instanceof Error)
                    {
                        log.error("FATAL - probably installation or environment", oops);
                        ret.abort = true;
                    }
                    else if (oops instanceof NullPointerException)
                    {
                        log.error("BUG", oops);
                        ret.abort = true;
                    }
                    else if (oops instanceof BadSqlGrammarException)
                    {
                        log.error("BUG", oops);
                        BadSqlGrammarException bad = (BadSqlGrammarException) oops;
                        SQLException sex1 = bad.getSQLException();

                        if (sex1 != null)
                        {
                            log.error("CAUSE", sex1);
                            SQLException sex2 = sex1.getNextException();
                            log.error("NEXT CAUSE", sex2);
                        }
                        ret.abort = true;
                    }
                    else if (oops instanceof DataAccessResourceFailureException)
                    {
                        log.error("SEVERE PROBLEM - probably out of space in database", oops);
                        ret.abort = true;
                    }
                    else if (oops instanceof DataIntegrityViolationException
                            && str.contains("duplicate key value violates unique constraint \"i_observationuri\""))
                    {
                        log.error("CONTENT PROBLEM - duplicate observation: " + " "
                                + o.getObs().getURI().getURI().toASCIIString());
                    }
                    else if (oops instanceof UncategorizedSQLException)
                    {
                        if (str.contains("spherepoly_from_array"))
                        {
                            log.error("UNDETECTED illegal polygon: " + o.getObs().getURI().getURI());
                        }
                        else
                            log.error("unexpected exception", oops);
                    }
                    else if (oops instanceof IllegalArgumentException && str.contains("CaomValidator")
                            && str.contains("keywords"))
                    {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " "
                                + o.getObs().getURI().getURI().toASCIIString());
                    }
                    else
                        log.error("unexpected exception", oops);
                }
                finally
                {
                }
                if (ret.abort)
                    return ret;
            }
            if (ret.found < expectedNum)
                ret.done = true;
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        finally
        {
            tTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + tState + "ms");
            log.debug("time to run ObservationListQuery: " + tQuery + "ms");
            log.debug("time to run transactions: " + tTransaction + "ms");
        }
        return ret;
    }

    Comparator<ObservationState> cState = new Comparator<ObservationState>()
    {
        @Override
        public int compare(ObservationState o1, ObservationState o2)
        {
            return o1.getURI().getObservationID().compareTo(o2.getURI().getObservationID());
        }

    };
    Comparator<Observation> c = new Comparator<Observation>()
    {
        @Override
        public int compare(Observation o1, Observation o2)
        {
            return o1.getURI().getObservationID().compareTo(o2.getURI().getObservationID());
        }

    };

    private List<ObservationError> calculateErroneousObservations(List<ObservationState> tmpSrcState,
            List<ObservationState> tmpDstState, List<Observation> tmpSrc, List<Observation> tmpDst)
    {
        List<ObservationError> listErroneous = new ArrayList<ObservationError>();
        Collections.sort(tmpSrcState, cState);
        Collections.sort(tmpDstState, cState);
        Collections.sort(tmpSrc, c);
        Collections.sort(tmpDst, c);

        List<Observation> copyTmpSrc = new ArrayList<Observation>();
        copyTmpSrc.addAll(tmpSrc);
        List<ObservationState> copyTmpSrcState = new ArrayList<ObservationState>();
        copyTmpSrcState.addAll(tmpSrcState);
        List<Observation> copyTmpDst = new ArrayList<Observation>();
        copyTmpDst.addAll(tmpDst);
        List<ObservationState> copyTmpDstState = new ArrayList<ObservationState>();
        copyTmpDstState.addAll(tmpDstState);

        for (int i = 0; i < tmpSrcState.size(); i++)
        {
            ObservationState osSrc = tmpSrcState.get(i);
            Observation oSrc = tmpSrc.get(i);

            if (!osSrc.getURI().getObservationID().equals(oSrc.getURI().getObservationID()))
            {
                log.debug("WRONG (not found): " + osSrc.getURI().getObservationID());
                ObservationError oe = new ObservationError(oSrc, "Observation " + oSrc.getObservationID()
                        + " not found in repo but present as ObservationState");
                listErroneous.add(oe);
                tmpSrcState.remove(osSrc);
                if (i >= 0)
                {
                    i--;
                }
                continue;
            }
            else
            {
                if (!(osSrc.accMetaChecksum != null && oSrc.getAccMetaChecksum() != null
                        && osSrc.accMetaChecksum.equals(oSrc.getAccMetaChecksum())))
                {
                    log.debug("WRONG MD5: " + osSrc.getURI().getObservationID());
                    ObservationError oe = new ObservationError(oSrc, "Observation " + oSrc.getObservationID()
                            + " has different accumulated checksum in ObservationState and in Observation");
                    listErroneous.add(oe);
                    tmpSrcState.remove(osSrc);
                    if (i >= 0)
                    {
                        i--;
                    }
                    continue;
                }
                else
                {
                    log.debug("CORRECT MD5: " + osSrc.getURI().getObservationID());
                    tmpSrcState.remove(osSrc);
                    tmpSrc.remove(oSrc);
                    if (i >= 0)
                    {
                        i--;
                    }
                }
            }
        }

        tmpSrc.clear();
        tmpSrc.addAll(copyTmpSrc);
        tmpSrcState.clear();
        tmpSrcState.addAll(copyTmpSrcState);

        for (int i = 0; i < tmpSrc.size(); i++)
        {
            Observation oSrc = tmpSrc.get(i);
            Observation oDst = tmpDst.get(i);

            if (!oSrc.getURI().getObservationID().equals(oDst.getURI().getObservationID()))
            {
                log.debug("WRONG (not found): " + oSrc.getURI().getObservationID());
                ObservationError oe = new ObservationError(oSrc, "Observation " + oSrc.getObservationID()
                        + " present in repo but not found in destination data base");
                listErroneous.add(oe);
                tmpSrc.remove(oSrc);
                if (i >= 0)
                {
                    i--;
                }
                continue;
            }
            else
            {
                if (!(oSrc.getAccMetaChecksum() != null && oDst.getAccMetaChecksum() != null
                        && oSrc.getAccMetaChecksum().equals(oDst.getAccMetaChecksum())))
                {
                    log.debug("WRONG MD5: " + oSrc.getURI().getObservationID());
                    ObservationError oe = new ObservationError(oSrc, "Observation " + oSrc.getObservationID()
                            + " has different accumulated checksum in repo and in destination database");
                    listErroneous.add(oe);
                    tmpSrc.remove(oSrc);
                    if (i >= 0)
                    {
                        i--;
                    }
                    continue;
                }
                else
                {
                    log.debug("CORRECT MD5: " + oSrc.getURI().getObservationID());
                    tmpSrc.remove(oSrc);
                    tmpDst.remove(oDst);
                    if (i >= 0)
                    {
                        i--;
                    }
                }
            }
        }

        tmpSrc.clear();
        tmpSrc.addAll(copyTmpSrc);
        tmpDst.clear();
        tmpDst.addAll(copyTmpDst);

        return listErroneous;
    }

    private List<SkippedWrapperURI<ObservationError>> wrap(List<ObservationError> obsList)
    {
        List<SkippedWrapperURI<ObservationError>> ret = new ArrayList<SkippedWrapperURI<ObservationError>>(
                obsList.size());
        for (ObservationError o : obsList)
        {
            ret.add(new SkippedWrapperURI<ObservationError>(o, null));
        }
        return ret;
    }
    private List<SkippedWrapperURI<Observation>> wrapObservation(List<Observation> obsList)
    {
        List<SkippedWrapperURI<Observation>> ret = new ArrayList<SkippedWrapperURI<Observation>>(obsList.size());
        for (Observation o : obsList)
        {
            ret.add(new SkippedWrapperURI<Observation>(o, null));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, @SuppressWarnings("rawtypes") Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2], batchSize);
    }
}