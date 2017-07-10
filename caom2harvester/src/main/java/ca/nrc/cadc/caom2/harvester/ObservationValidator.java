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
            List<SkippedWrapperURI<Observation>> entityListSrc = null;
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
            log.info("************************** read tmpDstState = " + tmpDstState.size());

            tmpDst = destObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);
            log.info("************************** read tmpDst = " + tmpDst.size());

            if (!this.service)
            {
                tmpSrcState = srcObservationDAO.getObservationList(collection, startDate, end, batchSize + 1);
                tmpSrc = srcObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);
            }
            else
            {
                tmpSrcState = srcObservationService.getObservationList(collection, startDate, end, batchSize + 1);
                log.info("************************** read tmpSrcState = " + tmpSrcState.size());

                List<WorkerResponse> aux = srcObservationService.getList(collection, startDate, end, batchSize + 1);
                tmpSrc = new ArrayList<Observation>();
                for (WorkerResponse wr : aux)
                {
                    if (wr.getObservation() == null)
                        continue;
                    tmpSrc.add(wr.getObservation());
                }
                log.info("************************** read tmpSrc = " + tmpSrc.size());

            }

            // List<ObservationState> errlistState =
            // calculateErroneousObservationState(tmpSrcState, tmpDstState);
            List<Observation> errlist = calculateErroneousObservations(tmpSrcState, tmpDstState, tmpSrc, tmpDst);
            // log.info("************************** errlistState.size() = " +
            // errlistState.size());
            log.info("************************** errlist.size() = " + errlist.size());
            log.info("************************** tmpSrcState.size() = " + tmpSrcState.size());
            log.info("************************** tmpDstState.size() = " + tmpDstState.size());
            log.info("************************** tmpSrc.size() = " + tmpSrc.size());
            log.info("************************** tmpDst.size() = " + tmpDst.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            // entityListSrc = wrapState(tmpSrcState);
            entityListSrc = wrap(errlist);
            entityListSrcComplete = wrap(tmpSrc);
            if (entityListSrcComplete.size() > 0)
            {
                curLastModified = entityListSrcComplete.get(entityListSrcComplete.size() - 1).entity
                        .getMaxLastModified();
            }

            ret.found = tmpSrcState.size();
            log.info("found: " + tmpSrcState.size());

            ListIterator<SkippedWrapperURI<Observation>> iter = entityListSrc.listIterator();
            while (iter.hasNext())
            {
                SkippedWrapperURI<Observation> ow = iter.next();
                Observation o = ow.entity;
                iter.remove(); // allow garbage collection during loop

                String lastMsg = null;
                String skipMsg = null;

                try
                {
                    // o could be null in skip mode cleanup
                    if (!dryrun)
                    {
                        if (o != null)
                        {
                            // if (state != null) {
                            // state.curLastModified = o.getMaxLastModified();
                            // state.curID = o.getID();
                            // }

                            // if (hs == null) // success in redo mode
                            // harvestState.put(state);

                            skipMsg = o + ": " + lastMsg;
                            lastMsg = null;
                            // destObservationDAO.getTransactionManager().rollbackTransaction();
                            // log.warn("rollback: OK");
                            // tTransaction += System.currentTimeMillis() - t;

                            try
                            {
                                log.debug("starting HarvestSkipURI transaction");
                                boolean putSkip = true;
                                HarvestSkipURI skip = harvestSkip.get(source, cname, o.getURI().getURI());
                                if (skip == null)
                                {
                                    skip = new HarvestSkipURI(source, cname, o.getURI().getURI(), skipMsg);
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
                    lastMsg = oops.getMessage();
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
                                + o.getURI().getURI().toASCIIString());
                    }
                    else if (oops instanceof UncategorizedSQLException)
                    {
                        if (str.contains("spherepoly_from_array"))
                        {
                            log.error("UNDETECTED illegal polygon: " + o.getURI().getURI());
                        }
                        else
                            log.error("unexpected exception", oops);
                    }
                    else if (oops instanceof IllegalArgumentException && str.contains("CaomValidator")
                            && str.contains("keywords"))
                    {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " " + o.getURI().getURI().toASCIIString());
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

    private List<Observation> calculateErroneousObservations(List<ObservationState> tmpSrcState,
            List<ObservationState> tmpDstState, List<Observation> tmpSrc, List<Observation> tmpDst)
    {
        List<ObservationState> listErroneousState = new ArrayList<ObservationState>();
        List<ObservationState> listValidState = new ArrayList<ObservationState>();
        List<Observation> listErroneous = new ArrayList<Observation>();
        List<Observation> listValid = new ArrayList<Observation>();
        Collections.sort(tmpSrcState, cState);
        Collections.sort(tmpDstState, cState);
        Collections.sort(tmpSrc, c);
        Collections.sort(tmpDst, c);

        for (ObservationState osSrc : tmpSrcState)
        {
            log.info("SRC -> ObservationState (URI): " + osSrc.getURI().getURI().toString());
            log.info("SRC -> ObservationState (AccMetaChecksum): " + osSrc.accMetaChecksum);

            boolean found = false;
            for (ObservationState osDst : tmpDstState)
            {
                log.info("DST -> ObsetvationState (URI): " + osDst.getURI().getURI().toString());
                log.info("DST -> ObservationState (AccMetaChecksum): " + osDst.accMetaChecksum);

                if (!osSrc.getURI().getObservationID().equals(osDst.getURI().getObservationID()))
                    continue;
                found = true;
                // if (osSrc.accMetaChecksum != null && osDst.accMetaChecksum !=
                // null
                // && osSrc.accMetaChecksum.equals(osDst.accMetaChecksum))
                // {
                listValidState.add(osSrc);
                // break;
                // }
                // listErroneousState.add(osDst);
            }
            if (!found)
            {
                listErroneousState.add(osSrc);
            }
        }

        for (ObservationState err : listErroneousState)
        {
            for (Observation o : tmpSrc)
            {
                if (!err.getURI().getObservationID().equals(o.getObservationID()))
                    continue;
                listErroneous.add(o);
            }
            for (Observation o : tmpDst)
            {
                if (!err.getURI().getObservationID().equals(o.getObservationID()))
                    continue;
                if (!listErroneous.contains(o))
                    listErroneous.add(o);
            }
        }
        for (ObservationState val : listValidState)
        {
            for (Observation o : tmpSrc)
            {
                if (!val.getURI().getObservationID().equals(o.getObservationID()))
                    continue;
                listValid.add(o);
            }
            for (Observation o : tmpDst)
            {
                if (!val.getURI().getObservationID().equals(o.getObservationID()))
                    continue;
                if (!listValid.contains(o))
                    listValid.add(o);
            }

        }

        for (Observation oSrc : listValid)
        {
            boolean found = false;
            log.info("SRC -> Observation (URI): " + oSrc.getURI().getURI().toString());
            log.info("SRC -> Observation (AccMetaChecksum): " + oSrc.getAccMetaChecksum());

            for (Observation oDst : tmpDst)
            {
                log.info("DST -> Observation (URI): " + oDst.getURI().getURI().toString());
                log.info("DST -> Observation (AccMetaChecksum): " + oDst.getAccMetaChecksum());

                if (!oSrc.getURI().getObservationID().equals(oDst.getURI().getObservationID()))
                    continue;
                found = true;
                // if (oSrc.getAccMetaChecksum() != null &&
                // oDst.getAccMetaChecksum() == null
                // &&
                // oSrc.getAccMetaChecksum().equals(oDst.getAccMetaChecksum()))
                // break;
                // if (!listErroneous.contains(oDst))
                // listErroneous.add(oDst);
            }
            if (!found)
            {
                if (!listErroneous.contains(oSrc))
                    listErroneous.add(oSrc);
            }
        }

        return listErroneous;
    }

    // private List<ObservationState>
    // calculateErroneousObservationState(List<ObservationState> tmpSrcState,
    // List<ObservationState> tmpDstState)
    // {
    // List<ObservationState> errList = new ArrayList<ObservationState>();
    // for (ObservationState osSrc : tmpSrcState)
    // {
    // log.debug("SRC -> ObservationState (URI): " +
    // osSrc.getURI().getURI().toString());
    // log.debug("SRC -> ObservationState (AccMetaChecksum): " +
    // osSrc.accMetaChecksum);
    //
    // boolean found = false;
    // for (ObservationState osDst : tmpDstState)
    // {
    // log.debug("DST -> ObsetvationState (URI): " +
    // osDst.getURI().getURI().toString());
    // log.debug("DST -> ObservationState (AccMetaChecksum): " +
    // osDst.accMetaChecksum);
    //
    // if (!osSrc.getURI().getURI().equals(osDst.getURI().getURI()))
    // continue;
    // found = true;
    // // if (osSrc.accMetaChecksum == null || osDst.accMetaChecksum ==
    // // null
    // // || !osSrc.accMetaChecksum.equals(osDst.accMetaChecksum))
    // // errList.add(osDst);
    // break;
    // }
    // if (!found)
    // {
    // log.debug("SRC -> Observation: " + osSrc.getURI().getURI() + " not
    // found.");
    // errList.add(osSrc);
    // }
    // }
    //
    // return errList;
    //
    // }

    // private List<Observation>
    // calculateErroneousObservations(List<ObservationState> list)
    // {
    // List<Observation> erroneous = new ArrayList<Observation>();
    // for (ObservationState os : list)
    // {
    // WorkerResponse wr = srcObservationService.get(os.getURI());
    // if (wr.getObservation() != null)
    // {
    // erroneous.add(wr.getObservation());
    // }
    // }
    // return erroneous;
    //
    // }
    //
    // private List<ObservationState>
    // calculateErroneousObservationsState(List<ObservationState> tmpSrcState,
    // List<ObservationState> tmpDstState)
    // {
    // List<ObservationState> errList = new ArrayList<ObservationState>();
    // for (ObservationState osSrc : tmpSrcState)
    // {
    // log.debug("SRC -> ObservationState (URI): " +
    // osSrc.getURI().getURI().toString());
    // log.debug("SRC -> ObservationState (AccMetaChecksum): " +
    // osSrc.accMetaChecksum);
    //
    // boolean found = false;
    // for (ObservationState osDst : tmpDstState)
    // {
    // log.debug("DST -> ObsetvationState (URI): " +
    // osDst.getURI().getURI().toString());
    // log.debug("DST -> ObservationState (AccMetaChecksum): " +
    // osDst.accMetaChecksum);
    //
    // if (!osSrc.getURI().getURI().equals(osDst.getURI().getURI()))
    // continue;
    // found = true;
    // if (osSrc.accMetaChecksum == null || osDst.accMetaChecksum == null
    // || !osSrc.accMetaChecksum.equals(osDst.accMetaChecksum))
    // errList.add(osDst);
    // break;
    // }
    // if (!found)
    // {
    // errList.add(osSrc);
    // }
    // }
    //
    // return errList;
    // }
    //
    // private void detectLoop(List<SkippedWrapperURI<Observation>> entityList)
    // {
    // if (entityList.size() < 2)
    // return;
    // SkippedWrapperURI<Observation> start = entityList.get(0);
    // SkippedWrapperURI<Observation> end = entityList.get(entityList.size() -
    // 1);
    //
    // if
    // (start.entity.getMaxLastModified().equals(end.entity.getMaxLastModified()))
    // {
    // throw new RuntimeException("detected infinite harvesting loop: " +
    // entityClass.getSimpleName() + " at "
    // + format(start.entity.getMaxLastModified()));
    // }
    // }
    //
    // private void detectLoopState(List<SkippedWrapperURI<ObservationState>>
    // entityList)
    // {
    // if (entityList.size() < 2)
    // return;
    // SkippedWrapperURI<ObservationState> start = entityList.get(0);
    // SkippedWrapperURI<ObservationState> end =
    // entityList.get(entityList.size() - 1);
    //
    // if (start.entity.maxLastModified.equals(end.entity.maxLastModified))
    // {
    // throw new RuntimeException("detected infinite harvesting loop: " +
    // entityClass.getSimpleName() + " at "
    // + format(start.entity.maxLastModified));
    // }
    // }
    //
    // private List<SkippedWrapperURI<ObservationState>>
    // wrapState(List<ObservationState> obsList)
    // {
    // List<SkippedWrapperURI<ObservationState>> ret = new
    // ArrayList<SkippedWrapperURI<ObservationState>>(
    // obsList.size());
    // for (ObservationState o : obsList)
    // {
    // ret.add(new SkippedWrapperURI<ObservationState>(o, null));
    // }
    // return ret;
    // }

    private List<SkippedWrapperURI<Observation>> wrap(List<Observation> obsList)
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