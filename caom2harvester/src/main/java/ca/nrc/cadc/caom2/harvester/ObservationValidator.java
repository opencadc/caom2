package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
    private boolean computePlaneMetadata = false;

    public ObservationValidator(String resourceId, String collection, int nthreads, String[] dest, Integer batchSize, boolean full, boolean dryrun)
            throws IOException, URISyntaxException
    {
        super(Observation.class, null, dest, batchSize, full, dryrun);
        init(resourceId, collection, nthreads);
    }

    public ObservationValidator(String[] src, String[] dest, Integer batchSize, boolean full, boolean dryrun) throws IOException, URISyntaxException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        init();
    }

    public void setMaxDate(Date maxDate)
    {
        this.maxDate = maxDate;
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

        Progress num = doit();

        if (num.found > 0)
            log.info("***************** finished batch: " + num + " *******************");

        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    private static class Progress
    {
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

        try
        {
            System.gc(); // hint
            t = System.currentTimeMillis();

            log.info("**************** state = " + curLastModified + " source = " + source + " )");
            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            startDate = curLastModified;

            Date end = maxDate;
            List<SkippedWrapperURI<ObservationStateError>> entityListSrc = null;

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

            log.info("harvest window: " + format(startDate) + " :: " + format(end));

            List<ObservationState> tmpSrcState = null;
            List<ObservationState> tmpDstState = null;

            tmpDstState = destObservationDAO.getObservationList(collection, null, null, null);

            if (!this.service)
            {
                tmpSrcState = srcObservationDAO.getObservationList(collection, null, null, null);
            }
            else
            {
                tmpSrcState = srcObservationService.getObservationList(collection, null, null, null);
            }

            Set<ObservationState> srcState = new TreeSet<>(cStateUri);
            srcState.addAll(tmpSrcState);
            tmpSrcState.clear();
            Set<ObservationState> dstState = new TreeSet<>(cStateUri);
            dstState.addAll(tmpDstState);
            tmpDstState.clear();

            Set<ObservationStateError> errlist = calculateErroneousObservationStates(srcState, dstState);

            log.info("************************** errlist.size() = " + errlist.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            entityListSrc = wrap(errlist);

            ret.found = srcState.size();
            log.info("found: " + srcState.size());

            ListIterator<SkippedWrapperURI<ObservationStateError>> iter = entityListSrc.listIterator();
            while (iter.hasNext())
            {
                SkippedWrapperURI<ObservationStateError> ow = iter.next();
                ObservationStateError o = ow.entity;
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
                                    throw new RuntimeException("BUG: found open trasnaction at start of next observation");
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
                    }
                    else if (oops instanceof NullPointerException)
                    {
                        log.error("BUG", oops);
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
                    }
                    else if (oops instanceof DataAccessResourceFailureException)
                    {
                        log.error("SEVERE PROBLEM - probably out of space in database", oops);
                    }
                    else if (oops instanceof DataIntegrityViolationException && str.contains("duplicate key value violates unique constraint \"i_observationuri\""))
                    {
                        log.error("CONTENT PROBLEM - duplicate observation: " + " " + o.getObs().getURI().getURI().toASCIIString());
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
                    else if (oops instanceof IllegalArgumentException && str.contains("CaomValidator") && str.contains("keywords"))
                    {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " " + o.getObs().getURI().getURI().toASCIIString());
                    }
                    else
                        log.error("unexpected exception", oops);
                }
                finally
                {
                }
            }
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

    private Set<ObservationStateError> calculateErroneousObservationStates(Set<ObservationState> srcState, Set<ObservationState> dstState)
    {
        Set<ObservationStateError> listErroneous = new TreeSet<ObservationStateError>(cError);
        Set<ObservationState> listCorrect = new TreeSet<ObservationState>(cStateSum);
        Iterator<ObservationState> iterSrc = srcState.iterator();
        Iterator<ObservationState> iterDst = dstState.iterator();
        while (iterSrc.hasNext())
        {
            ObservationState os = iterSrc.next();
            if (!dstState.contains(os))
            {
                ObservationStateError ose = new ObservationStateError(os, "missed harvest");
                log.info("************************ adding missed harvest: " + ose.getObs().getURI());
                listErroneous.add(ose);
            }
            else
                listCorrect.add(os);
        }
        while (iterDst.hasNext())
        {
            ObservationState os = iterDst.next();
            if (!srcState.contains(os))
            {
                ObservationStateError ose = new ObservationStateError(os, "missed deletion");
                log.info("************************ adding missed deletion: " + os.getURI());
                if (!listErroneous.contains(ose))
                    listErroneous.add(ose);
            }
            else if (!listCorrect.contains(os))// ObservationState is in both
            // lists. Here we check checksums
            // thanks to the comparator
            {
                ObservationStateError ose = new ObservationStateError(os, "computation or serialization bug");
                log.info("************************ adding computation or serialization bug: " + os.getURI());
                if (!listErroneous.contains(ose))
                    listErroneous.add(ose);
            }
        }

        return listErroneous;
    }

    Comparator<ObservationState> cStateUri = new Comparator<ObservationState>()
    {
        @Override
        public int compare(ObservationState o1, ObservationState o2)
        {
            return o1.getURI().compareTo(o2.getURI());
        }

    };
    Comparator<ObservationState> cStateSum = new Comparator<ObservationState>()
    {
        @Override
        public int compare(ObservationState o1, ObservationState o2)
        {
            int c1 = o1.getURI().compareTo(o2.getURI());
            if (c1 != 0)
                return c1; // different observations

            if (o1.accMetaChecksum == null || o2.accMetaChecksum == null)
                return 0; // cannot compare

            return o1.accMetaChecksum.compareTo(o2.accMetaChecksum);
        }

    };

    Comparator<ObservationStateError> cError = new Comparator<ObservationStateError>()
    {
        @Override
        public int compare(ObservationStateError o1, ObservationStateError o2)
        {
            return o1.getObs().getURI().compareTo(o2.getObs().getURI());
        }

    };

    private List<SkippedWrapperURI<ObservationStateError>> wrap(Set<ObservationStateError> errlist)
    {
        List<SkippedWrapperURI<ObservationStateError>> ret = new ArrayList<SkippedWrapperURI<ObservationStateError>>(errlist.size());
        for (ObservationStateError o : errlist)
        {
            ret.add(new SkippedWrapperURI<ObservationStateError>(o, null));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, @SuppressWarnings("rawtypes") Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2], batchSize);
    }

    public void setComputePlaneMetadata(boolean computePlaneMetadata)
    {
        this.computePlaneMetadata = computePlaneMetadata;
    }
    public boolean getComputePlaneMetadata()
    {
        return this.computePlaneMetadata;
    }
}