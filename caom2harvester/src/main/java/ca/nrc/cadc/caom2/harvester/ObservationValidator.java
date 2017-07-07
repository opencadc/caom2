package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

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
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;

/**
 *
 * @author pdowler
 */
public class ObservationValidator extends Harvester {

    private static Logger log = Logger.getLogger(ObservationValidator.class);

    private boolean service = false;

    private RepoClient srcObservationService;
    private DatabaseObservationDAO srcObservationDAO;
    private DatabaseObservationDAO destObservationDAO;

    private Date maxDate;
    HarvestSkipURIDAO harvestSkip = null;

    public ObservationValidator(String resourceId, String collection, int nthreads, String[] dest,
            Integer batchSize, boolean full, boolean dryrun)
            throws IOException, URISyntaxException {
        super(Observation.class, null, dest, batchSize, full, dryrun);
        init(resourceId, collection, nthreads);
    }

    public ObservationValidator(String[] src, String[] dest, Integer batchSize, boolean full,
            boolean dryrun) throws IOException, URISyntaxException {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        init();
    }

    public void setInteractive(boolean enabled) {
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public void setDoCollisionCheck(boolean doCollisionCheck) {
    }

    private void init() throws IOException, URISyntaxException {
        Map<String, Object> config1 = getConfigDAO(src);
        Map<String, Object> config2 = getConfigDAO(dest);
        this.srcObservationDAO = new DatabaseObservationDAO();
        srcObservationDAO.setConfig(config1);
        this.destObservationDAO = new DatabaseObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    private void init(String uri, String collection, int threads)
            throws IOException, URISyntaxException {
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
    public void run() {
        log.info("START VALIDATION: " + Observation.class.getSimpleName());

        boolean go = true;
        while (go) {
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

    private static class Progress {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int ingested = 0;
        int failed = 0;

        @Override
        public String toString() {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private Date startDate;
    HarvestState state = null;

    private Progress doit() {
        Progress ret = new Progress();

        // BufferedReader stdin = null;
        // if (interactive)
        // {
        // stdin = new BufferedReader(new InputStreamReader(System.in));
        // }

        long t = System.currentTimeMillis();
        long tState = -1;
        long tQuery = -1;
        long tTransaction = -1;

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null)
            expectedNum = batchSize.intValue();
        try {
            System.gc(); // hint
            t = System.currentTimeMillis();

            if (state == null) {
                state = harvestState.get(source, Observation.class.getSimpleName());
            }

            log.info("**************** state = " + state.curLastModified + " source = " + source
                    + " )");
            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            if (full)
                startDate = null;
            else
                startDate = state.curLastModified;
            // else: skipped: keep startDate across multiple batches since we
            // don't persist harvest
            // state

            Date end = maxDate;
            List<SkippedWrapperURI<ObservationState>> entityListSrc = null;
            // List<SkippedWrapperURI<ObservationState>> entityListDst = null;

            Date fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60000L); // 5
                                                                                 // minutes
                                                                                 // ago;
            if (end == null)
                end = fiveMinAgo;
            else {
                log.info("harvest limit: min( " + format(fiveMinAgo) + " " + format(end) + " )");
                if (end.getTime() > fiveMinAgo.getTime())
                    end = fiveMinAgo;
            }

            log.info("harvest window: " + format(startDate) + " :: " + format(end) + " ["
                    + batchSize + "]");

            List<ObservationState> tmpSrcState = null;
            List<ObservationState> tmpDstState = null;

            tmpDstState = destObservationDAO.getObservationList(collection, startDate, end,
                    batchSize + 1);
            // tmpDst = destObservationDAO.getList(Observation.class, startDate,
            // end, batchSize + 1);

            if (!this.service) {
                tmpSrcState = srcObservationDAO.getObservationList(collection, startDate, end,
                        batchSize + 1);
            } else {
                tmpSrcState = srcObservationService.getObservationList(collection, startDate, end,
                        batchSize + 1);
            }

            List<ObservationState> errlist = calculateErroneousObservations(tmpSrcState,
                    tmpDstState);
            log.info("************************** errlist.size() = " + errlist.size());
            log.info("************************** tmpSrcState.size() = " + tmpSrcState.size());
            log.info("************************** tmpDstState.size() = " + tmpDstState.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            entityListSrc = wrapState(tmpSrcState);

            ret.found = tmpSrcState.size();
            log.info("found: " + tmpSrcState.size());

            ListIterator<SkippedWrapperURI<ObservationState>> iter = entityListSrc.listIterator();
            while (iter.hasNext()) {
                SkippedWrapperURI<ObservationState> ow = iter.next();
                ObservationState o = ow.entity;
                HarvestSkipURI hs = ow.skip;

                iter.remove(); // allow garbage collection during loop

                String lastMsg = null;
                String skipMsg = null;

                try {
                    // o could be null in skip mode cleanup
                    if (!dryrun) {
                        if (o != null) {
                            if (state != null) {
                                state.curLastModified = o.maxLastModified;
                                state.curID = null;
                            }

                            if (hs == null) // success in redo mode
                                harvestState.put(state);

                            skipMsg = o + ": " + lastMsg;
                            lastMsg = null;
                            // destObservationDAO.getTransactionManager().rollbackTransaction();
                            // log.warn("rollback: OK");
                            // tTransaction += System.currentTimeMillis() - t;

                            try {
                                log.debug("starting HarvestSkipURI transaction");
                                boolean putSkip = true;
                                HarvestSkipURI skip = harvestSkip.get(source, cname,
                                        o.getURI().getURI());
                                if (skip == null) {
                                    skip = new HarvestSkipURI(source, cname, o.getURI().getURI(),
                                            skipMsg);
                                } else {
                                    if (skipMsg != null && !skipMsg.equals(skip.errorMessage)) {
                                        skip.errorMessage = skipMsg; // possible
                                                                     // update
                                    } else {
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
                                if (putSkip) {
                                    log.info("put: " + skip);
                                    harvestSkip.put(skip);
                                }

                            } catch (Throwable oops) {
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
                } catch (Throwable oops) {
                    lastMsg = oops.getMessage();
                    String str = oops.toString();
                    if (oops instanceof Error) {
                        log.error("FATAL - probably installation or environment", oops);
                        ret.abort = true;
                    } else if (oops instanceof NullPointerException) {
                        log.error("BUG", oops);
                        ret.abort = true;
                    } else if (oops instanceof BadSqlGrammarException) {
                        log.error("BUG", oops);
                        BadSqlGrammarException bad = (BadSqlGrammarException) oops;
                        SQLException sex1 = bad.getSQLException();

                        if (sex1 != null) {
                            log.error("CAUSE", sex1);
                            SQLException sex2 = sex1.getNextException();
                            log.error("NEXT CAUSE", sex2);
                        }
                        ret.abort = true;
                    } else if (oops instanceof DataAccessResourceFailureException) {
                        log.error("SEVERE PROBLEM - probably out of space in database", oops);
                        ret.abort = true;
                    } else if (oops instanceof DataIntegrityViolationException && str.contains(
                            "duplicate key value violates unique constraint \"i_observationuri\"")) {
                        log.error("CONTENT PROBLEM - duplicate observation: " + " "
                                + o.getURI().getURI().toASCIIString());
                    } else if (oops instanceof UncategorizedSQLException) {
                        if (str.contains("spherepoly_from_array")) {
                            log.error("UNDETECTED illegal polygon: " + o.getURI().getURI());
                        } else
                            log.error("unexpected exception", oops);
                    } else if (oops instanceof IllegalArgumentException
                            && str.contains("CaomValidator") && str.contains("keywords")) {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " "
                                + o.getURI().getURI().toASCIIString());
                    } else
                        log.error("unexpected exception", oops);
                } finally {
                }
                if (ret.abort)
                    return ret;
            }
            if (ret.found < expectedNum)
                ret.done = true;
        } finally {
            tTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + tState + "ms");
            log.debug("time to run ObservationListQuery: " + tQuery + "ms");
            log.debug("time to run transactions: " + tTransaction + "ms");
        }
        return ret;
    }

    private List<ObservationState> calculateErroneousObservations(
            List<ObservationState> tmpSrcState, List<ObservationState> tmpDstState) {
        List<ObservationState> errList = new ArrayList<ObservationState>();
        for (ObservationState osSrc : tmpSrcState) {
            log.debug("SRC -> ObservationState (URI): " + osSrc.getURI().getURI().toString());
            log.debug("SRC -> ObservationState (AccMetaChecksum): " + osSrc.accMetaChecksum);

            boolean found = false;
            for (ObservationState osDst : tmpDstState) {
                log.debug("DST -> ObsetvationState (URI): " + osDst.getURI().getURI().toString());
                log.debug("DST -> ObservationState (AccMetaChecksum): " + osDst.accMetaChecksum);

                if (!osSrc.getURI().getURI().equals(osDst.getURI().getURI()))
                    continue;
                found = true;
                if (osSrc.accMetaChecksum == null || osDst.accMetaChecksum == null
                        || !osSrc.accMetaChecksum.equals(osDst.accMetaChecksum))
                    errList.add(osDst);
                break;
            }
            if (!found) {
                errList.add(osSrc);
            }
        }

        return errList;
    }

    private void detectLoop(List<SkippedWrapperURI<Observation>> entityList) {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<Observation> start = entityList.get(0);
        SkippedWrapperURI<Observation> end = entityList.get(entityList.size() - 1);

        if (start.entity.getMaxLastModified().equals(end.entity.getMaxLastModified())) {
            throw new RuntimeException(
                    "detected infinite harvesting loop: " + entityClass.getSimpleName() + " at "
                            + format(start.entity.getMaxLastModified()));
        }
    }

    private void detectLoopState(List<SkippedWrapperURI<ObservationState>> entityList) {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<ObservationState> start = entityList.get(0);
        SkippedWrapperURI<ObservationState> end = entityList.get(entityList.size() - 1);

        if (start.entity.maxLastModified.equals(end.entity.maxLastModified)) {
            throw new RuntimeException("detected infinite harvesting loop: "
                    + entityClass.getSimpleName() + " at " + format(start.entity.maxLastModified));
        }
    }

    private List<SkippedWrapperURI<ObservationState>> wrapState(List<ObservationState> obsList) {
        List<SkippedWrapperURI<ObservationState>> ret = new ArrayList<SkippedWrapperURI<ObservationState>>(
                obsList.size());
        for (ObservationState o : obsList) {
            ret.add(new SkippedWrapperURI<ObservationState>(o, null));
        }
        return ret;
    }

    private List<SkippedWrapperURI<Observation>> wrap(List<Observation> obsList) {
        List<SkippedWrapperURI<Observation>> ret = new ArrayList<SkippedWrapperURI<Observation>>(
                obsList.size());
        for (Observation o : obsList) {
            ret.add(new SkippedWrapperURI<Observation>(o, null));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, Class c) {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2], batchSize);
    }
}