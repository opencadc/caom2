
package ca.nrc.cadc.caom2.harvester;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.repo.client.WorkerResponse;
import ca.nrc.cadc.caom2.util.CaomValidator;

/**
 *
 * @author pdowler
 */
public class ObservationHarvester extends Harvester
{

    private static Logger log = Logger.getLogger(ObservationHarvester.class);

    private boolean interactive;

    private boolean service = false;

    private RepoClient srcObservationService;
    private DatabaseObservationDAO srcObservationDAO;
    private DatabaseObservationDAO destObservationDAO;

    private boolean skipped;
    private Date maxDate;
    private boolean doCollisionCheck = false;
    private String collection = null;
    private String resourceId = null;

    HarvestSkipURIDAO harvestSkip = null;

    private ObservationHarvester()
    {
    }

    public ObservationHarvester(String resourceId, String collection,
            int nthreads, String[] dest, Integer batchSize, boolean full,
            boolean dryrun) throws IOException, URISyntaxException
    {
        super(Observation.class, null, dest, batchSize, full, dryrun);
        init(resourceId, collection, nthreads);
    }

    public ObservationHarvester(String[] src, String[] dest, Integer batchSize,
            boolean full, boolean dryrun) throws IOException, URISyntaxException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        init();
    }

    public void setSkipped(boolean skipped)
    {
        this.skipped = skipped;
    }

    public void setInteractive(boolean enabled)
    {
        this.interactive = enabled;
    }

    public void setMaxDate(Date maxDate)
    {
        this.maxDate = maxDate;
    }

    public void setDoCollisionCheck(boolean doCollisionCheck)
    {
        this.doCollisionCheck = doCollisionCheck;
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

    private void init(String uri, String collection, int threads)
            throws IOException, URISyntaxException
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

    private void close() throws IOException
    {
        // TODO
    }

    private String format(UUID id)
    {
        if (id == null)
            return "null";
        return Long.toString(id.getLeastSignificantBits());
    }

    @Override
    public void run()
    {
        log.info("START: " + Observation.class.getSimpleName());
        try
        {
            // init();
        } catch (Throwable oops)
        {
            throw new RuntimeException("failed to init connections and state",
                    oops);
        }

        boolean go = true;
        while (go)
        {
            Progress num = doit();

            if (num.found > 0)
                log.info("***************** finished batch: " + num
                        + " *******************");

            // double failFrac = ((double) num.failed - num.handled) / ((double)
            // num.found);
            // if (!skipped && failFrac > 0.5)
            // {
            // log.warn("failure rate is quite high: " + num.failed + "/" +
            // num.found);
            // num.abort = true;
            // }
            if (num.abort)
                log.error("batched aborted");
            go = (num.found > 0 && !num.abort && !num.done);
            if (batchSize != null && num.found < batchSize.intValue() / 2)
                go = false;
            full = false; // do not start at beginning again
            if (dryrun)
                go = false; // no state update -> infinite loop
        }
        try
        {
            close();
        } catch (Throwable oops)
        {
            log.error("failed to cleanup connections and state", oops);
            return;
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
        int handled = 0;

        @Override
        public String toString()
        {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private Date startDate;

    private Progress doit()
    {
        Progress ret = new Progress();

        BufferedReader stdin = null;
        if (interactive)
        {
            stdin = new BufferedReader(new InputStreamReader(System.in));
        }

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

            HarvestState state = null;

            if (!skipped)
                state = harvestState.get(source,
                        Observation.class.getSimpleName());

            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            if (full)
                startDate = null;
            else if (!skipped)
                startDate = state.curLastModified;
            // else: skipped: keep startDate across multiple batches since we
            // don't persist harvest
            // state

            Date end = maxDate;
            List<SkippedWrapperURI<Observation>> entityList = null;
            if (skipped)
            {
                entityList = getSkipped(startDate);
            } else
            {
                Date fiveMinAgo = new Date(
                        System.currentTimeMillis() - 5 * 60000L); // 5 minutes
                                                                  // ago;
                if (end == null)
                    end = fiveMinAgo;
                else
                {
                    log.info("harvest limit: min( " + format(fiveMinAgo) + " "
                            + format(end) + " )");
                    if (end.getTime() > fiveMinAgo.getTime())
                        end = fiveMinAgo;
                }

                log.info("harvest window: " + format(startDate) + " :: "
                        + format(end) + " [" + batchSize + "]");
                List<Observation> tmp = null;
                if (!this.service)
                {
                    tmp = srcObservationDAO.getList(Observation.class,
                            startDate, end, batchSize + 1);
                } else
                {
                    tmp = new ArrayList<Observation>();
                    List<WorkerResponse> l = srcObservationService
                            .getList(collection, startDate, end, batchSize + 1);
                    for (WorkerResponse wr : l)
                    {
                        if (wr.getObservation() != null)
                            tmp.add(wr.getObservation());
                    }
                }
                entityList = wrap(tmp);
            }

            if (entityList.size() >= expectedNum)
            {
                try
                {
                    detectLoop(entityList);
                } catch (RuntimeException rex)
                {
                    if (!skipped)
                    {
                        Integer tmpBatchSize = entityList.size() + 1;
                        log.info("(loop) temporary harvest window: "
                                + format(startDate) + " :: " + format(end)
                                + " [" + tmpBatchSize + "]");

                        List<Observation> tmp = null;
                        if (!this.service)
                        {
                            tmp = srcObservationDAO.getList(Observation.class,
                                    startDate, end, tmpBatchSize);
                        } else
                        {
                            tmp = new ArrayList<Observation>();
                            List<WorkerResponse> l = null;
                            l = srcObservationService.getList(collection,
                                    startDate, end, tmpBatchSize);
                            for (WorkerResponse wr : l)
                            {
                                if (wr.getObservation() != null)
                                    tmp.add(wr.getObservation());
                            }

                        }

                        entityList = wrap(tmp);
                        detectLoop(entityList);
                    } else
                        throw rex;
                }
            }

            // avoid re-processing the last successful one stored in
            // HarvestState
            if (!entityList.isEmpty() && !skipped)
            {
                ListIterator<SkippedWrapperURI<Observation>> iter = entityList
                        .listIterator();
                Observation curBatchLeader = iter.next().entity;
                log.debug("currentBatch: " + format(curBatchLeader.getID())
                        + " " + format(curBatchLeader.getMaxLastModified()));
                log.debug("harvestState: " + format(state.curID) + " "
                        + format(state.curLastModified));
                if (curBatchLeader.getID().equals(state.curID) // same obs as
                                                               // last time
                        && curBatchLeader.getMaxLastModified()
                                .equals(state.curLastModified)) // not
                                                                // modified
                                                                // since
                {
                    iter.remove(); // processed in last batch but picked up by
                                   // lastModified query
                    expectedNum--;
                }
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<SkippedWrapperURI<Observation>> iter = entityList
                    .listIterator();
            while (iter.hasNext())
            {
                SkippedWrapperURI<Observation> ow = iter.next();
                Observation o = ow.entity;
                HarvestSkipURI hs = ow.skip;
                iter.remove(); // allow garbage collection during loop

                String lastMsg = null;
                String skipMsg = null;

                if (!dryrun)
                {
                    if (destObservationDAO.getTransactionManager().isOpen())
                        throw new RuntimeException(
                                "BUG: found open trasnaction at start of next observation");
                    log.debug("starting transaction");
                    destObservationDAO.getTransactionManager()
                            .startTransaction();
                }
                boolean ok = false;
                try
                {
                    // o could be null in skip mode cleanup
                    if (o != null)
                    {
                        String treeSize = computeTreeSize(o);
                        log.info("put: " + o.getClass().getSimpleName() + " "
                                + format(o.getID()) + " "
                                + format(o.getMaxLastModified()) + " "
                                + treeSize);
                    }
                    if (!dryrun)
                    {
                        if (skipped)
                            startDate = hs.lastModified;
                        if (o != null)
                        {
                            if (state != null)
                            {
                                state.curLastModified = o.getMaxLastModified();
                                state.curID = o.getID();
                            }

                            // try to avoid DataIntegrityViolationException due
                            // to missed deletion
                            // of an observation
                            UUID curID = destObservationDAO.getID(o.getURI());
                            if (curID != null && !curID.equals(o.getID()))
                            {
                                ObservationURI oldSrc = srcObservationDAO
                                        .getURI(curID); // still in
                                                        // src?
                                if (oldSrc == null)
                                {
                                    // missed harvesting a deletion
                                    log.info("delete: "
                                            + o.getClass().getSimpleName() + " "
                                            + format(curID)
                                            + " (ObservationURI conflict avoided)");
                                    destObservationDAO.delete(curID);
                                }
                                // else: the put below with throw a valid
                                // exception because source
                                // is not enforcing
                                // unique ID and URI
                            }
                            if (doCollisionCheck)
                            {
                                Observation cc = destObservationDAO
                                        .getShallow(o.getID());
                                log.info("collision check: " + o.getURI() + " "
                                        + format(o.getMaxLastModified())
                                        + " vs "
                                        + format(cc.getMaxLastModified()));
                                if (!cc.getMaxLastModified()
                                        .equals(o.getMaxLastModified()))
                                    throw new IllegalStateException(
                                            "detected harvesting collision: "
                                                    + o.getURI()
                                                    + " maxLastModified: "
                                                    + format(o
                                                            .getMaxLastModified()));
                            }

                            // advannce the date before put as there are usually
                            // lots of fails
                            if (skipped)
                                startDate = hs.lastModified;

                            // temporary validation hack to avoid tickmarks in
                            // the keywords columns
                            CaomValidator.validateKeywords(o);

                            destObservationDAO.put(o);

                            if (hs != null) // success in redo mode
                            {
                                log.info("delete: " + hs + " "
                                        + format(hs.lastModified));
                                harvestSkip.delete(hs);
                            } else
                                harvestState.put(state);
                        } else if (skipped) // observation is gone from src
                        {
                            log.info("delete: " + hs + " "
                                    + format(hs.lastModified));
                            harvestSkip.delete(hs);
                        }

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager()
                                .commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                } catch (Throwable oops)
                {
                    lastMsg = oops.getMessage();
                    String str = oops.toString();
                    if (oops instanceof Error)
                    {
                        log.error(
                                "FATAL - probably installation or environment",
                                oops);
                        ret.abort = true;
                    } else if (oops instanceof NullPointerException)
                    {
                        log.error("BUG", oops);
                        ret.abort = true;
                    } else if (oops instanceof BadSqlGrammarException)
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
                    } else if (oops instanceof DataAccessResourceFailureException)
                    {
                        log.error(
                                "SEVERE PROBLEM - probably out of space in database",
                                oops);
                        ret.abort = true;
                    } else if (oops instanceof DataIntegrityViolationException
                            && str.contains(
                                    "duplicate key value violates unique constraint \"i_observationuri\""))
                    {
                        log.error("CONTENT PROBLEM - duplicate observation: "
                                + format(o.getID()) + " "
                                + o.getURI().getURI().toASCIIString());
                        ret.handled++;
                    } else if (oops instanceof UncategorizedSQLException)
                    {
                        if (str.contains("spherepoly_from_array"))
                        {
                            log.error("UNDETECTED illegal polygon: "
                                    + o.getURI());
                            ret.handled++;
                        } else
                            log.error("unexpected exception", oops);
                    } else if (oops instanceof IllegalArgumentException
                            && str.contains("CaomValidator")
                            && str.contains("keywords"))
                    {
                        log.error("CONTENT PROBLEM - invalid keywords: "
                                + format(o.getID()) + " "
                                + o.getURI().getURI().toASCIIString());
                        ret.handled++;
                    } else
                        log.error("unexpected exception", oops);
                } finally
                {
                    if (!ok && !dryrun)
                    {
                        log.warn("failed to insert " + o + ": " + lastMsg);
                        skipMsg = o + ": " + lastMsg;
                        lastMsg = null;
                        destObservationDAO.getTransactionManager()
                                .rollbackTransaction();
                        log.warn("rollback: OK");
                        tTransaction += System.currentTimeMillis() - t;

                        try
                        {
                            log.debug("starting HarvestSkipURI transaction");
                            boolean putSkip = true;
                            HarvestSkipURI skip = harvestSkip.get(source, cname,
                                    o.getURI().getURI());
                            if (skip == null)
                                skip = new HarvestSkipURI(source, cname,
                                        o.getURI().getURI(), skipMsg);
                            else
                            {
                                if (skipMsg != null
                                        && !skipMsg.equals(skip.errorMessage))
                                {
                                    skip.errorMessage = skipMsg; // possible
                                                                 // update
                                } else
                                {
                                    log.info("no change in status: " + hs);
                                    putSkip = false; // avoid timestamp update
                                }
                            }

                            destObservationDAO.getTransactionManager()
                                    .startTransaction();

                            if (!skipped)
                            {
                                // track the harvest state progress
                                harvestState.put(state);
                            }

                            // track the fail
                            if (putSkip)
                            {
                                log.info("put: " + skip);
                                harvestSkip.put(skip);
                            }

                            // TBD: delete previous version of obs?
                            destObservationDAO.delete(o.getID());
                            log.debug("committing HarvestSkipURI transaction");
                            destObservationDAO.getTransactionManager()
                                    .commitTransaction();
                            log.debug("commit HarvestSkipURI: OK");
                        } catch (Throwable oops)
                        {
                            log.warn("failed to insert HarvestSkipURI", oops);
                            destObservationDAO.getTransactionManager()
                                    .rollbackTransaction();
                            log.warn("rollback HarvestSkipURI: OK");
                            ret.abort = true;
                        }
                        ret.failed++;
                    }

                    if (interactive)
                    {
                        try
                        {
                            String str = "";
                            while (str != null)
                            {
                                System.out.print("\n\n(n=next, q=quit): ");
                                str = stdin.readLine();
                                if ("n".equals(str))
                                    break;
                                else if ("q".equals(str))
                                {
                                    ret.abort = true;
                                    break;
                                } else
                                    System.out.println(
                                            "unexpected input: " + str);
                            }
                        } catch (IOException e)
                        {

                        }
                    }
                }
                if (ret.abort)
                    return ret;
            }
            if (ret.found < expectedNum)
                ret.done = true;
        } catch (InterruptedException | ExecutionException e)
        {
            log.error(
                    "SEVERE PROBLEM - ThreadPool harvesting Observations failed: "
                            + e.getMessage());
            ret.abort = true;
        } finally
        {
            tTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + tState + "ms");
            log.debug("time to run ObservationListQuery: " + tQuery + "ms");
            log.debug("time to run transactions: " + tTransaction + "ms");
        }
        return ret;
    }

    private String computeTreeSize(Observation o)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int numA = 0;
        int numP = 0;
        for (Plane p : o.getPlanes())
        {
            numA += p.getArtifacts().size();
            for (Artifact a : p.getArtifacts())
            {
                numP += a.getParts().size();
            }
        }
        sb.append(o.getPlanes().size()).append(",");
        sb.append(numA).append(",");
        sb.append(numP).append("]");
        return sb.toString();
    }

    private void detectLoop(List<SkippedWrapperURI<Observation>> entityList)
    {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<Observation> start = entityList.get(0);
        SkippedWrapperURI<Observation> end = entityList
                .get(entityList.size() - 1);
        if (skipped)
        {
            if (start.skip.lastModified.equals(end.skip.lastModified))
                throw new RuntimeException("detected infinite harvesting loop: "
                        + HarvestSkipURI.class.getSimpleName() + " at "
                        + format(start.skip.lastModified));
            return;
        }
        if (start.entity.getMaxLastModified()
                .equals(end.entity.getMaxLastModified()))
        {
            throw new RuntimeException("detected infinite harvesting loop: "
                    + entityClass.getSimpleName() + " at "
                    + format(start.entity.getMaxLastModified()));
        }
    }

    private List<SkippedWrapperURI<Observation>> wrap(List<Observation> obsList)
    {
        List<SkippedWrapperURI<Observation>> ret = new ArrayList<SkippedWrapperURI<Observation>>(
                obsList.size());
        for (Observation o : obsList)
        {
            ret.add(new SkippedWrapperURI<Observation>(o, null));
        }
        return ret;
    }

    private List<SkippedWrapperURI<Observation>> getSkipped(Date start)
    {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize
                + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);

        log.info("skip.size(): " + skip.size());

        List<SkippedWrapperURI<Observation>> ret = new ArrayList<SkippedWrapperURI<Observation>>(
                skip.size());
        for (HarvestSkipURI hs : skip)
        {
            Observation o = null;
            WorkerResponse wr = srcObservationService.get(hs.getSkipID(),
                    start);

            if (wr.getObservation() != null)
                o = wr.getObservation();

            if (o != null)
            {
                ret.add(new SkippedWrapperURI<Observation>(o, hs));
            }
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2],
                batchSize);
        this.source = resourceId + "?" + collection;

    }
}