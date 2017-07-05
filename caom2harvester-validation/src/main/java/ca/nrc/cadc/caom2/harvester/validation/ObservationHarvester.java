
package ca.nrc.cadc.caom2.harvester.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.repo.client.ObservationState;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.repo.client.WorkerResponse;

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

    private Date maxDate;
    private boolean doCollisionCheck = false;

    HarvestSkipURIDAO harvestSkip = null;

    public ObservationHarvester(String resourceId, String collection, int nthreads, String[] dest, Integer batchSize,
            boolean full, boolean dryrun) throws IOException, URISyntaxException
    {
        super(Observation.class, null, dest, batchSize, full, dryrun);
        init(resourceId, collection, nthreads);
    }

    public ObservationHarvester(String[] src, String[] dest, Integer batchSize, boolean full, boolean dryrun)
            throws IOException, URISyntaxException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        init();
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
        int handled = 0;

        @Override
        public String toString()
        {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private Date startDate;
    HarvestState state = null;

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

            if (state == null)
            {
                state = harvestState.get(source, Observation.class.getSimpleName());
            }

            log.info("**************** state = " + state.curLastModified + " source = " + source + " )");
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
            List<SkippedWrapperURI<Observation>> entityListSrc = null;
            List<SkippedWrapperURI<Observation>> entityListDst = null;

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
            List<ca.nrc.cadc.caom2.ObservationState> tmpDstState = null;
            List<WorkerResponse> tmpSrcResponse = null;
            List<Observation> tmpSrc = null;
            List<Observation> tmpDst = null;

            tmpDstState = destObservationDAO.getObservationList(collection, startDate, end, batchSize + 1);
            tmpDst = destObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);

            if (!this.service)
            {
                // tmpSrcState =
                // srcObservationDAO.getObservationList(collection, startDate,
                // end, batchSize + 1);
                // tmpSrc = srcObservationDAO.getList(Observation.class,
                // startDate, end, batchSize + 1);
            }
            else
            {
                tmpSrc = new ArrayList<Observation>();
                tmpSrcState = srcObservationService.getObservationList(collection, startDate, end, batchSize + 1);
                tmpSrcResponse = srcObservationService.getList(collection, startDate, end, batchSize + 1);
                for (WorkerResponse wr : tmpSrcResponse)
                {
                    if (wr.getObservation() != null)
                        tmpSrc.add(wr.getObservation());
                }
            }

            entityListSrc = wrap(tmpSrc);
            // entityListDst = wrapState(tmpDstState);
            // detectLoopState(entityListSrc);
            // detectLoopState(entityListDst);

            List<ObservationState> errlist = calculateErroneousObservations(tmpSrcResponse, tmpDst);
            log.info("************************** errlist.size() = " + errlist.size());

            ret.found = entityListSrc.size();
            log.info("found: " + entityListSrc.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<SkippedWrapperURI<Observation>> iter = entityListSrc.listIterator();
            while (iter.hasNext())
            {
                SkippedWrapperURI<Observation> ow = iter.next();
                Observation o = ow.entity;

                iter.remove(); // allow garbage collection during loop

                try
                {
                    // o could be null in skip mode cleanup
                    if (o != null && state != null)
                    {
                        state.curLastModified = o.getMaxLastModified();
                        state.curID = o.getID();
                    }
                    ret.ingested++;
                }
                catch (Throwable oops)
                {
                }
                finally
                {
                    ret.failed++;
                }
                if (ret.abort)
                    return ret;
            }
            if (ret.found < expectedNum)
                ret.done = true;
        }
        catch (InterruptedException | ExecutionException e)
        {
            log.error("SEVERE PROBLEM - ThreadPool harvesting Observations failed: " + e.getMessage());
            ret.abort = true;
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

    private List<ObservationState> calculateErroneousObservations(List<WorkerResponse> tmpSrcResponse,
            List<Observation> tmpDst)
    {
        List<ObservationState> errList = new ArrayList<ObservationState>();
        for (WorkerResponse wr : tmpSrcResponse)
        {
            log.info("ObsertvationState: " + wr.getObservationState().toString());
            if (wr.getError() != null)
            {
                errList.add(wr.getObservationState());
                continue;
            }
            boolean found = false;
            for (Observation o2 : tmpDst)
            {
                if (!wr.getObservation().getObservationID().equals(o2.getObservationID()))
                    continue;
                found = true;
                if (!wr.getObservation().equals(o2))
                    errList.add(wr.getObservationState());
                break;
            }
            if (!found)
            {
                errList.add(wr.getObservationState());
            }
        }

        return errList;
    }

    private void detectLoop(List<SkippedWrapperURI<Observation>> entityList)
    {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<Observation> start = entityList.get(0);
        SkippedWrapperURI<Observation> end = entityList.get(entityList.size() - 1);

        if (start.entity.getMaxLastModified().equals(end.entity.getMaxLastModified()))
        {
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName() + " at "
                    + format(start.entity.getMaxLastModified()));
        }
    }

    private void detectLoopState(List<SkippedWrapperURI<ObservationState>> entityList)
    {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<ObservationState> start = entityList.get(0);
        SkippedWrapperURI<ObservationState> end = entityList.get(entityList.size() - 1);

        if (start.entity.getMaxLastModified().equals(end.entity.getMaxLastModified()))
        {
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName() + " at "
                    + format(start.entity.getMaxLastModified()));
        }
    }
    private List<SkippedWrapperURI<ObservationState>> wrapState(List<ObservationState> obsList)
    {
        List<SkippedWrapperURI<ObservationState>> ret = new ArrayList<SkippedWrapperURI<ObservationState>>(
                obsList.size());
        for (ObservationState o : obsList)
        {
            ret.add(new SkippedWrapperURI<ObservationState>(o, null));
        }
        return ret;
    }
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
    protected void initHarvestState(DataSource ds, Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2], batchSize);
    }
}