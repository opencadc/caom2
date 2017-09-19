
package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.net.TransientException;

/**
 *
 * @author pdowler
 */
public class ObservationHarvester extends Harvester
{

    private static Logger log = Logger.getLogger(ObservationHarvester.class);

    private boolean service = false;

    private RepoClient srcObservationService;
    private ObservationDAO srcObservationDAO;
    private ObservationDAO destObservationDAO;

    private boolean skipped;
    private Date maxDate;
    private boolean doCollisionCheck = false;
    private boolean computePlaneMetadata = false;
    private boolean nochecksum = false;

    HarvestSkipURIDAO harvestSkip = null;

    public ObservationHarvester(HarvestResource src, HarvestResource dest, Integer batchSize, 
            boolean full, boolean dryrun, boolean nochecksum, int nthreads) 
        throws IOException, URISyntaxException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        this.nochecksum = nochecksum;
        init(nthreads);
    }

    public void setSkipped(boolean skipped)
    {
        this.skipped = skipped;
    }

    public void setMaxDate(Date maxDate)
    {
        this.maxDate = maxDate;
    }

    public void setDoCollisionCheck(boolean doCollisionCheck)
    {
        this.doCollisionCheck = doCollisionCheck;
    }

    public void setComputePlaneMetadata(boolean computePlaneMetadata)
    {
        this.computePlaneMetadata = computePlaneMetadata;
    }

    public boolean getComputePlaneMetadata()
    {
        return computePlaneMetadata;
    }

    private void init(int nthreads) throws IOException, URISyntaxException
    {
        if (src.getDatabaseServer() != null)
        {
            Map<String, Object> config1 = getConfigDAO(src);
            this.srcObservationDAO = new ObservationDAO();
            srcObservationDAO.setConfig(config1);
        }
        else
        {
            this.srcObservationService = new RepoClient(src.getResourceID(), nthreads);
        }
        
        // for now, dest is always a database
        Map<String, Object> config2 = getConfigDAO(dest);
        this.destObservationDAO = new ObservationDAO();
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
                log.debug("***************** finished batch: " + num + " *******************");

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
    private boolean firstIteration = true;
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

            HarvestState state = null;

            if (!skipped)
            {
                state = harvestState.get(source, Observation.class.getSimpleName());
                log.debug("state " + state);
            }

            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            if (full && firstIteration)
            {
                startDate = null;
            }
            else if (!skipped)
            {
                log.debug("recalculate startDate");
                startDate = state.curLastModified;
            }
            log.debug("startDate " + startDate);
            // else: skipped: keep startDate across multiple batches since we
            // don't persist harvest
            // state
            firstIteration = false;

            log.debug("skipped: " + (skipped));

            Date end = maxDate;
            List<SkippedWrapperURI<ObservationResponse>> entityList = null;
            //List<SkippedWrapperURI<ObservationState>> entityListState = null;

            if (skipped)
            {
                entityList = getSkipped(startDate);
                //entityListState = getSkippedState(startDate);
            }
            else
            {
                Date fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60000L);
                if (end == null)
                    end = fiveMinAgo;
                else
                {
                    log.debug("harvest limit: min( " + format(fiveMinAgo) + " " + format(end) + " )");
                    if (end.getTime() > fiveMinAgo.getTime())
                        end = fiveMinAgo;
                }

                log.info("harvest window: " + format(startDate) + " :: " + format(end) + " [" + batchSize + "]");
                List<ObservationResponse> obsList = null;
                //List<ObservationState> stateList = null;
                if (srcObservationDAO != null)
                {
                    //List<Observation> tmp = srcObservationDAO.getList(Observation.class, startDate, end, batchSize + 1);
                    // wrap in ObservationResponse to mimic service response
                    //obsList = new ArrayList<ObservationResponse>();
                    //for (Observation o : tmp)
                    //    obsList.add(new ObservationResponse(o));
                    
                    obsList = srcObservationDAO.getList(src.getCollection(), startDate, end, batchSize + 1);
                    //stateList = srcObservationDAO.getObservationList(src.getCollection(), startDate, end, batchSize + 1);
                }
                else if (srcObservationService != null)
                {
                    obsList = srcObservationService.getList(src.getCollection(), startDate, end, batchSize + 1);
                    //stateList = srcObservationService.getObservationList(src.getCollection(), startDate, end, batchSize + 1);
                }
                entityList = wrap(obsList);
                //entityListState = wrapState(stateList);
            }

            if (entityList.size() >= expectedNum)
            {
                try
                {
                    detectLoop(entityList);
                }
                catch (RuntimeException rex)
                {
                    if (!skipped)
                    {
                        Integer tmpBatchSize = entityList.size() + 1;
                        log.info("(loop) temporary harvest window: " + format(startDate) + " :: " + format(end) + " [" + tmpBatchSize + "]");

                        List<ObservationResponse> obsList = null;
                        //List<ObservationState> stateList = null;
                        if (!this.service)
                        {
                            List<Observation> tmp = srcObservationDAO.getList(Observation.class, startDate, end, tmpBatchSize);
                            obsList = new ArrayList<ObservationResponse>();

                            for (Observation o : tmp)
                            {
                                obsList.add(new ObservationResponse(o));
                            }
                            //stateList = srcObservationDAO.getObservationList(src.getCollection(), startDate, end, tmpBatchSize);
                        }
                        else
                        {
                            obsList = srcObservationService.getList(src.getCollection(), startDate, end, tmpBatchSize);
                            //stateList = srcObservationService.getObservationList(src.getCollection(), startDate, end, tmpBatchSize);
                        }

                        entityList = wrap(obsList);
                        //entityListState = wrapState(stateList);
                        detectLoop(entityList);
                    }
                    else
                        throw rex;
                }
            }

            // avoid re-processing the last successful one stored in HarvestState
            if (!entityList.isEmpty() && !skipped)
            {
                ListIterator<SkippedWrapperURI<ObservationResponse>> iter = entityList.listIterator();
                Observation curBatchLeader = iter.next().entity.observation;
                if (curBatchLeader != null)
                {
                    log.debug("currentBatch: " + curBatchLeader.getURI() + " " + format(curBatchLeader.getMaxLastModified()));
                    log.debug("harvestState: " + format(state.curID) + " " + format(state.curLastModified));
                    if (curBatchLeader.getID().equals(state.curID)
                            && curBatchLeader.getMaxLastModified().equals(state.curLastModified))
                    {
                        //entityListState.remove(0);
                        iter.remove();
                        expectedNum--;
                    }

                }
            }

            ret.found = entityList.size();
            log.debug("found: " + entityList.size());
            //log.debug("found os: " + entityListState.size());

            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<SkippedWrapperURI<ObservationResponse>> iter1 = entityList.listIterator();
            //int i = 0;
            while (iter1.hasNext())
            {
                SkippedWrapperURI<ObservationResponse> ow = iter1.next();
                Observation o = null;
                if (ow.entity != null)
                    o = ow.entity.observation;
                HarvestSkipURI hs = ow.skip;
                iter1.remove(); // allow garbage collection during loop

                String lastMsg = null;
                String skipMsg = null;

                if (!dryrun)
                {
                    if (destObservationDAO.getTransactionManager().isOpen())
                        throw new RuntimeException("BUG: found open trasnaction at start of next observation");
                    log.debug("starting transaction");
                    destObservationDAO.getTransactionManager().startTransaction();
                }
                boolean ok = false;
                try
                {
                    // o could be null in skip mode cleanup
                    if (o != null)
                    {
                        String treeSize = computeTreeSize(o);
                        log.info("put: " + o.getClass().getSimpleName() + " " + o.getURI() + " " + format(o.getMaxLastModified()) + " " + treeSize);
                    }
                    else if (hs != null)
                    {
                        log.info("error put: " + hs.cname + " " + hs.skipID + " " + format(hs.lastModified));

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

                            // try to avoid DataIntegrityViolationException
                            // due
                            // to missed deletion of an observation
                            UUID curID = destObservationDAO.getID(o.getURI());
                            if (curID != null && !curID.equals(o.getID()))
                            {
                                ObservationURI oldSrc = srcObservationDAO.getURI(curID);
                                if (oldSrc == null)
                                {
                                    // missed harvesting a deletion
                                    log.info("delete: " + o.getClass().getSimpleName() + " " + format(curID) + " (ObservationURI conflict avoided)");
                                    destObservationDAO.delete(curID);
                                }
                                // else: the put below with throw a valid
                                // exception because source
                                // is not enforcing
                                // unique ID and URI
                            }
                            if (doCollisionCheck)
                            {
                                Observation cc = destObservationDAO.getShallow(o.getID());
                                log.debug("collision check: " + o.getURI() + " " + format(o.getMaxLastModified()) + " vs " + format(cc.getMaxLastModified()));
                                if (!cc.getMaxLastModified().equals(o.getMaxLastModified()))
                                    throw new IllegalStateException("detected harvesting collision: " + o.getURI() + " maxLastModified: " + format(o.getMaxLastModified()));
                            }

                            // advance the date before put as there are
                            // usually
                            // lots of fails
                            if (skipped)
                                startDate = hs.lastModified;

                            // temporary validation hack to avoid tickmarks
                            // in
                            // the keywords columns
                            CaomValidator.validateKeywords(o);

                            if (computePlaneMetadata)
                            {
                                log.debug("computePlaneMetadata: " + o.getObservationID());
                                for (Plane p : o.getPlanes())
                                    ComputeUtil.computeTransientState(o, p);
                            }

                            if (nochecksum || checkChecksumsAlt(o))
                                destObservationDAO.put(o);
                            else
                                throw new ChecksumError("mismatching checksums");

                            if (hs != null) // success in redo mode
                            {
                                log.info("delete: " + hs + " " + format(hs.lastModified));
                                harvestSkip.delete(hs);
                            }
                            else
                                harvestState.put(state);
                        }
                        else if (skipped && ow.entity == null) // observation is gone from src
                        {
                            log.info("delete: " + hs + " " + format(hs.lastModified));
                            harvestSkip.delete(hs);
                        }
                        else if (ow.entity.error != null)
                        {
                            // try to make progress on failures
                            if (state != null && ow.entity.observationState.maxLastModified != null)
                            {
                                state.curLastModified = ow.entity.observationState.maxLastModified;
                                state.curID = null; //unknown
                            }
                            throw ow.entity.error;
                        }

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                }
                catch (Throwable oops)
                {
                    lastMsg = oops.getMessage();
                    String str = oops.toString();
                    if (oops instanceof IllegalStateException)
                    {
                        if (oops.getMessage().contains("XML failed schema validation"))
                        {
                            log.error("CONTENT PROBLEM - XML failed schema validation: " + oops.getMessage());
                            ret.handled++;
                        }
                        else if (oops.getMessage().contains("failed to read"))
                        {
                            log.error("CONTENT PROBLEM - " + oops.getMessage(), oops.getCause());
                            ret.handled++;
                        }
                    }
                    else if (oops instanceof ChecksumError)
                    {
                        log.error("CONTENT PROBLEM - mismatching checksums for " + o);
                        ret.handled++;
                    }
                    else if (oops instanceof TransientException)
                    {
                        log.error("CONTENT PROBLEM - " + oops.getMessage());
                        ret.handled++;
                    }
                    else if (oops instanceof Error)
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
                    else if (oops instanceof DataIntegrityViolationException && str.contains("duplicate key value violates unique constraint \"i_observationuri\""))
                    {
                        log.error("CONTENT PROBLEM - duplicate observation: " + o.getURI());
                        ret.handled++;
                    }
                    else if (oops instanceof UncategorizedSQLException)
                    {
                        if (str.contains("spherepoly_from_array"))
                        {
                            log.error("UNDETECTED illegal polygon: " + o.getURI());
                            ret.handled++;
                        }
                        else
                            log.error("unexpected exception", oops);
                    }
                    else if (oops instanceof IllegalArgumentException && str.contains("CaomValidator") && str.contains("keywords"))
                    {
                        log.error("CONTENT PROBLEM - invalid keywords: " + o.getURI());
                        ret.handled++;
                    }
                    else
                        log.error("unexpected exception", oops);
                }
                finally
                {
                    //i++; // index over ObservationState collection
                    if (!ok && !dryrun)
                    {
                        if (o != null)
                        {
                            log.warn("failed to insert " + o + ": " + lastMsg);
                            skipMsg = o + ": " + lastMsg;
                        }
                        else
                        {
                            log.warn("failed to insert " + ow.entity.observationState.getURI().getURI() + ": " + lastMsg);
                            skipMsg = ow.entity.observationState.getURI().getURI() + ": " + lastMsg;
                        }
                        lastMsg = null;
                        destObservationDAO.getTransactionManager().rollbackTransaction();
                        log.warn("rollback: OK");
                        tTransaction += System.currentTimeMillis() - t;

                        try
                        {
                            log.debug("starting HarvestSkipURI transaction");
                            boolean putSkip = true;
                            HarvestSkipURI skip = null;
                            if (o != null)
                            {
                                skip = harvestSkip.get(source, cname, o.getURI().getURI());
                            }
                            else
                            {
                                skip = harvestSkip.get(source, cname, ow.entity.observationState.getURI().getURI());
                            }
                            log.debug("skip == " + skip);

                            if (skip == null)
                            {
                                if (o != null)
                                {
                                    skip = new HarvestSkipURI(source, cname, o.getURI().getURI(), skipMsg);
                                }
                                else
                                {
                                    skip = new HarvestSkipURI(source, cname, ow.entity.observationState.getURI().getURI(), skipMsg);
                                }
                            }
                            else
                            {
                                log.debug("skipMsg == " + skipMsg);
                                log.debug("skip.errorMessage == " + skip.errorMessage);

                                if (skipMsg != null && !skipMsg.equals(skip.errorMessage))
                                {
                                    skip.errorMessage = skipMsg; // possible
                                                                 // update
                                }
                                else
                                {
                                    log.debug("no change in status: " + hs);
                                    putSkip = false; // avoid timestamp
                                                     // update
                                }
                            }

                            destObservationDAO.getTransactionManager().startTransaction();

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
                            if (o != null)
                                destObservationDAO.delete(o.getID());

                            log.debug("committing HarvestSkipURI transaction");
                            destObservationDAO.getTransactionManager().commitTransaction();
                            log.debug("commit HarvestSkipURI: OK");
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

    private boolean checkChecksums(ObservationState os, Observation o) throws ChecksumError
    {
        try
        {
            URI calculatedUri = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("o.getURI() " + o.getURI());
            log.debug("os.getURI() " + os.getURI());
            log.debug("calculatedUri " + calculatedUri);
            log.debug("o.getAccMetaChecksum() " + o.getAccMetaChecksum());
            log.debug("os.accMetaChecksum " + os.accMetaChecksum);
            if (o != null && o.getAccMetaChecksum() != null && os != null && os.accMetaChecksum != null && calculatedUri != null
                    && o.getAccMetaChecksum().equals(os.accMetaChecksum) && o.getAccMetaChecksum().equals(calculatedUri))
            {
                return true;
            }
            return false;
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ChecksumError("no MD5 digest algorithm available");
        }
    }
    
    private boolean checkChecksumsAlt(Observation o) throws ChecksumError
    {
        try
        {
            URI calculatedChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("checkChecksumsAlt: " + o.getURI() + " -- " + o.getAccMetaChecksum() + " vs " + calculatedChecksum);
            if (o.getAccMetaChecksum() != null && o.getAccMetaChecksum().equals(calculatedChecksum))
            {
                return true;
            }
            return false;
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new ChecksumError("no MD5 digest algorithm available");
        }
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

    private void detectLoop(List<SkippedWrapperURI<ObservationResponse>> entityList)
    {
        if (entityList.size() < 2)
            return;
        SkippedWrapperURI<ObservationResponse> start = entityList.get(0);
        SkippedWrapperURI<ObservationResponse> end = entityList.get(entityList.size() - 1);
        if (skipped)
        {
            if (start.skip.lastModified.equals(end.skip.lastModified))
                throw new RuntimeException("detected infinite harvesting loop: " + HarvestSkipURI.class.getSimpleName() + " at " + format(start.skip.lastModified));
            return;
        }
        if (start.entity.observation != null && end.entity.observation != null
                && start.entity.observation.getMaxLastModified().equals(end.entity.observation.getMaxLastModified()))
        {
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName() + " at " + format(start.entity.observation.getMaxLastModified()));
        }
    }

    private List<SkippedWrapperURI<ObservationResponse>> wrap(List<ObservationResponse> obsList)
    {
        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(obsList.size());
        for (ObservationResponse wr : obsList)
        {
            ret.add(new SkippedWrapperURI<ObservationResponse>(wr, null));
        }
        return ret;
    }
    
    /*
    private List<SkippedWrapperURI<ObservationState>> wrapState(List<ObservationState> obsList)
    {
        List<SkippedWrapperURI<ObservationState>> ret = new ArrayList<SkippedWrapperURI<ObservationState>>(obsList.size());
        for (ObservationState o : obsList)
        {
            ret.add(new SkippedWrapperURI<ObservationState>(o, null));
        }
        return ret;
    }
    */
    
    private List<SkippedWrapperURI<ObservationResponse>> getSkipped(Date start)
    {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);

        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(skip.size());
        for (HarvestSkipURI hs : skip)
        {
            log.debug("getSkipped: " + hs.getSkipID());
            ObservationURI ouri = new ObservationURI(hs.getSkipID());
            ObservationResponse wr;
            if (srcObservationDAO != null)
                wr = srcObservationDAO.getAlt(ouri);
            else
                wr = srcObservationService.get(ouri);
            log.debug("response: " + wr);
            
            //if (wr != null)
            ret.add(new SkippedWrapperURI<ObservationResponse>(wr, hs));
        }
        return ret;
    }
    /*
    private List<SkippedWrapperURI<ObservationState>> getSkippedState(Date start)
    {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);

        List<SkippedWrapperURI<ObservationState>> ret = new ArrayList<SkippedWrapperURI<ObservationState>>(skip.size());
        for (HarvestSkipURI hs : skip)
        {
            ObservationState o = null;
            log.debug("getSkipped: " + hs.getSkipID());
            log.debug("start: " + start);

            ObservationResponse wr = srcObservationService.get(src.getCollection(), hs.getSkipID(), start);

            if (wr != null && wr.getObservationState() != null)
                o = wr.getObservationState();

            if (o != null)
            {
                ret.add(new SkippedWrapperURI<ObservationState>(o, hs));
            }
        }
        return ret;
    }
    */
    
    @Override
    protected void initHarvestState(DataSource ds, Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest.getDatabase(), dest.getSchema(), batchSize);
    }
}