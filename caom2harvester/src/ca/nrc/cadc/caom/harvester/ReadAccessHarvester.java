
package ca.nrc.cadc.caom.harvester;

import ca.nrc.cadc.caom.harvester.state.HarvestSkip;
import ca.nrc.cadc.caom.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.persistence.DatabaseReadAccessDAO;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Harvest a single type of entity in order of lastModified date.
 * 
 * @author pdowler
 */
public class ReadAccessHarvester extends Harvester
{
    private static Logger log = Logger.getLogger(ReadAccessHarvester.class);

    private DatabaseReadAccessDAO srcAccessDAO;
    private DatabaseReadAccessDAO destAccessDAO;

    /**
     * Harvest ReadAccess tuples.
     * @param src source server.database.schema
     * @param dest destination server.database.schema
     * @param entityClass the type of entity to harvest
     * @param batchSize ignored, always full list
     * @param full ignored, always in lastModfied order
     * @param restart discard harvest state and start at beginning
     * @throws IOException
     */
    public ReadAccessHarvester(Class entityClass, String[] src, String[] dest, Integer batchSize, boolean full, boolean dryrun)
        throws IOException
    {
        super(entityClass, src, dest, batchSize, full, dryrun);
    }

    private void init()
        throws IOException
    {
        Map<String,Object> config1 = getConfigDAO(src);
        Map<String,Object> config2 = getConfigDAO(dest);
        this.srcAccessDAO = new DatabaseReadAccessDAO();
        srcAccessDAO.setConfig(config1);
        this.destAccessDAO = new DatabaseReadAccessDAO();
        destAccessDAO.setConfig(config2);
        destAccessDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destAccessDAO.getDataSource(), entityClass);
    }

    private void close()
        throws IOException
    {
        // TODO
    }

    public void run()
    {
        log.info("START: " + entityClass.getSimpleName());
        try
        {
            init();
        }
        catch(Throwable oops)
        {
            log.error("failed to init connections and state", oops);
            return;
        }

        boolean go = true;
        while (go)
        {
            Progress num = doit();
            if (num.found > 0)
                log.info("finished batch: " + num);
            if (num.failed > num.found/2) // more than half failed
            {
                log.warn("failure rate is quite high: " + num.failed + "/" + num.found);
                num.abort = true;
            }
            if (num.abort)
                log.error("batched aborted");
            go = (num.found > 0 && !num.abort && !num.done);
            if (num.found < batchSize)
                go = false;
            full = false; // do not start at min(lastModified) again
            if (dryrun)
                go = false; // no state update -> infinite loop
            
            // hack to do one batch for testing
            //go = false;
        }
        try
        {
            close();
        }
        catch(Throwable oops)
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
        @Override
        public String toString() { return found + " ingested: " + ingested + " failed: " + failed; }
    }

    private Progress doit()
    {
        log.info("batch: " + entityClass.getSimpleName());
        Progress ret = new Progress();
        ReadAccess cur = null;
        try
        {
            HarvestState state = harvestState.get(source, cname);
            log.info("last harvest: " + format(state.curLastModified));

            Date start = state.curLastModified;
            if (full)
                start = null;
            List<ReadAccess> entityList = srcAccessDAO.getList(entityClass, start, batchSize);

            // check for infinite loop before we process this batch
            if (entityList.size() == batchSize.intValue())
            {
                ReadAccess first = entityList.get(0);
                ReadAccess last = entityList.get(entityList.size() - 1);
                if ( first.compareTo(last) == 0)
                    throw new RuntimeException("detected infinite harvesting loop: "
                        + entityClass.getSimpleName() + " at " + first.getLastModified());
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());

            ListIterator<ReadAccess> iter = entityList.listIterator();
            while ( iter.hasNext() )
            {
                ReadAccess ra = iter.next();
                cur = ra;
                iter.remove(); // allow garbage collection asap

                if (!dryrun)
                    destAccessDAO.getTransactionManager().startTransaction();
                boolean ok = false;
                try
                {
                    log.info("put: " + ra);
                    if (!dryrun)
                    {
                        state.curLastModified = ra.getLastModified();
                        state.curID = ra.getID();

                        destAccessDAO.put(ra);
                        harvestState.put(state);

                        log.debug("committing transaction");
                        destAccessDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                }
                catch(Throwable t)
                {
                    log.error("BUG - failed to put ReadAccess", t);
                }
                finally
                {
                    if (!ok && !dryrun)
                    {
                        log.warn("failed to process " + ra + ": trying to rollback the transaction");
                        destAccessDAO.getTransactionManager().rollbackTransaction();
                        log.warn("rollback: OK");

                        // track failures where possible
                        try
                        {
                            log.debug("starting harvestSkip transaction");
                            HarvestSkip skip = harvestSkip.get(source, cname, ra.getID());
                            if (skip == null)
                                skip = new HarvestSkip(source, cname, ra.getID());
                            destAccessDAO.getTransactionManager().startTransaction();
                            log.info("skip: " + skip);

                            harvestState.put(state);
                            harvestSkip.put(skip);
                            
                            log.debug("committing harvestSkip transaction");
                            destAccessDAO.getTransactionManager().commitTransaction();
                            log.debug("commit harvestSkip: OK");
                        }
                        catch(Throwable oops)
                        {
                            log.warn("failed to insert via HarvestSkip", oops);
                            destAccessDAO.getTransactionManager().rollbackTransaction();
                            log.warn("rollback harvestSkip: OK");
                        }
                        ret.failed++;
                    }
                }
            }
            if (ret.abort)
                return ret;
        }
        finally
        {
            log.debug("DONE");
        }
        return ret;
    }

    /*
    private void detectLoop(ReadAccess curBatchLeader)
    {
        log.warn("check for loop: " + prevBatchLeader + " vs " + curBatchLeader);
        if (prevBatchLeader != null)
        {
            log.debug("check for loop: " + prevBatchLeader + " vs " + curBatchLeader);
            if (prevBatchLeader != null)
            {
                ReadAccess pde = (ReadAccess) prevBatchLeader;
                ReadAccess cde = (ReadAccess) curBatchLeader;
                log.warn("check for infinite loop: " + pde.getAssetID() + "," + pde.getGroupID() + "," + pde.getLastModified().getTime()
                        + " vs " + cde.getAssetID() + "," + cde.getGroupID() + "," + cde.getLastModified().getTime());
                if (pde.getAssetID().equals(cde.getAssetID()) && pde.getGroupID().equals(cde.getGroupID()))
                    throw new RuntimeException("detected infinite harvesting loop by ID: "
                        + entityClass.getSimpleName() + " at " + cde.getAssetID() + "," + cde.getGroupID());
                if (pde.getLastModified().equals(cde.getLastModified()))
                    throw new RuntimeException("detected infinite harvesting loop: "
                        + entityClass.getSimpleName() + " at " + cde.getLastModified());
            }
        }
    }
    */
}
