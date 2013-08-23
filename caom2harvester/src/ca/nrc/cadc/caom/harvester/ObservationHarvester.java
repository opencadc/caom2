
package ca.nrc.cadc.caom.harvester;

import ca.nrc.cadc.caom.harvester.state.HarvestSkip;
import ca.nrc.cadc.caom.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.date.DateUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;

/**
 *
 * @author pdowler
 */
public class ObservationHarvester extends Harvester
{
    private static Logger log = Logger.getLogger(ObservationHarvester.class);
    
    private boolean interactive;

    private DatabaseObservationDAO srcObservationDAO;
    private DatabaseObservationDAO destObservationDAO;
    
    private boolean skipped;

    private ObservationHarvester() { }
    
    public ObservationHarvester(String[] src, String[] dest, Integer batchSize, boolean full, boolean dryrun)
        throws IOException
    {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        Date d = new Date();
    }
    
    public void setSkipped(boolean skipped)
    {
        this.skipped = skipped;
    }
    
    public void setInteractive(boolean enabled)
    {
        this.interactive = enabled;
    }

    private void init()
        throws IOException
    {
        Map<String,Object> config1 = getConfigDAO(src);
        Map<String,Object> config2 = getConfigDAO(dest);
        this.srcObservationDAO = new DatabaseObservationDAO();
        srcObservationDAO.setConfig(config1);
        this.destObservationDAO = new DatabaseObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    private void close()
        throws IOException
    {
        // TODO
    }
    
    public void run()
    {
        log.info("START: " + Observation.class.getSimpleName());
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
            /*
            double failFrac = ((double) num.failed) / ((double) num.found);
            if (failFrac > 0.5)
            {
                log.warn("failure rate is quite high: " + num.failed + "/" + num.found);
                num.abort = true;
            }
            */
            if (num.abort)
                log.error("batched aborted");
            go = (num.found > 0 && !num.abort && !num.done);
            if (batchSize != null && num.found < batchSize.intValue()/2)
                go = false;
            full = false; // do not start at beginning again
            if (dryrun)
                go = false; // no state update -> infinite loop
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
        log.info("batch: " + Observation.class.getSimpleName());

        Progress ret = new Progress();

        BufferedReader stdin = null;
        if (interactive)
        {
            stdin = new BufferedReader(new InputStreamReader(System.in));
        }
        
        long t = System.currentTimeMillis();
        long tState = -1;
        long tQuery = -1;
        long tCollision = -1;
        long tTransaction = -1;
        
        Observation cur = null; // for use in the catch clause
        
        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null)
            expectedNum = batchSize.intValue();
        try
        {
            System.gc(); // hint
            t = System.currentTimeMillis();

            HarvestState state = harvestState.get(source, Observation.class.getSimpleName());
            
            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            Date start = state.curLastModified;
            if (full)
                start = null;
            List<ObservationWrapper> entityList = null;
            
            if (skipped)
                entityList = getSkipped(start);
            else
            {
                Date end = new Date(System.currentTimeMillis() - 5*60000L); // 5 minutes ago
                log.info("harvest window: " + format(start) + " :: " + format(end));
                List<Observation> tmp = srcObservationDAO.getList(Observation.class, start, end, batchSize);
                entityList = wrap(tmp);
            }

            if (entityList.size() == expectedNum)
                detectLoop(entityList);
            
            // avoid re-processing the last successful one stored in HarvestState
            if ( !entityList.isEmpty() )
            {
                ListIterator<ObservationWrapper> iter = entityList.listIterator();
                Observation curBatchLeader = iter.next().obs;
                log.info("currentBatch: " + curBatchLeader.getID() + " " + curBatchLeader.getMaxLastModified());
                log.info("harvestState: " + state.curID + " " + state.curLastModified);
                if (curBatchLeader.getID().equals(state.curID)                                 // same obs as last time
                        && curBatchLeader.getMaxLastModified().equals(state.curLastModified) ) // not modified since
                {
                    iter.remove(); // processed in last batch but picked up by lastModified query
                    expectedNum--;
                }
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());
            
            tQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();
            
            ListIterator<ObservationWrapper> iter = entityList.listIterator();
            while ( iter.hasNext() )
            {
                ObservationWrapper ow = iter.next();
                Observation o = ow.obs;
                cur = o; // for use in catch
                HarvestSkip hs = ow.skip;
                iter.remove(); // allow garbage collection during loop
                
                String lastMsg = null;
                
                if (!dryrun)
                {
                    log.debug("starting transaction");
                    destObservationDAO.getTransactionManager().startTransaction();
                }
                boolean ok = false;
                try
                {
                    log.info("put: " + o.getClass().getSimpleName() + " " + o.getID() + " " + format(o.getMaxLastModified()));
                    if (!dryrun) 
                    {
                        state.curLastModified = o.getMaxLastModified();
                        state.curID = o.getID();

                        destObservationDAO.put(o);
                        harvestState.put(state);
                        if (hs != null)
                            harvestSkip.delete(hs);

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                }
                //catch(CollisionException cex) // found entities with same CaomEntity.getID but different source.hashCode
                //{
                //    lastMsg = "ID collision: " + cex.getMessage();
                //}
                catch(Throwable oops)
                {
                    lastMsg = "unexpected: " + oops.getMessage();
                    String str = oops.toString();
                    if (oops instanceof BadSqlGrammarException)
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
                    else if (oops instanceof DataIntegrityViolationException)
                    {
                        if (str.contains("duplicate key value violates unique constraint \"i_observationURI\""))
                        {
                            log.error("CONTENT PROBLEM - duplicate observation: "
                                    + cur.getCollection() + "," + cur.getObservationID());
                        }
                        else
                            log.error("unexpected exception", oops);
                    }
                    else if  (oops instanceof UncategorizedSQLException)
                    {
                        if (str.contains("spherepoly_from_array: a line segment overlaps"))
                        {
                            log.error("UNDETECTED illegal polygon: "
                                    + cur.getCollection() + "," + cur.getObservationID());
                        }
                        else
                            log.error("unexpected exception", oops);
                    }
                    else
                        log.error("unexpected exception", oops);
                }
                finally
                {
                    if (!ok && !dryrun)
                    {
                        log.warn("failed to insert " + o + ": " + lastMsg);
                        lastMsg = null;
                        destObservationDAO.getTransactionManager().rollbackTransaction();
                        log.warn("rollback: OK");
                        tTransaction = System.currentTimeMillis() - t;
                        
                        // track failures
                        try
                        {
                            log.debug("starting HarvestSkip transaction");
                            HarvestSkip skip = harvestSkip.get(source, cname, o.getID());
                            if (skip == null)
                                skip = new HarvestSkip(source, cname, o.getID());
                            log.info(skip);

                            destObservationDAO.getTransactionManager().startTransaction();
                            // track the harvest state progress
                            harvestState.put(state);
                            // track the fail
                            harvestSkip.put(skip);
                            // TBD: delete previous version of obs?
                            //destObservationDAO.delete(o.getID());
                            log.debug("committing HarvestSkip transaction");
                            destObservationDAO.getTransactionManager().commitTransaction();
                            log.debug("commit HarvestSkip: OK");
                        }
                        catch(Throwable oops)
                        {
                            log.warn("failed to insert HarvestSkip", oops);
                            destObservationDAO.getTransactionManager().rollbackTransaction();
                            log.warn("rollback HarvestSkip: OK");
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
                                }
                                else
                                    System.out.println("unexpected input: " + str);
                            }
                        }
                        catch (IOException e)
                        {

                        }
                    }
                }
                if (ret.abort)
                    return ret;
            }
            if (ret.found < expectedNum)
                ret.done = true;
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

    private void detectLoop(List<ObservationWrapper> entityList)
    {
        if (entityList.size() < 2)
            return;
        ObservationWrapper start = entityList.get(0);
        ObservationWrapper end = entityList.get(entityList.size() - 1);
        if (start.obs.getMaxLastModified().equals(end.obs.getMaxLastModified()))
        {
            throw new RuntimeException("detected infinite harvesting loop: "
                    + entityClass.getSimpleName() + " at " + format(start.obs.getMaxLastModified()));
        }
    }
    
    private class ObservationWrapper
    {
        Observation obs;
        HarvestSkip skip;
        ObservationWrapper(Observation o, HarvestSkip hs)
        {
            this.obs = o;
            this.skip = hs;
        }
    }
    
    private List<ObservationWrapper> wrap(List<Observation> obsList)
    {
        List<ObservationWrapper> ret = new ArrayList<ObservationWrapper>(obsList.size());
        for (Observation o : obsList)
        {
            ret.add(new ObservationWrapper(o, null));
        }
        return ret;
    }
    
    private List<ObservationWrapper> getSkipped(Date start)
    {
        
        List<HarvestSkip> skip = harvestSkip.get(source, cname);
        List<ObservationWrapper> ret = new ArrayList<ObservationWrapper>(skip.size());
        log.warn("getSkipped: found " + skip.size());
        for (HarvestSkip hs : skip)
        {
            Observation o = srcObservationDAO.get(hs.getSkipID());
            if (o != null)
                ret.add(new ObservationWrapper(o, hs));
        }
        return ret;
    }
}
