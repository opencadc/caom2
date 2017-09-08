package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.AbstractDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.caom2.persistence.DeletedEntityDAO;
import ca.nrc.cadc.caom2.persistence.TransactionManager;

/**
 * Harvest and perform deletions of CaomEntity instances.
 *
 * @author pdowler
 */
public class DeletionHarvester extends Harvester implements Runnable
{

    private static Logger log = Logger.getLogger(DeletionHarvester.class);

    private AbstractDAO deletedDAO;
    private WrapperDAO entityDAO;
    private TransactionManager txnManager;

    private boolean initHarvestState;
    private Date initDate;
    private boolean service = false;

    private String uri = null;
    private String collection = null;
    private int nthreads = 1;

    /**
     * Constructor.
     *
     * @param src
     *            source server.database.schema
     * @param dest
     *            destination server.database.schema
     * @param entityClass
     *            the class specifying what should be deleted
     * @param batchSize
     *            ignored, always full list
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
     * @throws IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     * @throws NumberFormatException
     *             NumberFormatException
     */
    public DeletionHarvester(Class<?> entityClass, HarvestResource src, HarvestResource dest, Integer batchSize, boolean dryrun) throws IOException, NumberFormatException, URISyntaxException
    {
        super(entityClass, src, dest, batchSize, false, dryrun);
        service = false;
    }

    /**
     * Initialise harvest state with the current date.
     *
     * @param initHarvestState
     *            value for this attribute
     */
    public void setInitHarvestState(boolean initHarvestState)
    {
        this.initHarvestState = initHarvestState;
        if (initHarvestState)
            this.initDate = new Date(); // timestamp at startup, not when run
    }

    /**
     * initialize of the harvester
     *
     * @param uri
     *            uri to be used
     * @param collection
     *            collection to work on
     * @param threads
     *            number of threads to be used
     * @throws IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    private void init(String uri, String collection, int threads) throws IOException, URISyntaxException
    {

        Map<String, Object> config2 = getConfigDAO(dest);

        this.deletedDAO = new ServiceDeletedEntityDAO<DeletedObservation>();

        if (DeletedObservation.class.equals(entityClass))
        {
            ObservationDAO dao = new ObservationDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, null);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedPlaneMetaReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedPlaneDataReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneDataReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedObservationMetaReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, ObservationMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else
            throw new UnsupportedOperationException("unsupported class: " + entityClass.getName());
    }

    /**
     * initialize of the harvester
     *
     * @throws IOException
     */
    private void init() throws IOException
    {
        Map<String, Object> config1 = getConfigDAO(src);
        Map<String, Object> config2 = getConfigDAO(dest);

        this.deletedDAO = new DeletedEntityDAO<DeletedObservation>();
        deletedDAO.setConfig(config1);

        if (DeletedObservation.class.equals(entityClass))
        {
            ObservationDAO dao = new ObservationDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, null);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedPlaneMetaReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedPlaneDataReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneDataReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else if (DeletedObservationMetaReadAccess.class.equals(entityClass))
        {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, ObservationMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        }
        else
            throw new UnsupportedOperationException("unsupported class: " + entityClass.getName());
    }

    /**
     * cleanup connections and state
     *
     * @throws IOException
     */
    private void close() throws IOException
    {
        // TODO
    }

    /**
     * invoke delete(Long) method on an arbitrary object via reflection
     *
     */
    private class WrapperDAO
    {

        private Object dao;
        private Method deleteMethod;
        private Class<?> targetClass;

        WrapperDAO(Object dao, Class<?> targetClass)
        {
            this.dao = dao;
            this.targetClass = targetClass;

            try
            {
                if (targetClass != null)
                    this.deleteMethod = dao.getClass().getMethod("delete", Class.class, UUID.class);
                else
                    this.deleteMethod = dao.getClass().getMethod("delete", UUID.class);

            }
            catch (NoSuchMethodException bug)
            {
                throw new RuntimeException("BUG", bug);
            }
            log.debug("created wrapper to call " + dao.getClass().getSimpleName() + ".delete(Long)");
        }

        public void delete(UUID id)
        {
            log.debug("invoking " + deleteMethod + " with id=" + id);
            try
            {
                if (targetClass != null)
                    deleteMethod.invoke(dao, targetClass, id);
                else
                    deleteMethod.invoke(dao, id);
            }
            catch (IllegalAccessException bug)
            {
                throw new RuntimeException("BUG", bug);
            }
            catch (InvocationTargetException bug)
            {
                throw new RuntimeException("BUG", bug);
            }
        }

        @Override
        public String toString()
        {
            return deleteMethod.toString();
        }
    }

    /**
     * run
     */
    @Override
    public void run()
    {
        log.info("START: " + entityClass.getSimpleName());
        try
        {
            if (service)
            {
                init(uri, collection, nthreads);
            }
            else
            {
                init();
            }
        }
        catch (Throwable oops)
        {
            throw new RuntimeException("failed to init connections and state", oops);
        }
        boolean go = true;
        while (go)
        {
            Progress num = doit();
            if (num.found > 0)
                log.info("finished batch: " + num);
            if (num.failed > num.found / 2) // more than half failed
            {
                log.warn("failure rate is quite high: " + num.failed + "/" + num.found);
                num.abort = true;
            }
            if (num.abort)
                log.error("batched aborted");
            go = (!num.abort && !num.done);
            full = false; // do not start at min(lastModified) again
            if (dryrun)
                go = false; // no state update -> infinite loop
        }
        try
        {
            close();
        }
        catch (Throwable oops)
        {
            log.error("failed to cleanup connections and state", oops);
            return;
        }
        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    /**
     * class that does the work
     *
     */
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

    Object prevBatchLeader = null;

    /**
     * Does the work
     *
     * @return progress status
     */
    @SuppressWarnings("unchecked")
    private Progress doit()
    {
        log.info("batch: " + entityClass.getSimpleName());
        Progress ret = new Progress();

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null)
            expectedNum = batchSize.intValue();

        try
        {
            HarvestState state = harvestState.get(source, cname);
            log.info("last harvest: " + format(state.curLastModified));

            if (initHarvestState && state.curLastModified == null)
            {
                state.curLastModified = initDate;
                harvestState.put(state);
                state = harvestState.get(source, cname);
                log.info("harvest state initialised to: " + df.format(state.curLastModified));
            }

            Date start = state.curLastModified;
            if (full)
                start = null;
            Date end = null;

            // lastModified is maintained in the DB so we do not need this
            // end = new Date(System.currentTimeMillis() - 5*60000L); // 5
            // minutes ago

            List<DeletedEntity> entityList = null;
            if (!service)
            {
                entityList = ((DeletedEntityDAO<DeletedEntity>) deletedDAO).getList(entityClass, start, end, batchSize);
            }
            else
            {
                entityList = ((ServiceDeletedEntityDAO<DeletedEntity>) deletedDAO).getList(entityClass, start, end, batchSize);
            }

            if (entityList.size() == expectedNum)
                detectLoop(entityList);

            ret.found = entityList.size();
            log.info("found: " + entityList.size());
            ListIterator<DeletedEntity> iter = entityList.listIterator();
            while (iter.hasNext())
            {
                DeletedEntity de = iter.next();
                iter.remove(); // allow garbage collection asap

                if (de.id.equals(state.curID))
                {
                    log.info("skip: " + de.getClass().getSimpleName() + " " + de.id + " -- was end of last batch");
                    break;
                }

                if (!dryrun)
                    txnManager.startTransaction();
                boolean ok = false;
                try
                {
                    log.info("put: " + de.getClass().getSimpleName() + " " + de.id + " " + format(de.lastModified));
                    if (!dryrun)
                    {
                        state.curLastModified = de.lastModified;
                        state.curID = de.id;

                        // keep a record of the deletion
                        // dORM.put(de);

                        // perform the actual deletion
                        entityDAO.delete(de.id);

                        // track progress
                        harvestState.put(state);

                        log.debug("committing transaction");
                        txnManager.commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                }
                catch (Throwable t)
                {
                    log.error("unexpected exception", t);
                }
                finally
                {
                    if (!ok && !dryrun)
                    {
                        log.warn("failed to process " + de + ": trying to rollback the transaction");
                        txnManager.rollbackTransaction();
                        log.warn("rollback: OK");
                        ret.abort = true;
                    }
                }
            }
            if (ret.found < expectedNum)
            {
                ret.done = true;
                if (state != null && state.curLastModified != null && ret.found > 0)
                {
                    // tweak HarvestState so we don't keep picking up the same
                    // one
                    Date n = new Date(state.curLastModified.getTime() + 1L); // 1
                                                                             // ms
                                                                             // ahead
                    Date now = new Date();
                    if (now.getTime() - n.getTime() > 600 * 1000L) // 10 minutes
                                                                   // aka very
                                                                   // old
                        n = new Date(state.curLastModified.getTime() + 100L); // 100
                                                                              // ms
                                                                              // ahead
                    state.curLastModified = n;
                    log.info("reached last " + entityClass.getSimpleName() + ": setting curLastModified to " + format(state.curLastModified));
                    harvestState.put(state);
                }
            }
        }
        finally
        {
            log.debug("DONE");
        }
        return ret;
    }

    /**
     * detects loops
     *
     * @param entityList
     *            list of entities to detect loops with
     */
    private void detectLoop(List<DeletedEntity> entityList)
    {
        if (entityList.size() < 2)
            return;
        DeletedEntity start = entityList.get(0);
        DeletedEntity end = entityList.get(entityList.size() - 1);
        if (start.lastModified.equals(end.lastModified))
            throw new RuntimeException("detected infinite harvesting loop: " 
                    + entityClass.getSimpleName() + " at " + 
                    format(start.lastModified));

    }
}