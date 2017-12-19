/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
*/

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.DeletedEntityDAO;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.caom2.persistence.TransactionManager;
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

/**
 * Harvest and perform deletions of ReadAccess tuples.
 * 
 * @author pdowler
 */
public class ReadAccessDeletionHarvester extends Harvester implements Runnable {
    private static final Logger log = Logger.getLogger(ReadAccessDeletionHarvester.class);

    private DeletedEntityDAO deletedDAO;
    
    private WrapperDAO entityDAO;
    private TransactionManager txnManager;

    private boolean initHarvestState;
    private Date initDate;
    
    public ReadAccessDeletionHarvester(Class<?> entityClass, HarvestResource src, HarvestResource dest,
            Integer batchSize, boolean dryrun) throws IOException, NumberFormatException, URISyntaxException {
        super(entityClass, src, dest, batchSize, false, dryrun);
    }
    
    /**
     * Initialise harvest state with the current date.
     *
     * @param initHarvestState
     * value for this attribute
     */
    public void setInitHarvestState(boolean initHarvestState) {
        this.initHarvestState = initHarvestState;
        if (initHarvestState) {
            this.initDate = new Date(); // timestamp at startup, not when run
        }
    }

    /**
     * initialize of the harvester
     *
     * @param uri
     * uri to be used
     * @param collection
     * collection to work on
     * @param threads
     * number of threads to be used
     * @throws IOException
     * IOException
     * @throws URISyntaxException
     * URISyntaxException
     */
    private void init(int threads) throws IOException, URISyntaxException {
        // source
        if (src.getDatabaseServer() != null) {
            Map<String, Object> config1 = getConfigDAO(src);
            this.deletedDAO = new DeletedEntityDAO();
            deletedDAO.setConfig(config1);
        }
        
        // destination
        Map<String, Object> config2 = getConfigDAO(dest);
        if (DeletedPlaneMetaReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else if (DeletedPlaneDataReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneDataReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else if (DeletedObservationMetaReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, ObservationMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else {
            throw new UnsupportedOperationException("unsupported class: " + entityClass.getName());
        }
    }

    /**
     * initialize of the harvester
     *
     * @throws IOException
     */
    private void init() throws IOException {
        Map<String, Object> config1 = getConfigDAO(src);
        Map<String, Object> config2 = getConfigDAO(dest);

        this.deletedDAO = new DeletedEntityDAO();
        deletedDAO.setConfig(config1);

        if (DeletedPlaneMetaReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else if (DeletedPlaneDataReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, PlaneDataReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else if (DeletedObservationMetaReadAccess.class.equals(entityClass)) {
            ReadAccessDAO dao = new ReadAccessDAO();
            dao.setConfig(config2);
            this.txnManager = dao.getTransactionManager();
            this.entityDAO = new WrapperDAO(dao, ObservationMetaReadAccess.class);
            initHarvestState(dao.getDataSource(), entityClass);
        } else {
            throw new UnsupportedOperationException("unsupported class: " + entityClass.getName());
        }
    }

    /**
     * cleanup connections and state
     *
     * @throws IOException
     */
    private void close() throws IOException {
        // TODO
    }

    /**
     * invoke delete(UUID) method on an arbitrary object via reflection
     *
     */
    private class WrapperDAO {

        private Object dao;
        private Method deleteMethod;
        private Class<?> targetClass;

        WrapperDAO(Object dao, Class<?> targetClass) {
            this.dao = dao;
            this.targetClass = targetClass;

            try {
                this.deleteMethod = dao.getClass().getMethod("delete", Class.class, UUID.class);
            } catch (NoSuchMethodException bug) {
                throw new RuntimeException("BUG", bug);
            }
            log.debug("created wrapper to call " + dao.getClass().getSimpleName() + ".delete(Class,UUID)");
        }

        public void delete(UUID id) {
            log.debug("invoking " + deleteMethod + " with id=" + id);
            try {
                deleteMethod.invoke(dao, targetClass, id);
            } catch (IllegalAccessException bug) {
                throw new RuntimeException("BUG", bug);
            } catch (InvocationTargetException bug) {
                throw new RuntimeException("BUG", bug);
            }
        }

        @Override
        public String toString() {
            return deleteMethod.toString();
        }
    }

    /**
     * run
     */
    @Override
    public void run() {
        log.info("START: " + entityClass.getSimpleName());
        try {
            init();
        } catch (Throwable oops) {
            throw new RuntimeException("failed to init connections and state", oops);
        }
        boolean go = true;
        while (go) {
            Progress num = doit();
            if (num.found > 0) {
                log.info("finished batch: " + num);
            }
            if (num.failed > num.found / 2) {
                log.warn("failure rate is quite high: " + num.failed + "/" + num.found);
                num.abort = true;
            }
            if (num.abort) {
                log.error("batched aborted");
            }
            go = (!num.abort && !num.done);
            full = false; // do not start at min(lastModified) again
            if (dryrun) {
                go = false; // no state update -> infinite loop
            }
        }
        try {
            close();
        } catch (Throwable oops) {
            log.error("failed to cleanup connections and state", oops);
            return;
        }
        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    /**
     * class that does the work
     *
     */
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

    Object prevBatchLeader = null;

    /**
     * Does the work
     *
     * @return progress status
     */
    @SuppressWarnings("unchecked")
    private Progress doit() {
        log.info("batch: " + entityClass.getSimpleName());
        Progress ret = new Progress();

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null) {
            expectedNum = batchSize.intValue();
        }

        try {
            HarvestState state = harvestStateDAO.get(source, cname);
            log.info("last harvest: " + format(state.curLastModified));

            if (initHarvestState && state.curLastModified == null) {
                state.curLastModified = initDate;
                harvestStateDAO.put(state);
                state = harvestStateDAO.get(source, cname);
                log.info("harvest state initialised to: " + df.format(state.curLastModified));
            }

            Date start = state.curLastModified;
            if (full) {
                start = null;
            }
            Date end = null;

            // lastModified is maintained in the DB so we do not need this
            // end = new Date(System.currentTimeMillis() - 5*60000L); // 5
            // minutes ago
            List<DeletedEntity> entityList = deletedDAO.getList(entityClass, start, end, batchSize);

            if (entityList.size() == expectedNum) {
                detectLoop(entityList);
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());
            ListIterator<DeletedEntity> iter = entityList.listIterator();
            while (iter.hasNext()) {
                DeletedEntity de = iter.next();
                iter.remove(); // allow garbage collection asap

                if (de.getID().equals(state.curID)) {
                    log.info("skip: " + de.getClass().getSimpleName() + " " + de.getID() + " -- was end of last batch");
                    break;
                }

                if (!dryrun) {
                    txnManager.startTransaction();
                }
                boolean ok = false;
                try {
                    log.info("put: " + de.getClass().getSimpleName() + " " + de.getID() + " " + format(de.getLastModified()));
                    if (!dryrun) {
                        state.curLastModified = de.getLastModified();
                        state.curID = de.getID();

                        // perform the actual deletion
                        entityDAO.delete(de.getID());

                        // track progress
                        harvestStateDAO.put(state);

                        log.debug("committing transaction");
                        txnManager.commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                } catch (Throwable t) {
                    log.error("unexpected exception", t);
                } finally {
                    if (!ok && !dryrun) {
                        log.warn("failed to process " + de + ": trying to rollback the transaction");
                        txnManager.rollbackTransaction();
                        log.warn("rollback: OK");
                        ret.abort = true;
                    }
                }
            }
            if (ret.found < expectedNum) {
                ret.done = true;
                if (state != null && state.curLastModified != null && ret.found > 0) {
                    // tweak HarvestState so we don't keep picking up the same
                    // one
                    Date n = new Date(state.curLastModified.getTime() + 1L); // 1
                    // ms
                    // ahead
                    Date now = new Date();
                    if (now.getTime() - n.getTime() > 600 * 1000L) {
                        n = new Date(state.curLastModified.getTime() + 100L);
                    }
                    // ahead
                    state.curLastModified = n;
                    log.info("reached last " + entityClass.getSimpleName() + ": setting curLastModified to " + format(state.curLastModified));
                    harvestStateDAO.put(state);
                }
            }
        } finally {
            log.debug("DONE");
        }
        return ret;
    }

    /**
     * detects loops
     *
     * @param entityList
     * list of entities to detect loops with
     */
    private void detectLoop(List<DeletedEntity> entityList) {
        if (entityList.size() < 2) {
            return;
        }
        DeletedEntity start = entityList.get(0);
        DeletedEntity end = entityList.get(entityList.size() - 1);
        if (start.getLastModified().equals(end.getLastModified())) {
            throw new RuntimeException("detected infinite harvesting loop: "
                    + entityClass.getSimpleName() + " at "
                    + format(start.getLastModified()));
        }

    }
}
