
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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.DeletedEntityDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.TransactionManager;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Harvest and perform deletions of observations.
 *
 * @author pdowler
 */
public class DeletionHarvester extends Harvester implements Runnable {

    private static Logger log = Logger.getLogger(DeletionHarvester.class);

    private DeletedEntityDAO deletedDAO;
    private RepoClient repoClient;
    
    private ObservationDAO obsDAO;
    private TransactionManager txnManager;

    private boolean initHarvestState;
    private Date initDate;

    /**
     * Constructor.
     *
     * @param src
     * source server.database.schema
     * @param dest
     * destination server.database.schema
     * @param entityClass
     * the class specifying what should be deleted
     * @param batchSize
     * ignored, always full list
     * @param dryrun
     * true if no changed in the data base are applied during the process
     * @throws IOException
     * IOException
     * @throws URISyntaxException
     * URISyntaxException
     * @throws NumberFormatException
     * NumberFormatException
     */
    public DeletionHarvester(Class<?> entityClass, HarvestResource src, HarvestResource dest,
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
    private void init() throws IOException, URISyntaxException {
        // source
        if (src.getDatabaseServer() != null) {
            Map<String, Object> config1 = getConfigDAO(src);
            this.deletedDAO = new DeletedEntityDAO();
            deletedDAO.setConfig(config1);
        } else {
            this.repoClient = new RepoClient(src.getResourceID(), 1);
        }
        
        // destination
        Map<String, Object> config2 = getConfigDAO(dest);
        this.obsDAO = new ObservationDAO();
        obsDAO.setConfig(config2);
        this.txnManager = obsDAO.getTransactionManager();
        initHarvestState(obsDAO.getDataSource(), entityClass);
        
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
            List<DeletedObservation> entityList = null;
            if (deletedDAO != null) {
                entityList = deletedDAO.getList(src.getCollection(), start, end, batchSize);
            } //else {
            //    entityList = repoClient.getDeleted(src.getCollection(), start, end, batchSize);
            //}

            if (entityList.size() == expectedNum) {
                detectLoop(entityList);
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());
            ListIterator<DeletedObservation> iter = entityList.listIterator();
            while (iter.hasNext()) {
                DeletedObservation de = iter.next();
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
                        obsDAO.delete(de.getID());

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
    private void detectLoop(List<DeletedObservation> entityList) {
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
