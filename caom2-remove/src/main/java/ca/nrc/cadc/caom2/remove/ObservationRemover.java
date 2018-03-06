/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.caom2.remove;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Remove observations and related harvest state records for designated collection
 * @author jeevesh
 */
public class ObservationRemover implements Runnable {

    private static Logger log = Logger.getLogger(ObservationRemover.class);

    private ObservationDAO destObservationDAO;

    // These used to exist in a base class that handled
    // database connections. Brought in here because there was no other harvester-type classes needed.
    // all DAOs in this class are using the same data source.
    protected Integer batchSize;
    protected HarvestResource src;
    protected HarvestResource target;

    HarvestSkipURIDAO harvestSkipDAO = null;

    public ObservationRemover(HarvestResource target, HarvestResource src, Integer batchSize)
        throws IOException, URISyntaxException {
        this.target = target;
        this.src = src;
        this.batchSize = batchSize;
        init();
    }

    private void init() throws IOException, URISyntaxException {
        Map<String, Object> config2 = getConfigDAO(target);
        this.destObservationDAO = new ObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setOrigin(false); // copy as-is
    }

    @Override
    public void run() {
        log.info("START: " + Observation.class.getSimpleName());

        boolean go = true;
        while (go) {
            Progress num = doit();

            if (num.found > 0) {
                log.debug("***************** finished batch: " + num + " *******************");
            }

            if (num.abort) {
                log.error("batched aborted");
            }
            go = (num.found > 0 && !num.abort && !num.done);
            if (batchSize != null && num.found < batchSize.intValue() / 2) {
                go = false;
            }
        }

        log.info("DONE: " + Observation.class.getSimpleName() + "\n");
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
    private Date endDate;
    private boolean firstIteration = true;

    private Progress doit() {
        Progress ret = new Progress();

        long t = System.currentTimeMillis();
        long timeTransaction = -1;

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null) {
            expectedNum = batchSize.intValue();
        }
        try {
            System.gc(); // hint

            List<ObservationState> obsList;
            List<HarvestSkipURI> harvestSkipList;
            boolean operationComplete = true;
            if (destObservationDAO != null) {
                // get HarvestStateDAO record to see if this collection has actually been harvested to this destination database
                // If it has, continue. If not, say so and stop.

                PostgresqlHarvestStateDAO harvestStateDAO = new PostgresqlHarvestStateDAO(
                                                                    destObservationDAO.getDataSource(),
                                                                    target.getDatabase(), target.getSchema());
                HarvestSkipURIDAO harvestSkipURIDAO = new HarvestSkipURIDAO(
                                                                    destObservationDAO.getDataSource(),
                                                                    target.getDatabase(), target.getSchema());

                HarvestState harvestStateRec = harvestStateDAO.get(src.getIdentifier(), Observation.class.getSimpleName());
                // Check that the record returned has a last modified date.
                // harvestStateDAO.get will create an empty HarvestState record using the identifier and cname passed in.
                // in this case, getLastModified is null

                if (harvestStateRec.getLastModified() != null) {
                    try {
                        obsList = destObservationDAO.getObservationList(target.getCollection(), null, null, batchSize + 1);
                        ret.found = obsList.size();

                        harvestSkipList = harvestSkipURIDAO.get(src.getIdentifier(),Observation.class.getSimpleName(), null, null, batchSize + 1);
                        int countDeleted = 0;

                        if (obsList.size() != 0) {
                            int countToDelete = obsList.size();
                            long obsDelTime = System.currentTimeMillis();

                            log.info("Attempting to remove " + obsList.size() + " observations...");
                            try {
                                // delete each observation from the destination.
                                for (ObservationState obsState : obsList) {
                                    ObservationURI obsURI = obsState.getURI();
                                    destObservationDAO.delete(obsURI);
                                    // Needs to be in a report of some sort: where should this go?
                                    log.info("removed observation uri: " + obsURI.toString());
                                    countDeleted++;
                                }

                            } catch (Exception e) {
                                log.error("Exception deleting observations: " + e.getLocalizedMessage());
                            } finally {
                                if (countToDelete == countDeleted) {
                                    log.info("Sucessfully removed all " + countToDelete + " observations for collection");
                                } else {
                                    operationComplete = false;
                                    log.info("Operation not complete: Removed " + countDeleted + "/" + countToDelete + " observations for collection.");
                                    // Signal to Main that things didn't end well. TODO
                                }
                                long dt = System.currentTimeMillis() - obsDelTime;
                                long seconds = (dt / 1000) % 60;
                                long minutes = ((dt - seconds) / 1000) / 60;
                                log.info("time to delete observations: " + minutes + " min, " + seconds + " sec : " + obsDelTime + "ms");
                            }
                        } else {
                            log.info("No observation records found for collextion " + target.getCollection());
                        }

                        // Delete Harvest Skip records
                        int skipCount = harvestSkipList.size();
                        long harvestSkipDelTime = System.currentTimeMillis();

                        countDeleted = 0;
                        if (skipCount != 0) {
                            log.info("Attempting to remove " + skipCount + " harvest skip records...");
                            try {
                                for (HarvestSkipURI harvestSkip : harvestSkipList) {
                                    log.info("Removed " + harvestSkip.getName());
                                    harvestSkipURIDAO.delete(harvestSkip);
                                    log.info("removed harvest skip dao uri: " + harvestSkip.toString());
                                    countDeleted++;
                                }
                            } catch (Exception e) {
                                log.error("Failed removing harvest skip records. Quitting...");
                                ret.abort = true;
                            } finally {
                                if (skipCount == countDeleted) {
                                    log.info("Sucessfully removed all " + skipCount + " harvest skip records for collection");
                                } else {
                                    operationComplete = false;
                                    log.info("Operation not complete: Removed " + countDeleted + "/" + skipCount + " harvest skip records for collection.");
                                }
                                long dt = System.currentTimeMillis() - harvestSkipDelTime;

                                long seconds = (dt / 1000) % 60;
                                long minutes = ((dt - seconds) / 1000) / 60;
                                log.info("Time to delete observations: " + minutes + " min, " + seconds + " sec : " + harvestSkipDelTime + "ms");
                            }
                        } else {
                            log.info("No harvest skip records found for " + target.getCollection() + ". Continuing...");
                        }

                    } catch (Exception e) {
                        // this is for observation List & harvestSkipRecordDAO list.
                        log.error("Could not get list of Observations & Harvest skip records for this collection");
                    }

                    if (operationComplete) {
                        log.info("Deleting harvest state records");
                        // Delete the HarvestState Observation Record for this source
                        harvestStateDAO.delete(harvestStateRec);

                        harvestStateRec = harvestStateDAO.get(src.getIdentifier(), DeletedObservation.class.getSimpleName());

                        if (harvestStateRec.getLastModified() != null) {
                            // Delete the HarvestState Deleted Observation Record for this source
                            harvestStateDAO.delete(harvestStateRec);
                        }
                    }
                }
            }

        } catch (Exception e) {
            log.error("SEVERE PROBLEM - ThreadPool harvesting Observations failed: " + e.getMessage());
            ret.abort = true;
        } finally {
            timeTransaction = System.currentTimeMillis() - t;
            log.debug("time to run transactions: " + timeTransaction + "ms");
        }
        return ret;
    }

    // These functions were in a base class at one point, but given that the Removal tool is
    // pretty simple, it's not seeming necessary.
    protected Map<String, Object> getConfigDAO(HarvestResource desc) throws IOException {
        if (desc.getDatabaseServer() == null) {
            throw new RuntimeException("BUG: getConfigDAO called with ObservationResource[service]");
        }

        Map<String, Object> ret = new HashMap<String, Object>();

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(desc.getDatabaseServer(), desc.getDatabase());
        String driver = cc.getDriver();
        if (driver == null) {
            throw new RuntimeException("failed to find JDBC driver for " + desc.getDatabaseServer() + " " + desc.getDatabase());
        }

        ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
        ret.put("disableHashJoin", Boolean.TRUE);

        ret.put("server", desc.getDatabaseServer());
        ret.put("database", desc.getDatabase());
        ret.put("schema", desc.getSchema());
        return ret;
    }

}
