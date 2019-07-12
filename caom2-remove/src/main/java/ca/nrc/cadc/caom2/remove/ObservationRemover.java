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
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import java.io.IOException;
import java.net.URISyntaxException;
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

    protected Integer batchSize;
    protected HarvestResource src;
    protected HarvestResource target;

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
        Progress observationProgress = new Progress();

        // Get HarvestStateDAO record to see if this collection has actually been harvested to this destination database
        // If it has, continue. If not, say so and stop.
        HarvestStateDAO harvestStateDAO = new PostgresqlHarvestStateDAO(
            destObservationDAO.getDataSource(),
            target.getDatabase(), target.getSchema());
        HarvestState harvestStateRec = harvestStateDAO.get(src.getIdentifier(), Observation.class.getSimpleName());

        // Check that the record returned has a last modified date.
        // harvestStateDAO.get will create an empty HarvestState record using the identifier and cname passed in.
        // in this case, getLastModified is null
        if (harvestStateRec.getLastModified() == null) {
            log.warn("Could not find harvest state records. Quitting...");
            return;
        }

        int total = 0;
        boolean go = true;
        log.info("Using batchSize: " + batchSize);
        log.info("Removing observations...");

        while (go) {
            observationProgress = deleteObservations();

            if (observationProgress.found > 0) {
                log.info("finished batch: " + observationProgress.toString());
            }

            if (observationProgress.abort) {
                log.error("batch aborted");
            }

            total += observationProgress.found;
            go = (observationProgress.found > 0 && !observationProgress.abort);
        }

        log.info("Removed " + total + " observations");
        if (observationProgress.abort) {
            log.warn("Problem removing observations. Quitting...");
            return;
        }

        Progress skipURIProgress = new Progress();
        total = 0;
        go = true;
        log.info("Removing harvesetSkipURI records...");
        while (go) {
            skipURIProgress = deleteHarvestSkipURI();

            if (skipURIProgress.found > 0) {
                log.info("finished batch: " + skipURIProgress.toString());
            }

            if (skipURIProgress.abort) {
                log.error("batch aborted");
            }

            total += skipURIProgress.found;
            go = (skipURIProgress.found > 0 && !skipURIProgress.abort);
        }

        log.info("Removed " + total + " harvest skip URI records");
        if (skipURIProgress.abort) {
            log.warn("Problem removing skip records. Quitting...");
            return;
        }

        log.info("Deleting harvest state records");
        harvestStateDAO.delete(harvestStateRec);
        harvestStateRec = harvestStateDAO.get(src.getIdentifier(), DeletedObservation.class.getSimpleName());
        if (harvestStateRec.getLastModified() != null) {
            harvestStateDAO.delete(harvestStateRec);
        }

    }

    private static class Progress {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int removed = 0;

        @Override
        public String toString() {
            return " removed: " + removed;
        }
    }

    private Progress deleteObservations() {
        Progress ret = new Progress();

        List<ObservationState> obsList;
        if (destObservationDAO != null) {
            try {
                obsList = destObservationDAO.getObservationList(target.getCollection(), null, null, batchSize);
                ret.found = obsList.size();

                for (ObservationState obsState : obsList) {
                    ObservationURI obsURI = obsState.getURI();
                    destObservationDAO.delete(obsURI);
                    log.info("removed: observation: " + obsURI.toString());
                    ret.removed++;
                }

            } catch (Exception e) {
                log.error("failed to list && delete observations", e);
                ret.abort = true;
            }
        } else {
            log.error("destination DAO is null: Quitting....");
            ret.abort = true;
        }
        return ret;
    }

    private Progress deleteHarvestSkipURI() {
        Progress ret = new Progress();

        if (destObservationDAO != null) {
            try {
                List<HarvestSkipURI> harvestSkipList;

                HarvestSkipURIDAO harvestSkipURIDAO = new HarvestSkipURIDAO(
                    destObservationDAO.getDataSource(),
                    target.getDatabase(), target.getSchema());

                harvestSkipList = harvestSkipURIDAO.get(src.getIdentifier(),Observation.class.getSimpleName(), null, null, batchSize);
                int skipCount = harvestSkipList.size();
                ret.found = skipCount;

                for (HarvestSkipURI harvestSkip : harvestSkipList) {
                    log.info("removed: skip record: " + harvestSkip.getSkipID());
                    harvestSkipURIDAO.delete(harvestSkip);
                    ret.removed++;
                }

            } catch (Exception e) {
                log.error("failed to list && delete harvest skip records", e);
                ret.abort = true;
            }
        } else {
            log.error("destination DAO is null: Quitting....");
            ret.abort = true;
        }
        return ret;
    }


    protected Map<String, Object> getConfigDAO(HarvestResource desc) throws IOException {
        if (desc.getDatabaseServer() == null) {
            throw new RuntimeException("BUG: getConfigDAO called with ObservationResource[service]");
        }

        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
        ret.put("server", desc.getDatabaseServer());
        ret.put("database", desc.getDatabase());
        ret.put("schema", desc.getSchema());
        return ret;
    }

}
