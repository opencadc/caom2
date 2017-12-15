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

package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.date.DateUtil;

import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class ArtifactHarvester implements PrivilegedExceptionAction<Integer> {

    public static final Integer DEFAULT_BATCH_SIZE = Integer.valueOf(1000);
    public static final String STATE_CLASS = Artifact.class.getSimpleName();

    private static final Logger log = Logger.getLogger(ArtifactHarvester.class);

    private ObservationDAO observationDAO;
    private ArtifactStore artifactStore;
    private HarvestStateDAO harvestStateDAO;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private String collection; // Will be used in the future
    private boolean full;
    private boolean dryrun;
    private int batchSize;
    private String source;
    private Date startDate;
    private Date stopDate;
    private boolean firstRun;

    public ArtifactHarvester(ObservationDAO observationDAO, String[] dbInfo,
                             ArtifactStore artifactStore, String collection, boolean dryrun, boolean full, int
                                 batchSize) {

        this.observationDAO = observationDAO;
        this.artifactStore = artifactStore;
        this.collection = collection;
        this.dryrun = dryrun;
        this.full = full;
        this.batchSize = batchSize;

        this.source = dbInfo[0] + "." + dbInfo[1] + "." + dbInfo[2];

        this.harvestStateDAO = new PostgresqlHarvestStateDAO(observationDAO.getDataSource(), dbInfo[1], dbInfo[2]);
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(observationDAO.getDataSource(), dbInfo[1], dbInfo[2], batchSize);

        this.startDate = null;
        this.stopDate = new Date();

        this.firstRun = true;
    }

    @Override
    public Integer run() throws Exception {

        long downloadCount = 0;
        int num = 0;
        int processedCount = 0;
        long start = System.currentTimeMillis();

        try {
            // Delete harvest skip URI records when in full mode
            if (full && firstRun) {
                harvestSkipURIDAO.delete(source, STATE_CLASS);
                log.debug("Cleared harvest skip URI records for full harvesting.");
            }

            // Determine the state of the last run
            HarvestState state = harvestStateDAO.get(source, STATE_CLASS);
            if (!full || !firstRun) {
                startDate = state.curLastModified;
            }
            firstRun = false;

            List<ObservationState> observationStates = observationDAO.getObservationList(collection, startDate,
                stopDate, batchSize);

            num = observationStates.size();
            log.debug("Found " + num + " observations to process.");

            for (ObservationState observationState : observationStates) {

                try {

                    observationDAO.getTransactionManager().startTransaction();

                    Observation observation = observationDAO.get(observationState.getURI());
                    for (Plane plane : observation.getPlanes()) {

                        // For now, until the work to deal with proprietary data is
                        // complete, skip non-public artifacts.

                        // Artifacts will a null dataReleaseData are assumed public
                        Date dataReleaseDate = plane.dataRelease;
                        if (dataReleaseDate != null && dataReleaseDate.after(stopDate)) {
                            log.debug("Skipping proprietary plane: " + plane);
                        } else {

                            for (Artifact artifact : plane.getArtifacts()) {
                                logStart(artifact);
                                boolean success = true;
                                boolean added = false;
                                String message = null;
                                try {
                                    processedCount++;

                                    boolean exists = artifactStore.contains(artifact.getURI(), artifact
                                        .contentChecksum);
                                    log.debug("Artifact " + artifact.getURI() + " with MD5 " + artifact
                                        .contentChecksum + " exists: " + exists);
                                    if (!exists) {

                                        // see if there's already an entry
                                        HarvestSkipURI skip = harvestSkipURIDAO.get(source, STATE_CLASS, artifact
                                            .getURI());
                                        if (skip == null) {
                                            downloadCount++;
                                            if (!dryrun) {
                                                // set the message to be an empty string
                                                skip = new HarvestSkipURI(source, STATE_CLASS, artifact.getURI(), "");
                                                harvestSkipURIDAO.put(skip);
                                                added = true;
                                            } else {
                                                added = true;
                                            }
                                        } else {
                                            message = "Artifact already exists in skip table.";
                                        }
                                    }

                                } catch (Throwable t) {
                                    success = false;
                                    message = "Failed to determine if artifact " + artifact.getURI() + " exists: " +
                                        t.getMessage();
                                    log.error(message, t);
                                    if (!dryrun) {
                                        log.debug("Adding artifact to skip table: " + artifact.getURI());
                                        // set the message to be an empty string
                                        HarvestSkipURI skip = new HarvestSkipURI(source, STATE_CLASS,
                                            artifact.getURI(), "");
                                        harvestSkipURIDAO.put(skip);
                                        added = true;
                                    }
                                } finally {
                                    state.curLastModified = artifact.getLastModified();
                                    logEnd(artifact, success, added, message);
                                }
                            }
                        }
                    }

                    if (!dryrun) {
                        harvestStateDAO.put(state);
                        log.debug("Updated artifact harvest state.  Date: " + state.curLastModified);
                    }
                } catch (Throwable t) {
                    observationDAO.getTransactionManager().rollbackTransaction();
                    throw t;
                } finally {
                    if (observationDAO.getTransactionManager().isOpen()) {
                        observationDAO.getTransactionManager().commitTransaction();
                    }
                }
            }

            return num;
        } finally {
            StringBuilder batchMessage = new StringBuilder();
            batchMessage.append("ENDBATCH: {");
            batchMessage.append("\"total\":\"").append(processedCount).append("\"");
            batchMessage.append(",");
            batchMessage.append("\"added\":\"").append(downloadCount).append("\"");
            batchMessage.append(",");
            batchMessage.append("\"time\":\"").append(System.currentTimeMillis() - start).append("\"");
            batchMessage.append(",");
            batchMessage.append("\"date\":\"").append(currentDateUTC()).append("\"");
            batchMessage.append("}");
            log.info(batchMessage.toString());
        }

    }

    private void logStart(Artifact artifact) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("START: {");
        startMessage.append("\"artifact\":\"").append(artifact.getURI()).append("\"");
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(currentDateUTC()).append("\"");
        startMessage.append("}");
        log.info(startMessage.toString());
    }

    private void logEnd(Artifact artifact, boolean success, boolean added, String message) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("END: {");
        startMessage.append("\"artifact\":\"").append(artifact.getURI()).append("\"");
        startMessage.append(",");
        startMessage.append("\"success\":\"").append(success).append("\"");
        startMessage.append(",");
        startMessage.append("\"added\":\"").append(added).append("\"");
        if (message != null) {
            startMessage.append(",");
            startMessage.append("\"message\":\"").append(message).append("\"");
        }
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(currentDateUTC()).append("\"");
        startMessage.append("}");
        log.info(startMessage.toString());
    }

    /**
     * Obtain the current UTC Date and format it.
     * TODO - This really ought to go into org.opencadc:cadc-util:ca.nrc.cadc.DateUtil.
     * TODO - 2017.12.15  jenkinsd
     *
     * @return String formatted UTC date.  Never null
     */
    private String currentDateUTC() {
        return DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil
            .UTC).format(Calendar.getInstance(DateUtil.UTC).getTime());
    }
}
