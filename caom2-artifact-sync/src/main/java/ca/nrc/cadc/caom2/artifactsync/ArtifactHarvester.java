/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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
import ca.nrc.cadc.caom2.access.AccessUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.StoragePolicy;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.date.DateUtil;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;

import javax.lang.model.type.NullType;

import org.apache.log4j.Logger;

public class ArtifactHarvester implements PrivilegedExceptionAction<NullType>, ShutdownListener {

    public static final Integer DEFAULT_BATCH_SIZE = Integer.valueOf(1000);
    public static final String STATE_CLASS = Artifact.class.getSimpleName();
    public static final String PROPRIETARY = "Proprietary";
    public static final String PUBLIC = "Public";

    private static final Logger log = Logger.getLogger(ArtifactHarvester.class);

    private ObservationDAO observationDAO;
    private ArtifactStore artifactStore;
    private HarvestStateDAO harvestStateDAO;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private String collection; 
    private StoragePolicy storagePolicy;
    private int batchSize;
    private boolean loop;
    private String source;
    private Date startDate;
    private DateFormat df;
    
    private String caomChecksum;
    private Long caomContentLength;
    private String reason = "None";

    // reset each run
    private long addCount;
    public long updateCount;
    private long totalCount;
    private Date start;

    public ArtifactHarvester(ObservationDAO observationDAO, HarvestResource harvestResource,
                             ArtifactStore artifactStore, int batchSize, boolean loop) {

        this.observationDAO = observationDAO;
        this.artifactStore = artifactStore;
        this.batchSize = batchSize;
        this.loop = loop;
        this.source = harvestResource.getIdentifier();
        this.collection = harvestResource.getCollection();
        this.storagePolicy = artifactStore.getStoragePolicy(collection);
        String database = harvestResource.getDatabase();
        String schema = harvestResource.getSchema();
        this.harvestStateDAO = new PostgresqlHarvestStateDAO(observationDAO.getDataSource(), database, schema);
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(observationDAO.getDataSource(), database, schema);

        this.startDate = null;
        
        this.df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    @Override
    public NullType run() throws Exception {
        int loopNum = 1;
        boolean stop = false;
        do {
            if (this.loop) {
                log.info("-- STARTING LOOP #" + loopNum + " --");
            }

            stop = runIt();

            if (this.loop) {
                log.info("-- ENDING LOOP #" + loopNum + " --");
            }

            loopNum++;
        } while (this.loop && !stop); // continue if work was done
        
        return null;
    }
    
    private Boolean runIt() throws Exception {

        this.addCount = 0;
        this.updateCount = 0;
        this.totalCount = 0;
        this.start = new Date();
        
        try {
            // Determine the state of the last run
            HarvestState state = harvestStateDAO.get(this.source, STATE_CLASS);
            this.startDate = state.curLastModified;
            // harvest up to a little in the past because the head of
            // the sequence may be volatile
            long fiveMinAgo = System.currentTimeMillis() - 5 * 60000L;
            Date stopDate = new Date(fiveMinAgo);
            if (this.startDate == null) {
                log.info("harvest window: null " + this.df.format(stopDate) + " [" + this.batchSize + "]");
            } else {
                log.info("harvest window: " + this.df.format(this.startDate) + " " + this.df.format(stopDate)
                             + " [" + this.batchSize + "]");
            }
            List<ObservationState> observationStates =
                this.observationDAO.getObservationList(this.collection, this.startDate, stopDate,
                                                       this.batchSize + 1);
            
            // avoid re-processing the last successful one stored in
            // HarvestState (normal case because query: >= startDate)
            if (!observationStates.isEmpty()) {
                ListIterator<ObservationState> iter = observationStates.listIterator();
                ObservationState curBatchLeader = iter.next();
                if (curBatchLeader != null) {
                    if (state.curLastModified != null) {
                        log.debug("harvestState: " + format(state.curID) + ", "
                                      + this.df.format(state.curLastModified));
                    }
                    if (curBatchLeader.getMaxLastModified().equals(state.curLastModified)) {
                        Observation observation = this.observationDAO.get(curBatchLeader.getID());
                        log.debug("current batch: " + format(observation.getID()) + ", "
                                      + this.df.format(curBatchLeader.getMaxLastModified()));
                        if (state.curID != null && state.curID.equals(observation.getID())) {
                            iter.remove();
                        }
                    }
                }
            }

            log.info("Found: " + observationStates.size());
            if (observationStates.isEmpty()) {
                return true; // stop
            }
            
            for (ObservationState observationState : observationStates) {

                try {
                    Observation observation = this.observationDAO.get(observationState.getID());
                    
                    if (observation != null) {
                        this.observationDAO.getTransactionManager().startTransaction();
                        // will make progress even on failures
                        state.curLastModified = observation.getMaxLastModified();
                        state.curID = observation.getID();
                        
                        for (Plane plane : observation.getPlanes()) {
                            for (Artifact artifact : plane.getArtifacts()) {
                                Date releaseDate = AccessUtil.getReleaseDate(artifact, plane.metaRelease,
                                                                             plane.dataRelease);
                                if (releaseDate == null) {
                                    // null date means private
                                    log.debug("null release date, skipping");
                                } else {
                                    logStart(format(state.curID), artifact);
                                    boolean success = true;
                                    boolean added = false;
                                    String message = null;
                                    this.caomChecksum = getMD5Sum(artifact.contentChecksum);
                                    if (this.caomChecksum == null) {
                                        this.caomChecksum = "null";
                                    }
                                    
                                    if (artifact.contentLength == null) {
                                        this.caomContentLength = null;
                                    } else {
                                        this.caomContentLength = artifact.contentLength;
                                    }

                                    // release date is not null, download in the future
                                    boolean isProprietary = releaseDate.after(start);
                                    String dataType = isProprietary ? ArtifactHarvester.PROPRIETARY : ArtifactHarvester.PUBLIC;
                                    this.reason = "None";
                                    this.totalCount++;

                                    try {
                                        HarvestSkipURI skip = harvestSkipURIDAO.get(source, STATE_CLASS,
                                                                                    artifact.getURI());
                                        message = dataType;
                                        if (skip == null) {
                                            // not in skip table, add it
                                            skip = new HarvestSkipURI(source, STATE_CLASS, artifact.getURI(),
                                                                      releaseDate, dataType);
                                            added = true;
                                            this.addCount++;
                                            message += " artifact added to skip table";
                                        } else {
                                            this.updateCount++;
                                            message += " artifact already exists in skip table";
                                            this.reason = "artifact already exists in skip table";
                                        }
                                        skip.setTryAfter(releaseDate);
                                        message += ", update tryAfter date to release date.";
                                        this.harvestSkipURIDAO.put(skip);
                                    } catch (Exception ex) {
                                        success = false;
                                        message = "Failed to determine if artifact " + artifact.getURI() + " exists: "
                                            + ex.getMessage();
                                        this.reason = "Failed to determine if artifact exists";
                                        log.error(message, ex);
                                    }
                                    logEnd(observation, artifact, success, added, message);
                                }
                            }
                        }
                        this.harvestStateDAO.put(state);
                        log.debug("Updated artifact harvest state.  Date: " + state.curLastModified);
                        log.debug("Updated artifact harvest state.  ID: " + format(state.curID));

                        this.observationDAO.getTransactionManager().commitTransaction();
                    } else {
                        log.debug("Observation no longer exists: " + observationState.getURI());
                    }
                    
                } catch (Throwable t) {
                    this.observationDAO.getTransactionManager().rollbackTransaction();
                    throw t;
                }
            }

            // batch completed: try another
            return false;
        } finally {
            logBatchEnd();
        }

    }
    
    private String getMD5Sum(URI checksum) throws UnsupportedOperationException {
        if (checksum == null) {
            return null;
        }

        if (checksum.getScheme().equalsIgnoreCase("MD5")) {
            return checksum.getSchemeSpecificPart();
        } else {
            throw new UnsupportedOperationException("Checksum algorithm " + checksum.getScheme() + " not supported.");
        }
    }

    private String format(UUID id) {
        if (id == null) {
            return "null";
        }
        return id.toString();
    }
    
    private String safeToString(Long n) {
        if (n == null) {
            return "null";
        }
        return n.toString();
    }

    private void logStart(String observationID, Artifact artifact) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("START: {");
        startMessage.append("\"observationID\":\"").append(observationID).append("\"");
        startMessage.append(",");
        startMessage.append("\"artifact\":\"").append(artifact.getURI()).append("\"");
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(this.df.format(new Date())).append("\"");
        startMessage.append("}");
        log.info(startMessage.toString());
    }

    private void logEnd(Observation o, Artifact artifact, boolean success, boolean added, String message) {
        final String caomContentLengthStr = safeToString(this.caomContentLength);
        StringBuilder endMessage = new StringBuilder();
        endMessage.append("END: {");
        endMessage.append("\"observationID\":\"").append(o.getID()).append("\"");
        endMessage.append(",");
        endMessage.append("\"artifact\":\"").append(artifact.getURI()).append("\"");
        endMessage.append(",");
        endMessage.append("\"success\":\"").append(success).append("\"");
        endMessage.append(",");
        if (message != null && message.contains("update tryAfter date")) {
            endMessage.append("\"updated\":\"").append(added).append("\"");
        } else {
            endMessage.append("\"added\":\"").append(added).append("\"");
        }
        endMessage.append(",");
        endMessage.append("\"reason\":\"").append(this.reason).append("\"");
        endMessage.append(",");
        endMessage.append("\"caomChecksum\":\"").append(this.caomChecksum).append("\"");
        endMessage.append(",");
        endMessage.append("\"caomContentLength\":\"").append(caomContentLengthStr).append("\"");
        endMessage.append(",");
        endMessage.append("\"collection\":\"").append(this.collection).append("\"");
        if (message != null) {
            endMessage.append(",");
            endMessage.append("\"message\":\"").append(message).append("\"");
        }
        endMessage.append(",");
        endMessage.append("\"lastModified\":\"").append(df.format(o.getMaxLastModified())).append("\"");
        endMessage.append("\"date\":\"").append(df.format(new Date())).append("\"");
        endMessage.append("}");
        log.info(endMessage.toString());
    }
    
    private void logBatchEnd() {
        logBatchEnd("ENDBATCH");
    }

    private void logBatchEnd(String endString) {
        StringBuilder batchMessage = new StringBuilder();
        batchMessage.append(endString + ": {");
        batchMessage.append("\"total\":\"").append(this.totalCount).append("\"");
        batchMessage.append(",");
        batchMessage.append("\"added\":\"").append(this.addCount).append("\"");
        batchMessage.append(",");
        batchMessage.append("\"updated\":\"").append(this.updateCount).append("\"");
        batchMessage.append(",");
        batchMessage.append("\"time\":\"").append(System.currentTimeMillis() - this.start.getTime()).append("\"");
        batchMessage.append(",");
        batchMessage.append("\"date\":\"").append(this.df.format(this.start)).append("\"");
        batchMessage.append("}");
        log.info(batchMessage.toString());
    }

    @Override
    public void shutdown() {
        logBatchEnd("ENDDISCOVER");
    }
    
}
