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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.access.AccessUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.StoragePolicy;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.StringUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.opencadc.tap.TapClient;

/**
 * Class that compares artifacts in the caom2 metadata with the artifacts
 * in storage (via ArtifactStore).
 * 
 * @author majorb
 *
 */
public class ArtifactValidator implements PrivilegedExceptionAction<Object>, ShutdownListener  {
    
    public static final String STATE_CLASS = Artifact.class.getSimpleName();
    
    private ObservationDAO observationDAO;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private String source;
    private ArtifactStore artifactStore;
    private String collection;
    private boolean reportOnly;
    private URI caomTapResourceID;
    private URL caomTapURL;
    private boolean supportSkipURITable = false;
    private boolean tolerateNullChecksum = false;
    private boolean tolerateNullContentLength = false;
        
    private ExecutorService executor;
    
    private static final Logger log = Logger.getLogger(ArtifactValidator.class);

    public ArtifactValidator(DataSource dataSource, HarvestResource harvestResource, ObservationDAO observationDAO, 
            boolean reportOnly, ArtifactStore artifactStore, boolean tolerateNullChecksum, boolean tolerateNullContentLength) {
        this(harvestResource.getCollection(), reportOnly, artifactStore, tolerateNullChecksum, tolerateNullContentLength);
        this.observationDAO = observationDAO;
        this.source = harvestResource.getIdentifier();
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(dataSource, harvestResource.getDatabase(), harvestResource.getSchema());
    }
    
    public ArtifactValidator(URI caomTapResourceID, String collection, 
            boolean reportOnly, ArtifactStore artifactStore, boolean tolerateNullChecksum, boolean tolerateNullContentLength) {
        this(collection, reportOnly, artifactStore, tolerateNullChecksum, tolerateNullContentLength);
        this.caomTapResourceID = caomTapResourceID;
    }
    
    public ArtifactValidator(URL caomTapURL, String collection, 
            boolean reportOnly, ArtifactStore artifactStore, boolean tolerateNullChecksum, boolean tolerateNullContentLength) {
        this(collection, reportOnly, artifactStore, tolerateNullChecksum, tolerateNullContentLength);
        this.caomTapURL = caomTapURL;
    }
    
    private ArtifactValidator(String collection, boolean reportOnly, 
            ArtifactStore artifactStore, boolean tolerateNullChecksum, boolean tolerateNullContentLength) {
        this.collection = collection;
        this.reportOnly = reportOnly;
        this.artifactStore = artifactStore;
        this.tolerateNullChecksum = tolerateNullChecksum;
        this.tolerateNullContentLength = tolerateNullContentLength;
    }

    @Override
    public Object run() throws Exception {
        
        final long start = System.currentTimeMillis();
        log.info("Starting validation for collection " + collection);
        executor = Executors.newFixedThreadPool(2);
        final Future<TreeSet<ArtifactMetadata>> logicalQuery = executor.submit(new Callable<TreeSet<ArtifactMetadata>>() {
            public TreeSet<ArtifactMetadata> call() throws Exception {
                return getLogicalMetadata();
            }
        });
        log.info("Submitted query to caom2");
        final Future<TreeSet<ArtifactMetadata>> physicalQuery = executor.submit(new Callable<TreeSet<ArtifactMetadata>>() {
            public TreeSet<ArtifactMetadata> call() throws Exception {
                return getPhysicalMetadata();
            }
        });
        log.info("Submitted query to storage");
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        log.info("Queries are complete");
        executor.shutdownNow();
        
        TreeSet<ArtifactMetadata> logicalMetadata = logicalQuery.get();
        log.info("number of artifacts in CAOM2: " + logicalMetadata.size());
        TreeSet<ArtifactMetadata> physicalMetadata = physicalQuery.get();
        log.info("number of artifacts in storage: " + physicalMetadata.size());
        if (logicalMetadata.isEmpty() || physicalMetadata.isEmpty()) {
            log.error("Number of artifacts in CAOM2 or in storage cannot be zero.");
        } else {
            compareMetadata(logicalMetadata, physicalMetadata, start);
        }
        return null;
    }
    
    void compareMetadata(TreeSet<ArtifactMetadata> logicalMetadata, TreeSet<ArtifactMetadata> physicalMetadata,
            long start) throws Exception {
        boolean supportSkipURITable = supportSkipURITable();
        long logicalCount = logicalMetadata.size();
        long physicalCount = physicalMetadata.size();
        long correct = 0;
        long diffLength = 0;
        long diffType = 0;
        long diffChecksum = 0;
        long notInLogical = 0;
        long skipURICount = 0;
        long inSkipURICount = 0;
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        ArtifactMetadata nextLogical = null;
        for (ArtifactMetadata nextPhysical : physicalMetadata) {
            
            if (logicalMetadata.contains(nextPhysical)) {
                nextLogical = logicalMetadata.ceiling(nextPhysical);
                logicalMetadata.remove(nextLogical);
                if (matches(nextLogical.getChecksum(), nextPhysical.getChecksum())) {
                    if (matches(nextLogical.contentLength, nextPhysical.contentLength)) {
                        if (matches(nextLogical.contentType, nextPhysical.contentType)) {
                            correct++;
                        } else {
                            // content type mismatch
                            diffType++;
                            logJSON(new String[]
                                {"logType", "detail",
                                 "anomaly", "diffType",
                                 "observationID", nextLogical.observationID,
                                 "artifactURI", nextLogical.getArtifactURI().toString(),
                                 "caomContentType", nextLogical.contentType,
                                 "storageContentType", nextPhysical.contentType,
                                 "caomCollection", collection},
                                false);
                        }
                    } else {
                        // content length mismatch
                        diffLength++;
                        if (supportSkipURITable) {
                            if (checkAddToSkipTable(nextLogical, "ContentLengths are different")) {
                                skipURICount++;
                            } else {
                                inSkipURICount++;
                            }
                        }
                        logJSON(new String[]
                            {"logType", "detail",
                             "anomaly", "diffLength",
                             "observationID", nextLogical.observationID,
                             "artifactURI", nextLogical.getArtifactURI().toASCIIString(),
                             "caomContentLength", safeToString(nextLogical.contentLength),
                             "storageContentLength", safeToString(nextPhysical.contentLength),
                             "caomCollection", collection},
                            false);
                    }
                } else {
                    // checksum mismatch
                    diffChecksum++;
                    if (supportSkipURITable) {
                        if (checkAddToSkipTable(nextLogical, "Checksums are different")) {
                            skipURICount++;
                        } else {
                            inSkipURICount++;
                        }
                    }
                    logJSON(new String[]
                        {"logType", "detail",
                         "anomaly", "diffChecksum",
                         "observationID", nextLogical.observationID,
                         "artifactURI", nextLogical.getArtifactURI().toString(),
                         "caomChecksum", nextLogical.getChecksum(),
                         "caomSize", safeToString(nextLogical.contentLength),
                         "storageChecksum", nextPhysical.getChecksum(),
                         "storageSize", safeToString(nextPhysical.contentLength),
                         "caomCollection", collection},
                        false);
                }
            } else {
                notInLogical++;
                logJSON(new String[]
                    {"logType", "detail",
                     "anomaly", "notInCAOM",
                     "artifactURI", nextPhysical.getArtifactURI().toString()},
                    false);
            }
        }
        
        // at this point, any artifact that is in logicalArtifacts, is not in physicalArtifacts
        long missingFromStorage = 0;
        long notPublic = 0;
        StoragePolicy storagePolicy = artifactStore.getStoragePolicy(this.collection);
        Date now = new Date();
        for (ArtifactMetadata metadata : logicalMetadata) {
            String errorMessage = null; 
            
            Artifact artifact = new Artifact(metadata.getArtifactURI(), metadata.productType, metadata.releaseType);
            Date releaseDate = AccessUtil.getReleaseDate(artifact, metadata.metaRelease, metadata.dataRelease);
            String releaseDateString = "null";
            boolean miss = false;
            if (releaseDate == null) {
                // proprietary artifact, skip
                log.debug("null release date, skipping");
                if (StoragePolicy.PUBLIC_ONLY == storagePolicy) {
                    notPublic++;
                } else {
                    // missing proprietary artifact, but won't be added to skip table
                    miss = true;
                }
            } else {
                releaseDateString = df.format(releaseDate);
                if (releaseDate.after(now)) {
                    // proprietary artifact, add to skip table for future download
                    errorMessage = ArtifactHarvester.PROPRIETARY;
                    if (StoragePolicy.PUBLIC_ONLY == storagePolicy) {
                        notPublic++;
                    } else {
                        // missing proprietary artifact, add to skip table
                        miss = true;
                    }
                } else {
                    // missing public artifact, add to skip table
                    miss = true;
                }
                
                // add to HavestSkipURI table if there is not already a row in the table
                if (supportSkipURITable) {
                    if (checkAddToSkipTable(metadata, errorMessage)) {
                        skipURICount++;
                    } else {
                        inSkipURICount++;
                    }
                }
            }
            
            if (miss) {
                missingFromStorage++;
                logJSON(new String[]
                    {"logType", "detail",
                     "anomaly", "missingFromStorage",
                     "releaseDate", releaseDateString,
                     "observationID", metadata.observationID,
                     "artifactURI", metadata.getArtifactURI().toASCIIString(),
                     "caomCollection", collection},
                    false);
            }
        }
        
        if (reportOnly) {
            // diff
            logJSON(new String[] {
                "logType", "summary",
                "collection", collection,
                "totalInCAOM", Long.toString(logicalCount),
                "totalInStorage", Long.toString(physicalCount),
                "totalCorrect", Long.toString(correct),
                "totalDiffChecksum", Long.toString(diffChecksum),
                "totalDiffLength", Long.toString(diffLength),
                "totalDiffType", Long.toString(diffType),
                "totalNotInCAOM", Long.toString(notInLogical),
                "totalMissingFromStorage", Long.toString(missingFromStorage),
                "totalNotPublic", Long.toString(notPublic),
                "time", Long.toString(System.currentTimeMillis() - start)
                }, true);
        } else {
            // validate
            logJSON(new String[] {
                "logType", "summary",
                "collection", collection,
                "totalInCAOM", Long.toString(logicalCount),
                "totalInStorage", Long.toString(physicalCount),
                "totalCorrect", Long.toString(correct),
                "totalDiffChecksum", Long.toString(diffChecksum),
                "totalDiffLength", Long.toString(diffLength),
                "totalDiffType", Long.toString(diffType),
                "totalNotInCAOM", Long.toString(notInLogical),
                "totalMissingFromStorage", Long.toString(missingFromStorage),
                "totalNotPublic", Long.toString(notPublic),
                "totalAlreadyInSkipURI", Long.toString(inSkipURICount),
                "totalNewSkipURI", Long.toString(skipURICount),
                "time", Long.toString(System.currentTimeMillis() - start)
                }, true);
        }
    }
    
    public void shutdown() {
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Shutdown interruped");
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private String safeToString(Long n) {
        if (n == null) {
            return "null";
        }
        return n.toString();
    }
    
    private void logJSON(String[] data, boolean summaryInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean paired = true;
        for (String s : data) {
            sb.append("\"");
            sb.append(s);
            sb.append("\"");
            if (paired) {
                sb.append(":");
            } else {
                sb.append(",");
            }
            paired = !paired;
        }
        sb.setLength(sb.length() - 1);
        sb.append("}");
        if (summaryInfo || reportOnly) {
            System.out.println(sb.toString());
        }
    }
    
    private boolean matches(String logical, String physical) {
        // consider it a match if logical is null or empty or there is an actual match
        if (logical == null || logical.length() == 0
            || logical.equals(physical)) {
            return true;
        } else {
            return false;
        }
    }
    
    private boolean matches(Long logical, Long physical) {
        // consider it a match if logical is null or empty or there is an actual match
        if (logical == null || logical.equals(physical)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean supportSkipURITable() {
        return supportSkipURITable;
    }
    
    private boolean checkAddToSkipTable(ArtifactMetadata metadata, String errorMessage) throws URISyntaxException {
        if (supportSkipURITable) {
            // add to HavestSkipURI table if there is not already a row in the table
            Artifact artifact = new Artifact(metadata.getArtifactURI(), metadata.productType, metadata.releaseType);
            Date releaseDate = AccessUtil.getReleaseDate(artifact, metadata.metaRelease, metadata.dataRelease);
            HarvestSkipURI skip = harvestSkipURIDAO.get(source, STATE_CLASS, metadata.getArtifactURI());
            if (releaseDate != null && !reportOnly) {
                if (Util.addToSkipTable(source, STATE_CLASS, metadata.getArtifactURI(), errorMessage, skip, releaseDate)) {
                    harvestSkipURIDAO.put(skip);
                    String errorMessageString = (errorMessage == null) ? "null" : skip.errorMessage;
                    logJSON(new String[]
                        {"logType", "detail",
                         "action", "addedToSkipTable",
                         "artifactURI", metadata.getArtifactURI().toASCIIString(),
                         "caomCollection", collection,
                         "caomChecksum", metadata.getChecksum(),
                         "errorMessage", errorMessageString},
                        true);
                    return true;
                }
            }
        }

        return false;
    }
    
    private TreeSet<ArtifactMetadata> getLogicalMetadata() throws Exception {
        TreeSet<ArtifactMetadata> result = new TreeSet<>(ArtifactMetadata.getComparator());
        if (StringUtil.hasText(source)) {
            // use database <server.database.schema>
            // HarvestSkipURI table is not supported in 'diff' mode, i.e. reportOnly = true
            this.supportSkipURITable = !reportOnly;
            long t1 = System.currentTimeMillis();
            List<ObservationState> states = observationDAO.getObservationList(collection, null, null, null);
            long t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            log.info("get-state-list: size=" + states.size() + " in " + dt + " ms");
            
            int depth = 3;
            ListIterator<ObservationState> iter = states.listIterator();
            t1 = System.currentTimeMillis();
            while (iter.hasNext()) {
                ObservationState s = iter.next();
                iter.remove(); // GC
                ObservationResponse resp = observationDAO.getObservationResponse(s, depth);
                if (resp == null) {
                    log.error("Null response from Observation DAO, ObservationURI: " + s.getURI().toString() + ", depth: " + depth);
                } else if (resp.observation == null) {
                    log.error("Observation is null, ObservationURI: " + s.getURI().toString() + ", depth: " + depth);
                } else {
                    for (Plane plane : resp.observation.getPlanes()) {
                        for (Artifact artifact : plane.getArtifacts()) {
                            String observationID = s.getURI().getObservationID();
                            result.add(getMetadata(observationID, artifact, plane.dataRelease, plane.metaRelease));
                        }
                    }
                }
            }
            
            log.info("Finished logical metadata query in " + (System.currentTimeMillis() - t1) + " ms");
        } else {
            this.supportSkipURITable = false;
            if (caomTapResourceID != null) {
                // source is a TAP resource ID
                AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(AuthenticationUtil.getCurrentSubject());
                TapClient tapClient = new TapClient(caomTapResourceID);
                try {
                    this.caomTapURL = tapClient.getSyncURL(authMethod);
                } catch (ResourceNotFoundException ex) {
                    if (ex.getMessage().contains("with password")) {
                        throw new ResourceNotFoundException("TAP service for "
                            + caomTapResourceID + " does not support password authentication.", ex);
                    }
                }
            }
            
            // source is a TAP service URL or a TAP resource ID
            String adql = "select distinct(a.uri), a.contentChecksum, a.contentLength, a.contentType, o.observationID, "
                    + "a.productType, a.releaseType, p.dataRelease, p.metaRelease "
                    + "from caom2.Artifact a "
                    + "join caom2.Plane p on a.planeID = p.planeID "
                    + "join caom2.Observation o on p.obsID = o.obsID "
                    + "where o.collection='" + collection + "'";

            log.debug("logical query: " + adql);
            long start = System.currentTimeMillis();
            result = query(caomTapURL, adql);
            log.info("Finished logical metadata query in " + (System.currentTimeMillis() - start) + " ms");
        }
        return result;
    }
    
    private ArtifactMetadata getMetadata(String observationID, Artifact artifact, Date dataRelease, Date metaRelease) throws Exception {
        String cs = null;
        if (artifact.contentChecksum == null) {
            if (!this.tolerateNullChecksum) {
                throw new RuntimeException("content checksum is null for artifact URI: " + artifact.getURI());
            }
        } else {
            cs = getStorageChecksum(artifact.contentChecksum.toASCIIString());
        }
        ArtifactMetadata metadata = new ArtifactMetadata(artifact.getURI(), cs); 
        
        
        if (artifact.contentLength == null) {
            if (!this.tolerateNullContentLength) {
                throw new RuntimeException("content length is null for artifact URI: " + metadata.getArtifactURI());
            }
        } else {
            metadata.contentLength = artifact.contentLength;
        }
        metadata.contentType = artifact.contentType;
        
        metadata.observationID = observationID;
        metadata.productType = artifact.getProductType();
        metadata.releaseType = artifact.getReleaseType();
        metadata.metaRelease = metaRelease;
        metadata.dataRelease = dataRelease;
        
        return metadata;
    }
    
    private TreeSet<ArtifactMetadata> query(URL baseURL, String adql) throws Exception {
        StringBuilder queryString = new StringBuilder();
        queryString.append("LANG=ADQL&RESPONSEFORMAT=tsv&QUERY=");
        queryString.append(URLEncoder.encode(adql, "UTF-8"));
        URL url = new URL(baseURL.toString() + "?" + queryString.toString());
        ResultReader resultReader = new ResultReader(artifactStore);
        HttpDownload get = new HttpDownload(url, resultReader);
        try {
            get.run();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
        if (get.getThrowable() != null) {
            if (get.getThrowable() instanceof Exception) {
                throw (Exception) get.getThrowable();
            } else {
                throw new RuntimeException(get.getThrowable());
            }
        }
        
        return resultReader.metadata;
    }
    
    private String getStorageChecksum(String checksum) throws Exception {
        int colon = checksum.indexOf(":");
        return checksum.substring(colon + 1, checksum.length());
    }

    private TreeSet<ArtifactMetadata> getPhysicalMetadata() throws Exception {
        TreeSet<ArtifactMetadata> metadata = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        long t1 = System.currentTimeMillis();
        metadata.addAll(artifactStore.list(collection));
        log.info("Finished physical metadata query in " + (System.currentTimeMillis() - t1) + " ms");
        return metadata;
    }
}
