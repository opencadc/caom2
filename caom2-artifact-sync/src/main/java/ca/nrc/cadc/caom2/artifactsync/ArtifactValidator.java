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


package ca.nrc.cadc.caom2.artifactsync;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.date.DateUtil;

/**
 * Class that compares artifacts in the caom2 metadata with the artifacts
 * in storage (via ArtifactStore).
 * 
 * @author majorb
 *
 */
public abstract class ArtifactValidator implements PrivilegedExceptionAction<Object>, ShutdownListener  {
    
    public static final String STATE_CLASS = Artifact.class.getSimpleName();
    
    protected ArtifactStore artifactStore;
    protected String collection;
    protected boolean reportOnly;
    
    private boolean summaryMode;
    
    private ExecutorService executor;
    
    private static final Logger log = Logger.getLogger(ArtifactValidator.class);
    
    abstract boolean checkAddToSkipTable(ArtifactMetadata artifact) throws URISyntaxException;
    abstract TreeSet<ArtifactMetadata> getLogicalMetadata() throws Exception;
    abstract boolean supportSkipURITable();
    
    public ArtifactValidator(String collection, boolean summaryMode, boolean reportOnly, ArtifactStore artifactStore) {
        this.collection = collection;
        this.summaryMode = summaryMode;
        this.reportOnly = reportOnly;
        this.artifactStore = artifactStore;
    }

    @Override
    public Object run() throws Exception {
        
        long start = System.currentTimeMillis();
        log.info("Starting validation for collection " + collection);
        executor = Executors.newFixedThreadPool(2);
        Future<TreeSet<ArtifactMetadata>> logicalQuery = executor.submit(new Callable<TreeSet<ArtifactMetadata>>() {
            public TreeSet<ArtifactMetadata> call() throws Exception {
                return getLogicalMetadata();
            }
        });
        log.debug("Submitted query to caom2");
        Future<TreeSet<ArtifactMetadata>> physicalQuery = executor.submit(new Callable<TreeSet<ArtifactMetadata>>() {
            public TreeSet<ArtifactMetadata> call() throws Exception {
                return getPhysicalMetadata();
            }
        });
        log.debug("Submitted query to ad");
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        log.debug("Queryies are complete");
        executor.shutdownNow();
        
        boolean supportSkipURITable = this.supportSkipURITable();
        TreeSet<ArtifactMetadata> logicalArtifacts = logicalQuery.get();
        TreeSet<ArtifactMetadata> physicalArtifacts = physicalQuery.get();
        long logicalCount = logicalArtifacts.size();
        long physicalCount = physicalArtifacts.size();
        log.debug("Found " + logicalCount + " logical artifacts.");
        log.debug("Found " + physicalCount + " physical artifacts.");
        long correct = 0;
        long diffLength = 0;
        long diffType = 0;
        long diffChecksum = 0;
        long notInLogical = 0;
        long notInPhysical = 0;
        long skipURICount = 0;
        long inSkipURICount = 0;
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, null);
        
        ArtifactMetadata nextLogical = null;
        
        for (ArtifactMetadata nextPhysical : physicalArtifacts) {
  
            if (logicalArtifacts.contains(nextPhysical)) {
                nextLogical = logicalArtifacts.ceiling(nextPhysical);
                logicalArtifacts.remove(nextLogical);
                if (nextLogical.checksum.equals(nextPhysical.checksum)) {
                    // check content length
                    if (nextLogical.contentLength == null || 
                            !nextLogical.contentLength.equals(nextPhysical.contentLength)) {
                        diffLength++;
                        if (supportSkipURITable) {
                        	if (this.checkAddToSkipTable(nextLogical)) {
                        		skipURICount++;
                        	} else {
                        		inSkipURICount++;
                        	}
                        }
                        logJSON(new String[]
                            {"logType", "detail",
                             "anomaly", "diffLength",
                             "artifactURI", nextLogical.artifactURI,
                             "storageID", nextLogical.storageID,
                             "caomContentLength", nextLogical.contentLength,
                             "storageContentLength", nextPhysical.contentLength,
                             "caomCollection", nextLogical.collection,
                             "caomLastModified", df.format(nextLogical.lastModified),
                             "ingestDate", df.format(nextPhysical.lastModified)},
                            false);
                    } else if (nextLogical.contentType == null ||
                            !nextLogical.contentType.equals(nextPhysical.contentType)) {
                        diffType++;
                        if (supportSkipURITable) {
                        	if (this.checkAddToSkipTable(nextLogical)) {
                        		skipURICount++;
                        	} else {
                        		inSkipURICount++;
                        	}
                        }
                        logJSON(new String[]
                            {"logType", "detail",
                             "anomaly", "diffType",
                             "artifactURI", nextLogical.artifactURI,
                             "storageID", nextLogical.storageID,
                             "caomContentType", nextLogical.contentType,
                             "storageContentType", nextPhysical.contentType,
                             "caomCollection", nextLogical.collection,
                             "caomLastModified", df.format(nextLogical.lastModified),
                             "ingestDate", df.format(nextPhysical.lastModified)},
                            false);
                    } else {
                        correct++;
                    }
                } else {
                    diffChecksum++;
                    if (supportSkipURITable) {
                    	if (this.checkAddToSkipTable(nextLogical)) {
                    		skipURICount++;
                    	} else {
                    		inSkipURICount++;
                    	}
                    }
                    logJSON(new String[]
                        {"logType", "detail",
                         "anomaly", "diffChecksum",
                         "artifactURI", nextLogical.artifactURI,
                         "storageID", nextLogical.storageID,
                         "caomChecksum", nextLogical.checksum,
                         "caomSize", nextLogical.contentLength,
                         "storageChecksum", nextPhysical.checksum,
                         "storageSize", nextPhysical.contentLength,
                         "caomCollection", nextLogical.collection,
                         "caomLastModified", df.format(nextLogical.lastModified),
                         "ingestDate", df.format(nextPhysical.lastModified)},
                        false);
                }
            } else {
                notInLogical++;
                logJSON(new String[]
                    {"logType", "detail",
                     "anomaly", "notInCAOM",
                     "storageID", nextPhysical.storageID,
                     "ingestDate", df.format(nextPhysical.lastModified)},
                    false);
            }
        }
        
        // at this point, any artifact that is in logicalArtifacts, is not in physicalArtifacts
        notInPhysical += logicalArtifacts.size();
        for (ArtifactMetadata next : logicalArtifacts) {
            if (!summaryMode) {
                logJSON(new String[]
                    {"logType", "detail",
                     "anomaly", "notInStorage",
                     "artifactURI", next.artifactURI,
                     "storageID", next.storageID,
                     "caomCollection", next.collection,
                     "caomLastModified", df.format(next.lastModified)},
                    false);
            }
                
            // add to HavestSkipURI table if there is not already a row in the table
            if (supportSkipURITable) {
            	if (this.checkAddToSkipTable(nextLogical)) {
            		skipURICount++;
            	} else {
            		inSkipURICount++;
            	}
            }
        }

        String skipURILabel = "totalNewSkipURI";
        if (reportOnly) {
            skipURILabel = "totalPotentialNewSkipURI";
        }
        
        String skipURIValue = "Not Applicable";
        String inSkipURIValue = "Not Applicable";
        if (supportSkipURITable) {
        	skipURIValue = Long.toString(skipURICount);
        	inSkipURIValue = Long.toString(inSkipURICount);
        }
        
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
            "totalNotInStorage", Long.toString(notInPhysical),
            "totalAlreadyInSkipURI", inSkipURIValue,
            skipURILabel, skipURIValue,
            "time", Long.toString(System.currentTimeMillis() - start)
        }, true);
        
        return null;
    }
    
    protected void logJSON(String[] data, boolean summaryInfo) {
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
        if (summaryInfo || !summaryMode) {
            log.info(sb.toString());
        }
    }

    private TreeSet<ArtifactMetadata> getPhysicalMetadata() throws Exception {
    	TreeSet<ArtifactMetadata> metadata = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
    	metadata.addAll(artifactStore.list(this.collection));
    	return metadata;
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
}

class ResultReader implements InputStreamWrapper {
    
    private static final Logger log = Logger.getLogger(ResultReader.class);
    
    TreeSet<ArtifactMetadata> artifacts;
    private boolean logical;
    private ArtifactStore artifactStore;
    
    public ResultReader(ArtifactStore artifactStore, boolean logical) 
    		throws NoSuchAlgorithmException {
        artifacts = new TreeSet<>(ArtifactMetadata.getComparator());
        this.logical = logical;
        this.artifactStore = artifactStore;
    }

    @Override
    public void read(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        String[] parts;
        ArtifactMetadata am = null;
        boolean firstLine = true;
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, null);
        while ((line = reader.readLine()) != null) {
            if (firstLine) {
                // first line is a header
                firstLine = false;
            } else {
                try {
                    parts = line.split("\t");
                    if (parts.length == 0) {
                        // empty line
                    } else {
                        am = new ArtifactMetadata();
                        if (logical) {
                            am.artifactURI = parts[0];
                            am.storageID = this.artifactStore.toStorageID(am.artifactURI);
                        } else {
                            am.storageID = parts[0];
                        }
                        
                        // read lastModified
                        am.lastModified = df.parse(parts[1]);
                        
                        if (parts.length > 2) {
                            if (logical) {
                                am.checksum = getStorageChecksum(parts[2]);
                            } else {
                                am.checksum = parts[2];
                            }
                        }
                        if (parts.length > 3) {
                            am.contentLength = parts[3];
                        }
                        if (parts.length > 4) {
                            am.contentType = parts[4];
                        }
                        if (parts.length > 5) {
                            am.releaseDate = df.parse(parts[5]);
                        }
                        artifacts.add(am);
                    }
                } catch (Exception e) {
                    log.warn("Failed to read " + (logical ? "logical" : "physical") +
                            " artifact: " + line, e);
                }
            }
        }
        log.debug("Finished reading " + (logical ? "logical" : "physical") + " artifacts.");
    }
    
    private String getStorageChecksum(String checksum) throws Exception {
        int colon = checksum.indexOf(":");
        return checksum.substring(colon + 1, checksum.length());
    }

}
