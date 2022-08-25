/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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

package org.opencadc.caom2.download;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.WriteException;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RangeNotSatisfiableException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 * Single file sync instance.
 *
 * @author pdowler
 */
public class FileSyncJob implements Runnable  {
    private static final Logger log = Logger.getLogger(FileSyncJob.class);

    private static final long[] RETRY_DELAY = new long[] { 6000L, 12000L };
    public static final int DEFAULT_CONNECTION_TIMEOUT = 6000;
    public static final int DEFAULT_READ_TIMEOUT = 60000;

    private final HarvestSkipURI harvestSkipURI;
    private final HarvestSkipURIDAO harvestSkipURIDAO;
    private final ArtifactDAO artifactDAO;
    private final ArtifactStore artifactStore;
    private final int retryAfter;
    private final Subject subject;
    private final List<Exception> fails = new ArrayList<>();

    /**
     * Construct a job to sync the specified artifact.
     *
     * @param harvestSkipURI artifact to sync
     * @param harvestSkipURIDAO harvest skip database persistence
     * @param artifactDAO artifact database persistence
     * @param artifactStore back end storage
     * @param retryAfter date after which to retry failed downloads
     * @param subject caller with credentials for downloads
     */
    public FileSyncJob(HarvestSkipURI harvestSkipURI, HarvestSkipURIDAO harvestSkipURIDAO,
                       ArtifactDAO artifactDAO, ArtifactStore artifactStore,
                       int retryAfter, Subject subject) {
        CaomValidator.assertNotNull(FileSyncJob.class, "harvestSkipURI", harvestSkipURI);
        CaomValidator.assertNotNull(FileSyncJob.class, "harvestSkipURIDAO", harvestSkipURIDAO);
        CaomValidator.assertNotNull(FileSyncJob.class, "artifactDAO", artifactDAO);
        CaomValidator.assertNotNull(FileSyncJob.class, "artifactStore", artifactStore);
        CaomValidator.assertNotNull(FileSyncJob.class, "subject", subject);

        this.harvestSkipURI = harvestSkipURI;
        this.harvestSkipURIDAO = harvestSkipURIDAO;
        this.artifactDAO = artifactDAO;
        this.artifactStore = artifactStore;
        this.retryAfter = retryAfter;
        this.subject = subject;
    }

    @Override
    public void run() {
        Subject currentSubject = new Subject();

        // Also synchronized in FileSync.run()
        synchronized (subject) {
            currentSubject.getPrincipals().addAll(subject.getPrincipals());
            currentSubject.getPublicCredentials().addAll(subject.getPublicCredentials());
        }
        Subject.doAs(currentSubject, new RunnableAction(this::doSync));
    }

    private void doSync() {

        log.info("FileSyncJob.START " + harvestSkipURI.getSkipID());
        long start = System.currentTimeMillis();
        int downloadAttempts = 0;
        long byteTransferTime = 0;
        boolean success = false;
        String msg = "";

        try {
            URI artifactURI = harvestSkipURI.getSkipID();
            Artifact curArtifact = artifactDAO.get(artifactURI);
            if (curArtifact == null) {
                // artifact no longer exists, remove from skip uri table
                success = true;
                msg = "reason=obsolete-artifact";
                return;
            }

            CaomArtifactResolver caomArtifactResolver = new CaomArtifactResolver();
            URL url;
            try {
                url = caomArtifactResolver.getURL(artifactURI);
                log.debug("download url: " + url);
            } catch (MalformedURLException | IllegalStateException ex) {
                log.debug("FileSyncJob.ERROR", ex);
                msg = "CaomArtifactResolver failed: " + ex;
                return;
            }
            if (url == null) {
                log.debug("FileSyncJob.ERROR CaomArtifactResolver unable to resolve " + artifactURI);
                msg = "CaomArtifactResolver failed to resolve artifact uri";
                return;
            }

            int retryCount = 0;
            try {
                while (!success && retryCount < RETRY_DELAY.length) {
                    log.debug(String.format("FileSyncJob.SYNC %s attempts=%s", artifactURI, retryCount));

                    curArtifact = artifactDAO.get(artifactURI);
                    if (curArtifact == null) {
                        msg = "reason=obsolete-artifact";
                        success = true;
                        return;
                    }
                    
                    FileMetadata fileMetadata = new FileMetadata();
                    fileMetadata.setContentType(curArtifact.contentType);
                    fileMetadata.setContentLength(curArtifact.contentLength);
                    if (curArtifact.contentChecksum != null) {
                        fileMetadata.setMd5Sum(curArtifact.contentChecksum.getSchemeSpecificPart());
                    }

                    boolean postPrepare = false;
                    try {
                        downloadAttempts++;

                        // get the artifact metadata
                        OutputStream out = new ByteArrayOutputStream();
                        HttpGet head = new HttpGet(url, out);
                        head.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
                        head.setReadTimeout(DEFAULT_READ_TIMEOUT);
                        head.setHeadOnly(true);
                        head.prepare();

                        // compare artifact metadata
                        URI hdrContentChecksum = head.getDigest();
                        if (curArtifact.contentChecksum != null && hdrContentChecksum != null) {
                            if (!hdrContentChecksum.equals(curArtifact.contentChecksum)) {
                                throw new PreconditionFailedException(
                                    String.format("contentChecksum artifact: %s storage: %s", curArtifact.contentChecksum,
                                                  hdrContentChecksum));
                            }
                        }

                        long hdrContentLength = head.getContentLength();
                        if (hdrContentLength != -1 && hdrContentLength != fileMetadata.getContentLength()) {
                            throw new PreconditionFailedException(
                                String.format("contentLength artifact: %s storage: %s", fileMetadata.getContentLength(),
                                              hdrContentLength));
                        }

                        // check again to be sure the destination doesn't already have it
                        ArtifactMetadata tempMetadata = artifactStore.get(artifactURI);
                        if (tempMetadata != null && tempMetadata.getChecksum() != null
                            && tempMetadata.getChecksum().equals(curArtifact.contentChecksum.getSchemeSpecificPart())) {
                            msg = "ArtifactStore already has correct copy";
                            success = true;
                            return;
                        }

                        HttpGet download = new HttpGet(url, true);
                        download.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
                        download.setReadTimeout(DEFAULT_READ_TIMEOUT);
                        log.debug(String.format("download: %s as %s", url, AuthenticationUtil.getCurrentSubject()));

                        final long dlStart = System.currentTimeMillis();
                        download.prepare();
                        postPrepare = true;

                        artifactStore.store(harvestSkipURI.getSkipID(), download.getInputStream(), fileMetadata);
                        byteTransferTime = System.currentTimeMillis() - dlStart;
                        success = true;
                        log.debug(String.format("Completed download of %s from %s", artifactURI, url));

                    } catch (ByteLimitExceededException | WriteException ex) {
                        // IOException will capture this if not explicitly caught and rethrown
                        log.debug("FileSyncJob.FAIL", ex);
                        log.error(String.format("FileSyncJob.FAIL %s reason=%s", artifactURI, ex));
                        throw ex;
                    } catch (MalformedURLException | ResourceNotFoundException | ResourceAlreadyExistsException
                             | PreconditionFailedException | RangeNotSatisfiableException
                             | AccessControlException | NotAuthenticatedException ex) {
                        log.debug("FileSyncJob.ERROR", ex);
                        log.warn(String.format("FileSyncJob.ERROR %s reason=%s", artifactURI, ex));
                        fails.add(ex);
                        return; // fatal
                    } catch (IOException | TransientException ex) {
                        // includes ReadException
                        // - prepare or put throwing this error
                        log.debug("FileSyncJob.ERROR", ex);
                        log.warn(String.format("FileSyncJob.ERROR %s reason=%s", artifactURI, ex));
                        fails.add(ex);
                    } catch (Exception ex) {
                        if (!postPrepare) {
                            // remote server 5xx response: discard
                            log.debug("FileSyncJob.ERROR", ex);
                            log.warn(String.format("FileSyncJob.ERROR %s reason=%s", artifactURI, ex));
                            fails.add(ex);
                        } else {
                            // ArtifactStore.store internal fail: abort
                            log.debug("FileSyncJob.FAIL", ex);
                            log.warn(String.format("FileSyncJob.FAIL %s reason=%s", artifactURI, ex));
                            throw ex;
                        }
                    }

                    if (!success) {
                        log.info("FileSyncJob.SLEEP dt=" + RETRY_DELAY[retryCount]);
                        Thread.sleep(RETRY_DELAY[retryCount++]);
                    }
                }
                if (!success) {
                    Exception commonFail = null;
                    for (Exception e : fails) {
                        if (commonFail == null) {
                            commonFail = e;
                        }
                        if (!commonFail.getClass().equals(e.getClass())) {
                            commonFail = null;
                            break;
                        }
                    }
                    if (commonFail != null) {
                        msg = "reason=" + commonFail;
                    }
                }
            }  catch (ByteLimitExceededException | IllegalStateException ex) {
                log.debug("artifact download aborted: " + harvestSkipURI.getSkipID(), ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            } catch (IllegalArgumentException | InterruptedException | WriteException ex) {
                log.debug("artifact download error: " + harvestSkipURI.getSkipID(), ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            } catch (Exception ex) {
                log.debug("unexpected fail: " + harvestSkipURI.getSkipID(), ex);
                msg = "reason=" + ex.getClass().getName() + " " + ex.getMessage();
            }
        } finally {
            // Update the skip table
            try {
                synchronized (harvestSkipURIDAO) {
                    if (success) {
                        harvestSkipURIDAO.delete(harvestSkipURI);
                    } else {
                        harvestSkipURI.errorMessage = msg;
                        Calendar c = Calendar.getInstance();
                        c.add(Calendar.HOUR, retryAfter);
                        harvestSkipURI.setTryAfter(c.getTime());
                        harvestSkipURIDAO.put(harvestSkipURI);
                    }
                }
            } catch (Throwable t) {
                log.error("Failed to update or delete from skip table", t);
            }
            // log final results
            long dt = System.currentTimeMillis() - start;
            long overheadTime = dt - byteTransferTime;
            StringBuilder sb = new StringBuilder();
            sb.append("FileSyncJob.END ").append(harvestSkipURI.getSkipID());
            sb.append(" success=").append(success);
            sb.append(" duration=").append(dt);
            sb.append(" attempts=").append(downloadAttempts);
            if (byteTransferTime > 0) {
                sb.append(" transfer=").append(byteTransferTime);
                sb.append(" overhead=").append(overheadTime);
            }
            sb.append(" ").append(msg);
            log.info(sb.toString());
        }
    }

}
