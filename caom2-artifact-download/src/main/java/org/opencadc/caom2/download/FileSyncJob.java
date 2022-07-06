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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.util.FileMetadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 * Single file sync instance.
 *
 * @author pdowler
 */
public class FileSyncJob implements Runnable  {
    private static final Logger log = Logger.getLogger(FileSyncJob.class);

    public static final int DEFAULT_TIMEOUT = 600000;  // 10 minutes

    private final HarvestSkipURI harvestSkipURI;
    private final HarvestSkipURIDAO harvestSkipURIDAO;
    private final ArtifactDAO artifactDAO;
    private final ArtifactStore artifactStore;
    private final boolean tolerateNullChecksum;
    private final Date retryAfter;
    private final Subject subject;
    private final DateFormat dateFormat;

    /**
     * Construct a job to sync the specified artifact.
     *
     * @param harvestSkipURI artifact to sync
     * @param harvestSkipURIDAO harvest skip database persistence
     * @param artifactDAO artifact database persistence
     * @param artifactStore back end storage
     * @param tolerateNullChecksum download even when checksum is null
     * @param retryAfter date after which to retry failed downloads
     * @param subject caller with credentials for downloads
     */
    public FileSyncJob(HarvestSkipURI harvestSkipURI, HarvestSkipURIDAO harvestSkipURIDAO,
                       ArtifactDAO artifactDAO, ArtifactStore artifactStore,
                       boolean tolerateNullChecksum, Date retryAfter, Subject subject) {
        CaomValidator.assertNotNull(FileSyncJob.class, "harvestSkipURI", harvestSkipURI);
        CaomValidator.assertNotNull(FileSyncJob.class, "harvestSkipURIDAO", harvestSkipURIDAO);
        CaomValidator.assertNotNull(FileSyncJob.class, "artifactDAO", artifactDAO);
        CaomValidator.assertNotNull(FileSyncJob.class, "artifactStore", artifactStore);
        CaomValidator.assertNotNull(FileSyncJob.class, "retryAfter", retryAfter);
        CaomValidator.assertNotNull(FileSyncJob.class, "subject", subject);

        this.harvestSkipURI = harvestSkipURI;
        this.harvestSkipURIDAO = harvestSkipURIDAO;
        this.artifactDAO = artifactDAO;
        this.artifactStore = artifactStore;
        this.tolerateNullChecksum = tolerateNullChecksum;
        this.retryAfter = retryAfter;
        this.subject = subject;

        this.dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    @Override
    public void run() {
        Subject currentSubject = new Subject();

        // Also synchronized in FileSync.run()
        synchronized (this.subject) {
            currentSubject.getPrincipals().addAll(this.subject.getPrincipals());
            currentSubject.getPublicCredentials().addAll(this.subject.getPublicCredentials());
        }
        Subject.doAs(currentSubject, new RunnableAction(this::doSync));
    }

    private void doSync() {

        logStart(this.harvestSkipURI);
        Profiler profiler = new Profiler(FileSyncJob.class);
        ArtifactDownloadResult result = new ArtifactDownloadResult(this.harvestSkipURI.getSkipID());

        try {
            URI artifactURI = this.harvestSkipURI.getSkipID();
            final Artifact artifact = this.artifactDAO.get(artifactURI);
            profiler.checkpoint("artifactDAO.get");

            if (artifact == null) {
                // artifact no longer exists, remove from skip uri table
                this.harvestSkipURIDAO.delete(this.harvestSkipURI);
                result.message = "artifact no longer exists";
                return;
            }

            FileMetadata metadata = new FileMetadata();
            metadata.setContentType(artifact.contentType);
            metadata.setContentLength(artifact.contentLength);
            if (artifact.contentChecksum == null) {
                if (!this.tolerateNullChecksum) {
                    result.message = "artifact checksum is null";
                    return;
                }
            } else {
                String checksumFromCAOM = artifact.contentChecksum.getSchemeSpecificPart();
                metadata.setMd5Sum(checksumFromCAOM);
                result.md5sumMessage = "(md5sum from CAOM was " + checksumFromCAOM + ")";
                log.debug(artifactURI.getScheme() + " content MD5 from CAOM: " + checksumFromCAOM);
            }

            // get the md5 and contentLength of the artifact
            CaomArtifactResolver caomArtifactResolver = new CaomArtifactResolver();
            URL url = caomArtifactResolver.getURL(artifactURI);
            log.debug("caomArtifactResolver url: " + url);

            OutputStream out = new ByteArrayOutputStream();
            HttpGet head = new HttpGet(url, out);
            head.setConnectionTimeout(DEFAULT_TIMEOUT);
            head.setReadTimeout(DEFAULT_TIMEOUT);
            head.setHeadOnly(true);
            head.run();
            int respCode = head.getResponseCode();
            profiler.checkpoint("remote.httpHead");

            if (head.getThrowable() != null || respCode != 200) {
                StringBuilder sb = new StringBuilder("(" + respCode + ") ");
                if (head.getThrowable() != null) {
                    sb.append(head.getThrowable().getMessage());
                    log.debug("Error determining artifact checksum: " + sb, head.getThrowable());
                    result.message = sb.toString();
                    return;
                }

                String md5String = head.getContentMD5();
                log.debug(artifactURI.getScheme() + " content MD5: " + md5String);
                if (md5String != null) {
                    URI sourceChecksum = URI.create("MD5:" + md5String);
                    if (!sourceChecksum.equals(artifact.contentChecksum)) {
                        result.message = "Remote checksum doesn't match artifact checksum";
                        return;
                    }
                }

                long contentLength = head.getContentLength();
                if (contentLength >= 0) {
                    if (contentLength != artifact.contentLength) {
                        result.message = "Remote content length doesn't match artifact content length";
                        return;
                    }
                }
            } else {
                // if we don't have a checksum yet and if checksum in header is not null, use it
                String md5FromHeader = head.getContentMD5();
                if (md5FromHeader != null) {
                    if (metadata.getMd5Sum() == null) {
                        metadata.setMd5Sum(md5FromHeader);
                        result.md5sumMessage = "(md5sum from Http header was " + md5FromHeader + ")";
                        log.debug(artifactURI.getScheme() + " content MD5 from header: " + md5FromHeader);
                    } else {
                        // both md5sum from CAOM and md5sum from Http header are not null
                        if (!metadata.getMd5Sum().equals(md5FromHeader)) {
                            String msg = "md5Sums are different, CAOM: " + metadata.getMd5Sum()
                                + ", Http header: " + md5FromHeader;
                            throw new RuntimeException(msg);
                        }
                    }
                }
            }

            // check again to be sure the destination doesn't already have it
            ArtifactMetadata tempMetadata = this.artifactStore.get(artifactURI);
            if (tempMetadata != null && tempMetadata.getChecksum() != null
                && tempMetadata.getChecksum().equals(artifact.contentChecksum.getSchemeSpecificPart())) {
                result.message = "ArtifactStore already has correct copy";
                result.success = true;
                return;
            }
            profiler.checkpoint("local.httpHead");

            InputStreamWrapper wrapper = inputStream -> {
                String threadName = Thread.currentThread().getName();
                URI artifactURI1 = this.harvestSkipURI.getSkipID();
                ByteCountInputStream byteCounter = new ByteCountInputStream(inputStream);
                try {
                    log.debug("[" + threadName + "] Starting upload of " + artifactURI1);
                    this.artifactStore.store(artifactURI1, byteCounter, metadata);
                    result.uploadSuccess = true;
                    log.debug("[" + threadName + "] Completed upload of " + artifactURI1);
                } catch (Throwable t) {
                    result.uploadSuccess = false;
                    // uncomment to obtain stack traces in log file
                    // threadLog.error("[" + threadName + "] Failed to upload " + artifactURI, t);
                    result.uploadErrorMessage = "Upload error: " + t.getMessage();
                    throw new IOException(t);
                } finally {
                    result.bytesTransferred = byteCounter.getByteCount();
                }
            };
            HttpGet download = new HttpGet(url, wrapper);
            download.setConnectionTimeout(DEFAULT_TIMEOUT);
            download.setReadTimeout(DEFAULT_TIMEOUT);

            log.debug("Starting download of " + artifactURI + " from " + url);
            long dlStart = System.currentTimeMillis();
            download.run();
            result.elapsedTimeMillis = System.currentTimeMillis() - dlStart;

            respCode = download.getResponseCode();
            log.debug("Download response code: " + respCode);
            profiler.checkpoint("download/upload");

            if (download.getThrowable() != null || respCode != 200) {
                StringBuilder sb = new StringBuilder();
                sb.append("Download error (").append(respCode).append(")");
                if (download.getThrowable() != null) {
                    sb.append(": ").append(download.getThrowable().getMessage());
                }
                result.message = sb.toString();
            } else {
                if (result.uploadSuccess) {
                    log.debug("Completed download of " + artifactURI + " from " + url);
                    result.success = true;
                } else {
                    if (result.md5sumMessage == null) {
                        result.message = result.uploadErrorMessage;
                    } else {
                        result.message = result.uploadErrorMessage + " " + result.md5sumMessage;
                    }
                }
            }
        } catch (Throwable t) {
            // unexpected error
            log.error("unexpected", t);
            result.message = "unexpected error: " + t.getMessage();
        } finally {
            // Update the skip table
            try {
                synchronized (this.harvestSkipURIDAO) {
                    if (result.success) {
                        this.harvestSkipURIDAO.delete(harvestSkipURI);
                        profiler.checkpoint("harvestSkipURIDAO.delete");
                    } else {
                        result.bytesTransferred = 0;
                        harvestSkipURI.errorMessage = result.message;
                        harvestSkipURI.setTryAfter(this.retryAfter);
                        this.harvestSkipURIDAO.put(harvestSkipURI);
                        profiler.checkpoint("harvestSkipURIDAO.update");
                    }
                }
            } catch (Throwable t) {
                log.error("Failed to update or delete from skip table", t);
            }
            logEnd(result);
        }
    }

    private void logStart(HarvestSkipURI skip) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("FileSyncJob.START: {");
        startMessage.append("\"artifact\":\"").append(skip.getSkipID()).append("\"");
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(this.dateFormat.format(new Date())).append("\"");
        startMessage.append("}");
        log.info(startMessage.toString());
    }

    private void logEnd(ArtifactDownloadResult result) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("FileSyncJob.END: {");
        startMessage.append("\"artifact\":\"").append(result.artifactURI).append("\"");
        startMessage.append(",");
        startMessage.append("\"success\":\"").append(result.success).append("\"");
        startMessage.append(",");
        startMessage.append("\"time\":\"").append(result.elapsedTimeMillis).append("\"");
        startMessage.append(",");
        startMessage.append("\"bytes\":\"").append(result.bytesTransferred).append("\"");
        if (result.message != null) {
            startMessage.append(",");
            startMessage.append("\"message\":\"").append(result.message).append("\"");
        }
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(this.dateFormat.format(new Date())).append("\"");
        startMessage.append("}");
        log.info(startMessage.toString());
    }

    static class ArtifactDownloadResult {
        URI artifactURI;
        boolean success = false;
        boolean uploadSuccess = false;
        String message;
        String uploadErrorMessage;
        String md5sumMessage;
        long elapsedTimeMillis = 0;
        long bytesTransferred = 0;

        ArtifactDownloadResult(URI artifactURI) {
            this.artifactURI = artifactURI;
        }
    }

}
