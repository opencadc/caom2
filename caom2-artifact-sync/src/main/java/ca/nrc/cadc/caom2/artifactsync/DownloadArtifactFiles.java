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
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.resolvers.MastResolver;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.profiler.Profiler;
import ca.nrc.cadc.util.FileMetadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;
import org.apache.log4j.Logger;

public class DownloadArtifactFiles implements PrivilegedExceptionAction<Integer>, ShutdownListener {

    private static final Logger log = Logger.getLogger(DownloadArtifactFiles.class);

    private static final int DEFAULT_RETRY_AFTER_ERROR_HOURS = 24;

    private ArtifactStore artifactStore;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private ArtifactDAO artifactDAO;
    private String source;
    private int batchSize;
    private int threads;
    private boolean verify;
    private Date startDate = null;
    private Date stopDate;
    private int retryAfterHours;
    private DateFormat df;
    
    ExecutorService executor = null;
    List<Future<ArtifactDownloadResult>>  results;
    long start;

    public DownloadArtifactFiles(ArtifactDAO artifactDAO, String[] dbInfo, ArtifactStore artifactStore, int threads,
                                 int batchSize, Integer retryAfterHours, boolean verify) {
        this.artifactStore = artifactStore;

        this.artifactDAO = artifactDAO;
        this.source = dbInfo[0] + "." + dbInfo[1] + "." + dbInfo[2];
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(artifactDAO.getDataSource(), dbInfo[1], dbInfo[2]);
        
        this.threads = threads;
        this.batchSize = batchSize;
        this.verify = verify;
        this.stopDate = new Date();
        if (retryAfterHours == null) {
            retryAfterHours = DEFAULT_RETRY_AFTER_ERROR_HOURS;
        } else {
            this.retryAfterHours = retryAfterHours;
        }
        
        df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    @Override
    public Integer run() throws Exception {

        log.debug("Querying for skip records between " + startDate + " and " + stopDate);
        List<HarvestSkipURI> artifacts = harvestSkipURIDAO.get(source, ArtifactHarvester.STATE_CLASS, startDate,
            stopDate, batchSize);
        executor = Executors.newFixedThreadPool(threads);

        Integer workCount = artifacts.size();
        List<Callable<ArtifactDownloadResult>> tasks = new ArrayList<Callable<ArtifactDownloadResult>>();

        for (HarvestSkipURI skip : artifacts) {
            ArtifactDownloader downloader = new ArtifactDownloader(skip, artifactStore, harvestSkipURIDAO);
            tasks.add(downloader);
        }
        // set the start date so that the next batch resumes after our last record
        if (workCount > 0) {
            startDate = artifacts.get(workCount - 1).getTryAfter();
        }
        
        // reset each batch
        long successes = 0;
        long totalElapsedTime = 0;
        long totalBytes = 0;
        results = new ArrayList<Future<ArtifactDownloadResult>>();
        start = System.currentTimeMillis();
        
        try {
            // submit the results asynchronously
            for (Callable<ArtifactDownloadResult> task : tasks) {
                results.add(executor.submit(task));
            }
            
            // let pool know no new tasks can be added
            executor.shutdown();
            
            // wait for them to complete by calling f.get()
            for (Future<ArtifactDownloadResult> f : results) {
                try {
                    ArtifactDownloadResult result = f.get();
                    if (result.success) {
                        successes++;
                        totalElapsedTime += result.elapsedTimeMillis;
                        totalBytes += result.bytesTransferred;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.info("Thread execution error", e);
                } finally {
                    if (!f.isDone()) {
                        log.info("Manually stopping task");
                        f.cancel(true);
                    }
                }
            }
        } catch (Exception e) {
            log.info("Thread pool error", e);
        } finally {
            if (executor != null && !executor.isShutdown()) {
                log.warn("Manually shutting down thread pool");
                executor.shutdownNow();
            }
            executor = null;
            logBatchEnd(results.size(), successes, totalElapsedTime, totalBytes);
        }

        return workCount;
    }

    class ArtifactDownloader implements Callable<ArtifactDownloadResult>, InputStreamWrapper {

        final HarvestSkipURI skip;
        final ArtifactStore artifactStore;
        final HarvestSkipURIDAO harvestSkipURIDAO;
        boolean uploadSuccess = true;
        String uploadErrorMessage;
        long bytesTransferred;
        
        Logger threadLog = Logger.getLogger(ArtifactDownloader.class);

        FileMetadata metadata;

        ArtifactDownloader(HarvestSkipURI skip, ArtifactStore artifactStore, HarvestSkipURIDAO harvestSkipURIDAO) {
            this.skip = skip;
            this.artifactStore = artifactStore;
            this.harvestSkipURIDAO = harvestSkipURIDAO;
        }

        @Override
        public ArtifactDownloadResult call() throws Exception {

            Profiler profiler = new Profiler(ArtifactDownloader.class);
            logStart(threadLog, skip);
            
            ArtifactDownloadResult result = null;
            
            try {

                URI artifactURI = skip.getSkipID();
                final Artifact artifact = artifactDAO.get(artifactURI);
                profiler.checkpoint("artifactDAO.get");
                
                result = new ArtifactDownloadResult(artifactURI);
                result.success = false;
            
                if (artifact == null) {
                    // artifact no longer exists, remove from skip uri table
                    threadLog.debug("Artifact no longer exists, removing from skip uri table");
                    harvestSkipURIDAO.delete(skip);
                    result.message = "Artifact no longer exists";
                    return result;
                }
                
                MastResolver resolver = new MastResolver();
                final URL url = resolver.toURL(artifactURI);
    
                metadata = new FileMetadata();
                metadata.setContentType(artifact.contentType);
                metadata.setMd5Sum(artifact.contentChecksum.getSchemeSpecificPart());
                metadata.setContentLength(artifact.contentLength);
            
                // get the md5 and contentLength of the artifact
                OutputStream out = new ByteArrayOutputStream();
                HttpDownload head = new HttpDownload(url, out);
                head.setHeadOnly(true);
                head.run();
                int respCode = head.getResponseCode();
                profiler.checkpoint("remote.httpHead");

                if (head.getThrowable() != null || respCode != 200) {
                    StringBuilder sb = new StringBuilder("(" + respCode + ") ");
                    if (head.getThrowable() != null) {
                        sb.append(head.getThrowable().getMessage());
                        threadLog.debug("Error determining artifact checksum: " + sb.toString(), head.getThrowable());
                        result.message = sb.toString();
                        return result;
                    }

                    String md5String = head.getContentMD5();
                    threadLog.debug("MAST content MD5: " + md5String);
                    if (md5String != null) {
                        URI sourceChecksum = URI.create("MD5:" + md5String);
                        if (!sourceChecksum.equals(artifact.contentChecksum)) {
                            result.message = "Remote checksum doesn't match artifact checksum";
                            return result;
                        }
                    }

                    long contentLength = head.getContentLength();
                    if (contentLength >= 0) {
                        if (contentLength != artifact.contentLength) {
                            result.message = "Remote content length doesn't match artifact content length";
                            return result;
                        }
                    }
                }

                // check again to be sure the destination doesn't already have it
                if (artifactStore.contains(artifactURI, artifact.contentChecksum)) {
                    result.message = "ArtifactStore already has correct copy";
                    result.success = true;
                    return result;
                }
                profiler.checkpoint("local.httpHead");

                HttpDownload download = new HttpDownload(url, this);

                threadLog.debug("Starting download of " + artifactURI + " from " + url);
                long start = System.currentTimeMillis();
                download.run();
                threadLog.debug("Completed download of " + artifactURI + " from " + url);
                result.elapsedTimeMillis = System.currentTimeMillis() - start;
                
                respCode = download.getResponseCode();
                threadLog.debug("Download response code: " + respCode);
                profiler.checkpoint("download/upload");

                if (download.getThrowable() != null || respCode != 200) {
                    StringBuilder sb = new StringBuilder("Download error (" + respCode + ")");
                    if (download.getThrowable() != null) {
                        sb.append(": " + download.getThrowable().getMessage());
                    }
                    result.message = sb.toString();
                } else {
                    if (uploadSuccess) {
                        if (verify) {
                            if (!artifactStore.contains(artifactURI,artifact.contentChecksum)) {
                                String msgDetail = "Artifact with checksum [" + artifact.contentChecksum + "] not in storage.";
                                result.message = "Post download verification failure: " + msgDetail;
                            } else {
                                result.success = true;
                            }
                            profiler.checkpoint("local.httpHead.verify");
                        } else {
                            result.success = true;
                        }
                    } else {
                        result.message = uploadErrorMessage;
                    }
                }

                return result;
                
            } catch (Throwable t) {
                // unexpected error
                log.error("unexpected", t);
                result.message = "unexpected error: " + t.getMessage();
                return result;
                
            } finally {
                // Update the skip table
                try {
                    synchronized (harvestSkipURIDAO) {
                        if (result.success) {
                            result.bytesTransferred = bytesTransferred;
                            harvestSkipURIDAO.delete(skip);
                            profiler.checkpoint("harvestSkipURIDAO.delete");
                        } else {
                            skip.errorMessage = result.message;
                            Date tryAfter = getTryAfter();
                            skip.setTryAfter(tryAfter);
                            harvestSkipURIDAO.put(skip);
                            profiler.checkpoint("harvestSkipURIDAO.update");
                        }
                    }
                    
                } catch (Throwable t) {
                    threadLog.error("Failed to update or delete from skip table", t);
                }
                logEnd(threadLog, result);
                threadLog = null;
            }
        }

        private Date getTryAfter() {
            Calendar c = Calendar.getInstance();
            c.add(Calendar.HOUR, retryAfterHours);
            return c.getTime();
        }

        @Override
        public void read(InputStream inputStream) throws IOException {
            String threadName = Thread.currentThread().getName();
            URI artifactURI = skip.getSkipID();
            ByteCountInputStream byteCounter = new ByteCountInputStream(inputStream);
            try {
                threadLog.debug("[" + threadName + "] Starting upload of " + artifactURI);
                artifactStore.store(artifactURI, byteCounter, metadata);
                threadLog.debug("[" + threadName + "] Completed upload of " + artifactURI);
            } catch (Throwable t) {
                uploadSuccess = false;
                threadLog.error("[" + threadName + "] Failed to upload " + artifactURI, t);
                uploadErrorMessage = "Upload error: " + t.getMessage();
            } finally {
                bytesTransferred = byteCounter.getByteCount();
            }
        }
    }

    class ArtifactDownloadResult {
        URI artifactURI;
        boolean success;
        String message;
        long elapsedTimeMillis;
        long bytesTransferred = 0;

        ArtifactDownloadResult(URI artifactURI) {
            this.artifactURI = artifactURI;
        }
    }

    private void logStart(Logger threadLog, HarvestSkipURI skip) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("START: {");
        startMessage.append("\"artifact\":\"").append(skip.getSkipID()).append("\"");
        startMessage.append(",");
        startMessage.append("\"date\":\"").append(df.format(new Date())).append("\"");
        startMessage.append("}");
        threadLog.info(startMessage.toString());
    }

    private void logEnd(Logger threadLog, ArtifactDownloadResult result) {
        StringBuilder startMessage = new StringBuilder();
        startMessage.append("END: {");
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
        startMessage.append("\"date\":\"").append(df.format(new Date())).append("\"");
        startMessage.append("}");
        threadLog.info(startMessage.toString());
    }
    
    private void logBatchEnd(long total, long successes, long totalElapsedTime, long totalBytes) {
        final long end = System.currentTimeMillis() - start;
        StringBuilder endMessage = new StringBuilder();
        endMessage.append("ENDBATCH: {");
        endMessage.append("\"total\":\"").append(total).append("\"");
        endMessage.append(",");
        endMessage.append("\"successCount\":\"").append(successes).append("\"");
        endMessage.append(",");
        endMessage.append("\"failureCount\":\"").append(total - successes).append("\"");
        endMessage.append(",");
        endMessage.append("\"time\":\"").append(end).append("\"");
        endMessage.append(",");
        endMessage.append("\"date\":\"").append(df.format(new Date())).append("\"");
        endMessage.append(",");
        endMessage.append("\"downloadTime\":\"").append(totalElapsedTime).append("\"");
        endMessage.append(",");
        endMessage.append("\"bytes\":\"").append(totalBytes).append("\"");
        endMessage.append(",");
        endMessage.append("\"threads\":\"").append(threads).append("\"");
        endMessage.append("}");
        log.info(endMessage.toString());
    }

    @Override
    public void shutdown() {
        if (executor != null) {
            log.info("Shutting down downloader.");
            List<Runnable> incomplete = executor.shutdownNow();
            
            if (results == null) {
                StringBuilder endMessage = new StringBuilder();
                endMessage.append("ENDBATCH: {");
                endMessage.append("\"total\":\"0\"");
                endMessage.append(",");
                endMessage.append("\"successCount\":\"0\"");
                endMessage.append(",");
                endMessage.append("\"failureCount\":\"0\"");
                endMessage.append(",");
                endMessage.append("\"date\":\"").append(df.format(new Date())).append("\"");
                endMessage.append(",");
                endMessage.append("\"threads\":\"").append(threads).append("\"");
                endMessage.append("}");
                log.info(endMessage.toString());
            } else {

                long total = 0;
                long successes = 0;
                long totalElapsedTime = 0;
                long totalBytes = 0;
                
                // wait for them to complete by calling f.get()
                for (Future<ArtifactDownloadResult> f : results) {
                    if (f.isDone()) {
                        try {
                            ArtifactDownloadResult result = f.get();
                            total++;
                            if (result.success) {
                                successes++;
                                totalElapsedTime += result.elapsedTimeMillis;
                                totalBytes += result.bytesTransferred;
                            }
                        } catch (ExecutionException | InterruptedException e) {
                            log.info("Failed to get result of completed job", e);
                        }
                    }
                }
                
                logBatchEnd(total, successes, totalElapsedTime, totalBytes);
            }
            
            try {
                executor.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.info("Shutdown interruped");
            }
            if (incomplete != null) {
                log.info("Incomplete downloads: " + incomplete.size());
            }
        }
    }

}
