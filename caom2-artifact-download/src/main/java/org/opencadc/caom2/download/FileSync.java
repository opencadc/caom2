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
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.thread.ThreadedRunnableExecutor;
import ca.nrc.cadc.util.BucketSelector;
import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * FileSync code that queries for and queues download jobs.
 * 
 * @author pdowler
 */
public class FileSync implements Runnable {
    private static final Logger log = Logger.getLogger(FileSync.class);

    private static final int MAX_THREADS = 64;

    // The number of hours that the validity checker for the current Subject will request ahead to see if the Subject's
    // X500 certificate is about to expire.  This will also be used to update to schedule updates to the Subject's
    // credentials.
    private static final int CERT_CHECK_MINUTE_INTERVAL_COUNT = 10;

    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";

    private final ArtifactStore artifactStore;
    private final BucketSelector buckets;
    private final int retryAfterHours;
    
    private final HarvestSkipURIDAO harvestSkipURIDAO;
    //private final ArtifactDAO artifactDAO;
    //private final HarvestSkipURIDAO jobHarvestSkipURIDAO;
    private final DataSource jobDataSource;
    private final String database;
    private final String schema;
    private final Map<String, Object> daoConfig;
    
    private final ThreadedRunnableExecutor threadPool;
    private final LinkedBlockingQueue<Runnable> jobQueue;
    private final String storageNamespace;

    // test usage only
    int testRunLoops = 0; // default: forever

    /**
     * Constructor.
     *
     * @param daoConfig config map to pass to cadc-inventory-db DAO classes
     * @param connectionConfig ConnectionConfig object to use for creating jndiDataSource
     * @param artifactStore back end storage
     * @param buckets uriBucket prefix or range of prefixes
     * @param threads number of threads in download thread pool
     * @param retryAfterHours hours after which to retry failed downloads
     */
    public FileSync(Map<String, Object> daoConfig, ConnectionConfig connectionConfig, ArtifactStore artifactStore,
                    String storageNamespace, BucketSelector buckets, 
                    int threads, int retryAfterHours) {

        CaomValidator.assertNotNull(FileSync.class, "daoConfig", daoConfig);
        CaomValidator.assertNotNull(FileSync.class, "connectionConfig", connectionConfig);
        CaomValidator.assertNotNull(FileSync.class, "artifactStore", artifactStore);
        CaomValidator.assertNotNull(FileSync.class, "uriPrefixes", buckets);

        this.artifactStore = artifactStore;
        this.buckets = buckets;
        this.retryAfterHours = retryAfterHours;
        
        if (threads <= 0 || threads > MAX_THREADS) {
            throw new IllegalArgumentException(String.format("invalid config: threads must be in [1,%s], found: %s",
                                                             MAX_THREADS, threads));
        }

        try {
            // make single connection data source to query and create jobs
            final String fileSyncDS = "jdbc/fileSync";
            DBUtil.createJNDIDataSource(fileSyncDS, connectionConfig);
            this.harvestSkipURIDAO = new HarvestSkipURIDAO(DBUtil.findJNDIDataSource(fileSyncDS),
                                                           (String) daoConfig.get("database"),
                                                           (String) daoConfig.get("schema"));

            // make shared connection pool for jobs
            final String jobDS = "jdbc/fileSyncJob";
            daoConfig.put("jndiDataSourceName", jobDS);
            // 2-3 threads ~2 connection, 4-5 threads ~3 connections, ... 30-31 threads ~16 connections
            int poolSize = 1 + threads / 2;
            DBUtil.PoolConfig pc = new DBUtil.PoolConfig(connectionConfig, poolSize, 20000L, "select 123");
            DBUtil.createJNDIDataSource(jobDS, pc);
            this.jobDataSource = DBUtil.findJNDIDataSource(jobDS);
            this.database = null; // unused (String) daoConfig.get("database");
            this.schema = (String) daoConfig.get("schema");
            this.daoConfig = daoConfig;
            
            try {
                DataSource ds = DBUtil.findJNDIDataSource(fileSyncDS);
                InitDatabase init = new InitDatabase(ds, database, schema);
                init.doInit();
                log.info("initDatabase: " + schema + " OK");
            } catch (Exception ex) {
                throw new IllegalStateException("check/init database failed", ex);
            }

        } catch (NamingException ne) {
            throw new IllegalStateException("unable to access database: " + daoConfig.get("database"), ne);
        }

        // jobQueue is the queue used in this producer/consumer implementation.
        // producer: FileSync uses jobQueue.put(); blocks if queue is full
        // consumer: ThreadPool uses jobQueue.take(); blocks if queue is empty
        this.jobQueue = new LinkedBlockingQueue<>(threads * 2);
        this.threadPool = new ThreadedRunnableExecutor(this.jobQueue, threads);

        this.storageNamespace = null; // all

        log.debug("FileSync ctor done");
    }

    // start a Scheduler thread to renew the subject periodically.
    public static void scheduleSubjectUpdates(final Subject subject) {
        log.debug("START: scheduleSubjectUpdates");
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.schedule(() -> {
            // Also synchronized on FileSyncJob.run().
            synchronized (subject) {
                SSLUtil.renewSubject(subject, new File(CERTIFICATE_FILE_LOCATION));
            }
        }, CERT_CHECK_MINUTE_INTERVAL_COUNT, TimeUnit.MINUTES);
        log.debug("END: scheduleSubjectUpdates OK");
    }

    @Override
    public void run() {
        final File certificateFile = new File(CERTIFICATE_FILE_LOCATION);
        final Subject subject = certificateFile.exists()
            ? SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION))
            : AuthenticationUtil.getAnonSubject();
        scheduleSubjectUpdates(subject);
        doit(subject);
    }

    // package access for test code
    void doit(final Subject currentUser) {
        // poll time while watching job queue to empty
        long poll = 30 * 1000L; // 30 sec
        if (testRunLoops > 0) {
            poll = 100L;
        }
        // idle time from when jobs finish until next query
        long idle = 10 * poll;

        boolean ok = true;
        long loopCount = 0;
        while (ok) {
            try {
                loopCount++;
                final long startQ = System.currentTimeMillis();
                long num = 0L;
                log.debug("FileSync.QUERY START");

                String minBucket = buckets.getMinBucket(HarvestSkipURIDAO.BUCKET_LENGTH);
                String maxBucket = buckets.getMaxBucket(HarvestSkipURIDAO.BUCKET_LENGTH);
                Date now = new Date();
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                log.info("FileSync.QUERY buckets=" + minBucket + "-" + maxBucket + " end=" + df.format(now));
                try (final ResourceIterator<HarvestSkipURI> skipIterator =
                    harvestSkipURIDAO.iterator(Artifact.class.getSimpleName(), storageNamespace, minBucket, maxBucket, now)) {
                    while (skipIterator.hasNext()) {
                        HarvestSkipURI harvestSkip = skipIterator.next();
                        // DAO classes may not be thread safe so we have to create for each job
                        // but can use the shared connection pool
                        ArtifactDAO ad = new ArtifactDAO();
                        ad.setConfig(daoConfig);
                        HarvestSkipURIDAO hsd = new HarvestSkipURIDAO(jobDataSource, database, schema);

                        FileSyncJob fileSyncJob = new FileSyncJob(harvestSkip, 
                                hsd, ad, artifactStore, retryAfterHours, currentUser);
                        jobQueue.put(fileSyncJob); // blocks when queue capacity is reached
                        log.info("FileSync.CREATE: " + harvestSkip.getSkipID() + " " + df.format(harvestSkip.getTryAfter()));
                        num++;
                    }
                } catch (Exception e) {
                    // TODO:  handle errors from this more sanely after they
                    // are available from the cadc-inventory-db API
                    throw e;
                }
                long dtQ = System.currentTimeMillis() - startQ;
                log.info("FileSync.QUERY END dt=" + dtQ + " num=" + num);

                boolean waiting = true;
                while (waiting) {
                    if (jobQueue.isEmpty()) {
                        // look more closely at state of thread pool
                        if (threadPool.getAllThreadsIdle()) {
                            log.debug("queue empty; jobs complete");
                            waiting = false;
                        } else {
                            log.info("FileSync.POLL dt=" + poll);
                            Thread.sleep(poll);
                        }
                    } else {
                        log.info("FileSync.POLL dt=" + poll);
                        Thread.sleep(poll);
                    }

                }
                if (testRunLoops > 0 && loopCount >= testRunLoops) {
                    log.warn("TEST MODE: testRunLoops=" + testRunLoops + " ... terminating!");
                    ok = false;
                }
            } catch (Exception e) {
                log.error("fatal error - terminating", e);
                ok = false;
            }
            if (ok) {
                try {
                    log.info("FileSync.IDLE dt=" + idle);
                    Thread.sleep(idle);
                } catch (InterruptedException ex) {
                    ok = false;
                }
            }
        }
        if (testRunLoops > 0) {
            log.warn("TEST MODE: testRunLoops=" + testRunLoops + " ... threadPool.terminate");
        }
        this.threadPool.terminate();
    }

}
