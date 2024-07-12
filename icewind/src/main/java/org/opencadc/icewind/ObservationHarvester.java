/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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

package org.opencadc.icewind;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.CaomWCSValidator;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.BadSqlGrammarException;

/**
 *
 * @author pdowler
 */
public class ObservationHarvester extends Harvester {

    private static final Logger log = Logger.getLogger(ObservationHarvester.class);

    private final URI basePublisherID;
    private final boolean nochecksum;
    private RepoClient srcRepoClient;
    private ObservationDAO srcObservationDAO;
    private ObservationDAO destObservationDAO;
    private HarvestSkipURIDAO harvestSkipDAO;
    private boolean skipped;
    private boolean ready = false;
    private int ingested = 0;
    private String errorMessagePattern;

    public ObservationHarvester(HarvesterResource src, HarvesterResource dest, String collection, URI basePublisherID,
                                Integer batchSize, int nthreads, boolean full, boolean nochecksum) {
        super(Observation.class, src, dest, collection, batchSize, full);
        this.basePublisherID = basePublisherID;
        this.nochecksum = nochecksum;
        init(nthreads);
    }

    public void setSkipped(boolean skipped, String errorMessagePattern) {
        this.skipped = skipped;
        this.harvestSkipDAO.errorMessagePattern = errorMessagePattern;
    }

    public void setErrorMessagePattern(String errorMessagePattern) {
        this.errorMessagePattern = errorMessagePattern;
    }
    
    public int getIngested() {
        return this.ingested;
    }

    private void init(int nthreads) {
        this.srcRepoClient = new RepoClient(src.getResourceID(), nthreads);
        // TODO: make these configurable
        srcRepoClient.setConnectionTimeout(18000); // 18 sec
        srcRepoClient.setReadTimeout(120000);      // 2 min
        
        // dest is always a database
        final String destDS = "jdbc/ObservationHarvester";
        Map<String, Object> destConfig = getConfigDAO(dest);
        try {
            DataSource cur = null;
            try {
                cur = DBUtil.findJNDIDataSource(destDS);
            } catch (NamingException notInitialized) {
                log.debug("JNDI not initialized yet... OK");
            }
            if (cur == null) {
                ConnectionConfig destConnectionConfig = new ConnectionConfig(null, null,
                    dest.getUsername(), dest.getPassword(), HarvesterResource.POSTGRESQL_DRIVER, dest.getJdbcUrl());
                DBUtil.createJNDIDataSource(destDS, destConnectionConfig);
            } else {
                log.debug("found DataSource: " + destDS + " -- re-using");
            }
        } catch (NamingException e) {
            throw new IllegalStateException(String.format("Error creating destination JNDI datasource for %s reason: %s",
                    dest, e.getMessage()), e);
        }
        destConfig.put("jndiDataSourceName", destDS);
        destConfig.put("basePublisherID", basePublisherID.toASCIIString());
        this.destObservationDAO = new ObservationDAO();
        destObservationDAO.setConfig(destConfig);
        if (src.getIdentifier(collection).equals(dest.getIdentifier(collection))) {
            log.info("source = destination = " + dest.getIdentifier(collection) + ": setting origin=true");
            destObservationDAO.setOrigin(true); // reproc in a single db should update timestamps
        } else {
            destObservationDAO.setOrigin(false); // copy as-is
        }
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
        
        if (srcRepoClient.isObsAvailable()) {
            ready = true;
        } else {
            log.error("Not available: obs endpoint in " + srcRepoClient.toString());
        }
    }

    private String format(UUID id) {
        if (id == null) {
            return "null";
        }
        return id.toString();
    }

    @Override
    public void run() {
        boolean go = true;
        while (go) {
            Progress num = doit();

            ingested += num.ingested;
            if (num.found > 0) {
                log.debug("***************** finished batch: " + num + " *******************");
            }

            if (num.abort) {
                log.error("batched aborted");
            }
            go = (num.found > 0 && !num.abort && !num.done);
            if (num.found < batchSize / 2) {
                go = false;
            }
            full = false; // do not start at beginning again
        }
    }

    private static class Progress {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int ingested = 0;
        int failed = 0;
        int handled = 0;

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

        if (!ready) {
            log.error("Observation Harvester not ready");
            ret.abort = true;
            return ret;
        }
        long t = System.currentTimeMillis();
        long timeState = -1;
        long timeQuery = -1;
        long timeTransaction = -1;
        int expectedNum = batchSize;

        try {
            System.gc(); // hint
            t = System.currentTimeMillis();

            HarvestState state = null;
            if (!skipped) {
                state = harvestStateDAO.get(source, Observation.class.getSimpleName());
                startDate = state.curLastModified;
                log.debug("state " + state);
            }

            timeState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            if (firstIteration) {
                if (full) {
                    startDate = null;
                } else if (super.minDate != null) {
                    startDate = super.minDate;
                }
                endDate = super.maxDate;
                if (!skipped) {
                    // harvest up to a little in the past because the head of
                    // the sequence may be volatile
                    long fiveMinAgo = System.currentTimeMillis() - 5 * 60000L;
                    if (endDate == null) {
                        endDate = new Date(fiveMinAgo);
                    } else {
                        endDate = new Date(Math.min(fiveMinAgo, endDate.getTime()));
                    }
                }
            }
            firstIteration = false;

            List<SkippedWrapperURI<ObservationResponse>> entityList;
            if (skipped) {
                entityList = getSkipped(startDate);
            } else {
                log.info("harvest window: " + format(startDate) + " :: " + format(endDate) + " [" + batchSize + "]");
                List<ObservationResponse> obsList;
                if (srcObservationDAO != null) {
                    obsList = srcObservationDAO.getList(collection, startDate, endDate, batchSize + 1);
                } else {
                    obsList = srcRepoClient.getList(collection, startDate, endDate, batchSize + 1);
                }
                entityList = wrap(obsList);
            }

            // avoid re-processing the last successful one stored in
            // HarvestState (normal case because query: >= startDate)
            if (!entityList.isEmpty() && !skipped) {
                ListIterator<SkippedWrapperURI<ObservationResponse>> iter = entityList.listIterator();
                Observation curBatchLeader = iter.next().entity.observation;
                if (curBatchLeader != null) {
                    log.debug("currentBatch: " + curBatchLeader.getURI() + " " + format(curBatchLeader.getMaxLastModified()));
                    log.debug("harvestState: " + format(state.curID) + " " + format(state.curLastModified));
                    if (curBatchLeader.getID().equals(state.curID) && curBatchLeader.getMaxLastModified().equals(state.curLastModified)) {
                        iter.remove();
                        expectedNum--;
                    }
                }
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());

            timeQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<SkippedWrapperURI<ObservationResponse>> iter1 = entityList.listIterator();
            // int i = 0;
            while (iter1.hasNext()) {
                SkippedWrapperURI<ObservationResponse> ow = iter1.next();
                Observation o = null;
                if (ow.entity != null) {
                    o = ow.entity.observation;
                }
                HarvestSkipURI hs = ow.skip;
                iter1.remove(); // allow garbage collection during loop

                String skipMsg = null;

                
                if (destObservationDAO.getTransactionManager().isOpen()) {
                    throw new RuntimeException("BUG: found open transaction at start of next observation");
                }
                long tmp = System.currentTimeMillis();
                log.debug("starting transaction");
                destObservationDAO.getTransactionManager().startTransaction();
                boolean ok = false;
                long txnStartTime = System.currentTimeMillis() - tmp;
                log.debug("skipped=" + skipped
                        + " o=" + o
                        + " ow.entity=" + ow.entity
                        + " ow.entity.error=" + (ow.entity == null ? null : ow.entity.error));
                try {
                    // o could be null in skip mode cleanup
                    if (o != null) {
                        String treeSize = computeTreeSize(o);
                        log.info("put: " + o.getClass().getSimpleName() + " " + o.getURI() + " " + format(o.getMaxLastModified()) + " " + treeSize);
                    } else if (hs != null) {
                        log.info("put (retry error): " + hs.getName() + " " + hs.getSkipID() + " " + format(hs.getLastModified()));
                    } else {
                        log.info("put (error): Observation " + ow.entity.observationState.getURI() + " " + format(ow.entity.observationState.maxLastModified));
                    }

                    if (skipped) {
                        startDate = hs.getTryAfter();
                    }

                    if (o != null) {
                        if (state != null) {
                            state.curLastModified = o.getMaxLastModified();
                            state.curID = o.getID();
                        }

                        // try to avoid DataIntegrityViolationException due
                        // to missed deletion followed by insert with new
                        // UUID
                        ObservationState cur = destObservationDAO.getState(o.getURI());
                        if (cur != null && !cur.getID().equals(o.getID())) {
                            // missed harvesting a deletion: trust source
                            log.info("delete: " + o.getClass().getSimpleName() + " " + cur.getURI() + " " + cur.getID()
                                    + " (ObservationURI conflict avoided)");
                            destObservationDAO.delete(cur.getID());
                        }

                        // verify we retrieved the observation intact
                        if (!nochecksum) {
                            validateChecksum(o);
                        }

                        // extended content verification
                        CaomValidator.validate(o);

                        for (Plane p : o.getPlanes()) {
                            for (Artifact a : p.getArtifacts()) {
                                CaomWCSValidator.validate(a);
                            }
                        }

                        // everything is OK
                        destObservationDAO.put(o);

                        if (!skipped) {
                            harvestStateDAO.put(state);
                        }

                        if (hs == null) {
                            // normal harvest mode: try to cleanup skip
                            // records immediately
                            hs = harvestSkipDAO.get(source, cname, o.getURI().getURI());
                        }

                        if (hs != null) {
                            String emsg = hs.errorMessage;
                            if (emsg.length() > 32) {
                                emsg = emsg.substring(0, 32);
                            }
                            log.info("delete: " + hs.getClass().getSimpleName() + "[" + hs.getSkipID() + " " + emsg + "]");
                            harvestSkipDAO.delete(hs);
                        }
                    } else if (skipped) {
                        // o == null
                        if (srcObservationDAO != null || ow.entity.error instanceof ResourceNotFoundException) {
                            // observation not obtainable from source == missed deletion
                            ObservationURI uri = new ObservationURI(hs.getSkipID());
                            log.info("delete: " + uri);
                            destObservationDAO.delete(uri);
                            log.info("delete: " + hs.getClass().getSimpleName() + "[" + hs.getSkipID() + "]");
                            harvestSkipDAO.delete(hs);
                        } else {
                            // defer to the main catch for error handling
                            throw ow.entity.error;
                        }
                    } else if (ow.entity.error != null) {
                        // o == null when harvesting from service: try to make progress on failures
                        if (state != null && ow.entity.observationState.maxLastModified != null) {
                            state.curLastModified = ow.entity.observationState.maxLastModified;
                            state.curID = null; // unknown
                        }
                        //if (srcObservationDAO != null || ow.entity.error instanceof ResourceNotFoundException) {
                        if (ow.entity.error instanceof ResourceNotFoundException) {
                            ObservationURI uri = ow.entity.observationState.getURI();
                            log.info("delete: " + uri);
                            destObservationDAO.delete(uri);
                            if (hs != null) {
                                log.info("delete: " + hs + " " + format(hs.getLastModified()));
                                harvestSkipDAO.delete(hs);
                            }
                        } else {
                            throw ow.entity.error;
                        }
                    }

                    tmp = System.currentTimeMillis();
                    log.debug("committing transaction");
                    destObservationDAO.getTransactionManager().commitTransaction();
                    log.debug("commit: OK");
                    long txnCommitTime = System.currentTimeMillis() - tmp;
                    log.warn("transaction: start=" + txnStartTime + " commit=" + txnCommitTime);
                    ok = true;
                    ret.ingested++;
                } catch (IllegalStateException oops) {
                    if (oops.getMessage().contains("XML failed schema validation")) {
                        log.error("CONTENT PROBLEM - XML failed schema validation: " + oops.getMessage());
                        ret.handled++;
                    } else if (oops.getMessage().contains("failed to read")) {
                        log.error("CONTENT PROBLEM - " + oops.getMessage(), oops.getCause());
                        ret.handled++;
                    } else {
                        // TODO
                    }
                    skipMsg = oops.getMessage(); // message for HarvestSkipURI record
                } catch (MismatchedChecksumException oops) {
                    log.error("CONTENT PROBLEM - mismatching checksums: " + ow.entity.observationState.getURI());
                    ret.handled++;
                    skipMsg = oops.getMessage(); // message for HarvestSkipURI record
                } catch (IllegalArgumentException oops) {
                    log.error("CONTENT PROBLEM - invalid observation: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                    if (oops.getCause() != null) {
                        log.error("cause: " + oops.getCause());
                    }
                    ret.handled++;
                    skipMsg = oops.getMessage(); // message for HarvestSkipURI record
                } catch (TransientException oops) {
                    log.error("NETWORK PROBLEM - " + oops.getMessage());
                    ret.handled++;
                    skipMsg = oops.getMessage(); // message for HarvestSkipURI record
                } catch (NullPointerException ex) {
                    log.error("BUG", ex);
                    ret.abort = true;
                    skipMsg = "BUG: " + ex.getClass().getName(); // message for HarvestSkipURI record
                } catch (BadSqlGrammarException ex) {
                    log.error("BUG", ex);
                    BadSqlGrammarException bad = (BadSqlGrammarException) ex;
                    SQLException sex1 = bad.getSQLException();
                    if (sex1 != null) {
                        log.error("CAUSE", sex1);
                        SQLException sex2 = sex1.getNextException();
                        log.error("NEXT CAUSE", sex2);
                    }
                    ret.abort = true;
                    skipMsg = ex.getMessage(); // message for HarvestSkipURI record
                } catch (DataAccessResourceFailureException ex) {
                    log.error("FATAL PROBLEM - probably out of space in database", ex);
                    ret.abort = true;
                    skipMsg = "FATAL: " + ex.getMessage(); // message for HarvestSkipURI record
                } catch (Exception oops) {
                    // need to inspect the error messages
                    log.debug("exception during harvest", oops);
                    skipMsg = null;
                    String str = oops.toString();
                    
                    if (str.contains("duplicate key value violates unique constraint \"i_observationuri\"")) {
                        log.error("CONTENT PROBLEM - duplicate observation: " + ow.entity.observationState.getURI());
                        ret.handled++;
                    } else if (str.contains("spherepoly_from_array")) {
                        log.error("PGSPHERE PROBLEM - failed to persist: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                        oops = new IllegalArgumentException("invalid polygon (spoly): " + oops.getMessage(), oops);
                        ret.handled++;
                    } else if (str.contains("value out of range: underflow")) {
                        log.error("UNDIAGNOSED PROBLEM - failed to persist: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                        ret.handled++;
                    } else {
                        log.error("unexpected exception", oops);
                    }
                    // message for HarvestSkipURI record
                    skipMsg = oops.getMessage();
                } catch (Error err) {
                    log.error("FATAL - probably installation or environment", err);
                    ret.abort = true;
                } finally {
                    if (!ok) {
                        try {
                            destObservationDAO.getTransactionManager().rollbackTransaction();
                            log.debug("rollback: OK");
                            timeTransaction += System.currentTimeMillis() - t;
                        } catch (Exception tex) {
                            log.error("failed to rollback obs transaction", tex);
                        }

                        try {
                            log.debug("starting HarvestSkipURI transaction");
                            HarvestSkipURI skip = null;
                            if (o != null) {
                                skip = harvestSkipDAO.get(source, cname, o.getURI().getURI());
                            } else {
                                skip = harvestSkipDAO.get(source, cname, ow.entity.observationState.getURI().getURI());
                            }
                            Date tryAfter = ow.entity.observationState.maxLastModified;
                            if (o != null) {
                                tryAfter = o.getMaxLastModified();
                            }
                            if (skip == null) {
                                if (o != null) {
                                    skip = new HarvestSkipURI(source, cname, o.getURI().getURI(), tryAfter, skipMsg);
                                } else {
                                    skip = new HarvestSkipURI(source, cname, ow.entity.observationState.getURI().getURI(), tryAfter, skipMsg);
                                }
                            } else {
                                skip.errorMessage = skipMsg;
                                skip.setTryAfter(tryAfter);
                            }

                            log.debug("starting HarvestSkipURI transaction");
                            destObservationDAO.getTransactionManager().startTransaction();

                            if (!skipped) {
                                // track the harvest state progress
                                harvestStateDAO.put(state);
                            }

                            // track the fail
                            log.info("put: " + skip);
                            harvestSkipDAO.put(skip);

                            if (!src.getIdentifier(collection).equals(dest.getIdentifier(collection))) {
                                // delete previous version of observation (if any)
                                log.info("delete: " + ow.entity.observationState.getURI());
                                destObservationDAO.delete(ow.entity.observationState.getURI());
                            }

                            log.debug("committing HarvestSkipURI transaction");
                            destObservationDAO.getTransactionManager().commitTransaction();
                            log.debug("commit HarvestSkipURI: OK");
                        } catch (Throwable oops) {
                            log.warn("failed to insert HarvestSkipURI", oops);
                            try {
                                destObservationDAO.getTransactionManager().rollbackTransaction();
                                log.debug("rollback HarvestSkipURI: OK");
                            } catch (Exception tex) {
                                log.error("failed to rollback skip transaction", tex);
                            }
                            ret.abort = true;
                        }
                        ret.failed++;
                    }
                }
                if (ret.abort) {
                    return ret;
                }
            }
            if (ret.found < expectedNum) {
                ret.done = true;
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("SEVERE PROBLEM - ThreadPool harvesting Observations failed: " + e.getMessage());
            ret.abort = true;
        } catch (Throwable th) {
            log.error("unexpected exception", th);
            throw th;
        } finally {
            timeTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + timeState + "ms");
            log.debug("time to run ObservationListQuery: " + timeQuery + "ms");
            log.debug("time to run transactions: " + timeTransaction + "ms");
        }
        return ret;
    }

    private void validateChecksum(Observation o) throws MismatchedChecksumException {
        if (o.getAccMetaChecksum() == null) {
            return; // no check
        }
        try {
            URI calculatedChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("validateChecksum: " + o.getURI() + " -- " + computeTreeSize(o) + " -- " + o.getAccMetaChecksum() + " vs " + calculatedChecksum);
            if (!calculatedChecksum.equals(o.getAccMetaChecksum())) {
                //detailedChecksumDiagnostics(o);
                throw new MismatchedChecksumException("Observation.accMetaChecksum mismatch");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 digest algorithm not available");
        }
    }
    
    private void detailedChecksumDiagnostics(Observation obs) {
        // code yanked from caom2-validator
        try {
            int depth = 5;
            boolean acc = true;
            StringBuilder cs = new StringBuilder();
            StringBuilder acs = new StringBuilder();
            MessageDigest digest = MessageDigest.getInstance("MD5");
            if (depth > 1) {
                for (Plane pl : obs.getPlanes()) {
                    if (depth > 2) {
                        for (Artifact ar : pl.getArtifacts()) {
                            if (depth > 3) {
                                for (Part pa : ar.getParts()) {
                                    if (depth > 4) {
                                        for (Chunk ch : pa.getChunks()) {
                                            URI chunkCS = ch.computeMetaChecksum(digest);
                                            cs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                            compare(cs, ch.getMetaChecksum(), chunkCS);
                                            if (acc) {
                                                URI chunkACS = ch.computeAccMetaChecksum(digest);
                                                acs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                                compare(acs, ch.getAccMetaChecksum(), chunkACS);
                                            }
                                        }
                                    }
                                    URI partCS = pa.computeMetaChecksum(digest);
                                    cs.append("\n       part: ").append(pa.getID()).append(" ");
                                    compare(cs, pa.getMetaChecksum(), partCS);
                                    if (acc) {
                                        URI partACS = pa.computeAccMetaChecksum(digest);
                                        acs.append("\n      part: ").append(pa.getID()).append(" ");
                                        compare(acs, pa.getAccMetaChecksum(), partACS);
                                    }
                                }
                            }
                            URI artifactCS = ar.computeMetaChecksum(digest);
                            cs.append("\n       artifact: ").append(ar.getID()).append(" ");
                            compare(cs, ar.getMetaChecksum(), artifactCS);
                            if (acc) {
                                URI artifactACS = ar.computeAccMetaChecksum(digest);
                                acs.append("\n      artifact: ").append(ar.getID()).append(" ");
                                compare(acs, ar.getAccMetaChecksum(), artifactACS);
                            }
                        }
                    }
                    URI planeCS = pl.computeMetaChecksum(digest);
                    cs.append("\n      plane: ").append(pl.getID()).append(" ");
                    compare(cs, pl.getMetaChecksum(), planeCS);
                    if (acc) {
                        URI planeACS = pl.computeAccMetaChecksum(digest);
                        acs.append("\n     plane: ").append(pl.getID()).append(" ");
                        compare(acs, pl.getAccMetaChecksum(), planeACS);
                    }
                }
            }
            URI observationCS = obs.computeMetaChecksum(digest);
            cs.append("\nobservation: ").append(obs.getID()).append(" ");
            compare(cs, obs.getMetaChecksum(), observationCS);
            
            if (acc) {
                URI observationACS = obs.computeAccMetaChecksum(digest);
                acs.append("\nobservation: ").append(obs.getID()).append(" ");
                compare(acs, obs.getAccMetaChecksum(), observationACS);
            }

            log.warn("** metaChecksum **");
            log.warn(cs.toString());
            if (acc) {
                log.warn("** accMetaChecksum **");
                log.warn(acs.toString());
            }
            log.warn("default charset: " + Charset.defaultCharset().displayName());
            ObservationWriter ow = new ObservationWriter();
            ow.write(obs, System.out);
            
        } catch (Exception oops) {
            log.error("failure during detailedChecksumDiagnostics", oops);
        }
    }

    private void compare(StringBuilder sb, URI u1, URI u2) {
        sb.append(u1);
        boolean eq = u1.equals(u2);
        if (eq) {
            sb.append(" == ");
        } else {
            sb.append(" != ");
        }
        sb.append(u2);
        if (!eq) {
            sb.append(" [MISMATCH]");
        }
    }
    
    private String computeTreeSize(Observation o) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int numA = 0;
        int numP = 0;
        int numC = 0;
        for (Plane p : o.getPlanes()) {
            numA += p.getArtifacts().size();
            for (Artifact a : p.getArtifacts()) {
                numP += a.getParts().size();
                for (Part pp : a.getParts()) {
                    numC += pp.getChunks().size();
                }
            }
        }
        sb.append("1,"); // obs
        sb.append(o.getPlanes().size()).append(",");
        sb.append(numA).append(",");
        sb.append(numP).append(",");
        sb.append(numC).append("]");
        return sb.toString();
    }

    private void detectLoop(List<SkippedWrapperURI<ObservationResponse>> entityList) {
        if (entityList.size() < 2) {
            return;
        }
        SkippedWrapperURI<ObservationResponse> start = entityList.get(0);
        SkippedWrapperURI<ObservationResponse> end = entityList.get(entityList.size() - 1);
        if (skipped) {
            if (start.skip.getLastModified().equals(end.skip.getLastModified())) {
                throw new RuntimeException("detected infinite harvesting loop: " + HarvestSkipURI.class.getSimpleName() + " at "
                        + format(start.skip.getLastModified()));
            }
            return;
        }
        Date d1 = null;
        Date d2 = null;
        if (start.entity.observation != null) {
            d1 = start.entity.observation.getMaxLastModified();
        } else if (start.entity.observationState != null) {
            d1 = start.entity.observationState.maxLastModified;
        }
        if (end.entity.observation != null) {
            d2 = end.entity.observation.getMaxLastModified();
        } else if (end.entity.observationState != null) {
            d2 = end.entity.observationState.maxLastModified;
        }
        if (d1 == null || d2 == null) {
            throw new RuntimeException("detectLoop: FAIL -- cannotr comapre timestamps " + d1 + " vs " + d2);
        }

        if (d1.equals(d2)) {
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName() + " at "
                    + format(start.entity.observation.getMaxLastModified()));
        }
    }

    private List<SkippedWrapperURI<ObservationResponse>> wrap(List<ObservationResponse> obsList) {
        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(obsList.size());
        for (ObservationResponse wr : obsList) {
            ret.add(new SkippedWrapperURI<ObservationResponse>(wr, null));
        }
        return ret;
    }

    private List<SkippedWrapperURI<ObservationResponse>> getSkipped(Date start) throws ExecutionException, InterruptedException {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkipDAO.get(source, cname, start, null, batchSize);

        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(skip.size());

        if (srcObservationDAO != null) {
            for (HarvestSkipURI hs : skip) {
                log.debug("getSkipped: " + hs.getSkipID());
                ObservationURI ouri = new ObservationURI(hs.getSkipID());
                ObservationResponse wr = srcObservationDAO.getObservationResponse(ouri);
                log.debug("response: " + wr);
                ret.add(new SkippedWrapperURI<ObservationResponse>(wr, hs));
            }
        } else {
            // srcObservationService
            List<ObservationURI> listUris = new ArrayList<>();
            for (HarvestSkipURI hs : skip) {
                log.debug("getSkipped: " + hs.getSkipID());
                listUris.add(new ObservationURI(hs.getSkipID()));
            }
            List<ObservationResponse> listResponses = srcRepoClient.get(listUris);
            log.warn("getSkipped: " + skip.size() + " HarvestSkipURI -> " + listResponses.size() + " ObservationResponse");

            for (ObservationResponse o : listResponses) {
                HarvestSkipURI hs = findSkip(o.observationState.getURI().getURI(), skip);
                o.observationState.maxLastModified = hs.getTryAfter(); // overwrite bogus value from RepoClient
                ret.add(new SkippedWrapperURI<>(o, hs));
            }
        }
        // re-order so we process in tryAfter order
        Collections.sort(ret, new SkipWrapperComparator());
        return ret;
    }

    private HarvestSkipURI findSkip(URI uri, List<HarvestSkipURI> skip) {
        for (HarvestSkipURI hs : skip) {
            if (hs.getSkipID().equals(uri)) {
                return hs;
            }
        }
        return null;
    }

    private static class SkipWrapperComparator implements Comparator<SkippedWrapperURI> {
        @Override
        public int compare(SkippedWrapperURI o1, SkippedWrapperURI o2) {
            return o1.skip.getTryAfter().compareTo(o2.skip.getTryAfter());
        }
    }

    /*
     * private List<SkippedWrapperURI<ObservationState>> getSkippedState(Date
     * start) { log.info("harvest window (skip): " + format(start) + " [" +
     * batchSize + "]" + " source = " + source + " cname = " + cname);
     * List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);
     * 
     * List<SkippedWrapperURI<ObservationState>> ret = new
     * ArrayList<SkippedWrapperURI<ObservationState>>(skip.size()); for
     * (HarvestSkipURI hs : skip) { ObservationState o = null;
     * log.debug("getSkipped: " + hs.getSkipID()); log.debug("start: " + start);
     * 
     * ObservationResponse wr = srcObservationService.get(src.getCollection(),
     * hs.getSkipID(), start);
     * 
     * if (wr != null && wr.getObservationState() != null) o =
     * wr.getObservationState();
     * 
     * if (o != null) { ret.add(new SkippedWrapperURI<ObservationState>(o, hs));
     * } } return ret; }
     */
    @Override
    protected void initHarvestState(DataSource ds, Class c) {
        super.initHarvestState(ds, c);
        this.harvestSkipDAO = new HarvestSkipURIDAO(ds, dest.getDatabase(), dest.getSchema());
    }

}
