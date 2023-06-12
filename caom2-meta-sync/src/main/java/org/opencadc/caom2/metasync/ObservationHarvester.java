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

package org.opencadc.caom2.metasync;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ac.ReadAccessGenerator;
import ca.nrc.cadc.caom2.compute.CaomWCSValidator;
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
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
    private RepoClient srcObservationService;
    private ObservationDAO srcObservationDAO;
    private ObservationDAO destObservationDAO;
    private HarvestSkipURIDAO harvestSkipDAO;
    private boolean skipped;
    private boolean computePlaneMetadata;
    private ReadAccessGenerator acGenerator;
    private boolean ready = false;
    private int ingested = 0;

    public ObservationHarvester(HarvesterResource src, HarvesterResource dest, String collection, URI basePublisherID,
                                Integer batchSize, int nthreads, boolean full, boolean nochecksum) {
        super(Observation.class, src, dest, collection, batchSize, full);
        this.basePublisherID = basePublisherID;
        this.nochecksum = nochecksum;
        this.computePlaneMetadata = false;
        init(nthreads);
    }

    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    public boolean getComputePlaneMetadata() {
        return this.computePlaneMetadata;
    }
    
    public void setComputePlaneMetadata(boolean computePlaneMetadata) {
        this.computePlaneMetadata = computePlaneMetadata;
    }

    public void setGenerateReadAccessTuples(File config) {
        try {
            Properties props = new Properties();
            props.load(new FileReader(config));
            String line = props.getProperty(collection);
            if (line == null) {
                throw new IllegalArgumentException("CONFIG: collection not found: " + collection);
            }
            
            Map<String,Object> groupConfig = new HashMap<>();
            String[] parts = line.split("[ \t]+"); // one or more spaces and tabs
            for (int i = 0; i < parts.length; i++) {
                String option = parts[i]; // key=value pair
                String[] kv = option.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException("invalid key=value pair: " + option);
                }
                if (ReadAccessGenerator.PROPOSAL_GROUP_KEY.equals(kv[0])) {
                    boolean proposalGroup = "true".equals(kv[1]);
                    if (proposalGroup) {
                        groupConfig.put(ReadAccessGenerator.PROPOSAL_GROUP_KEY, proposalGroup);
                    }
                } else if (ReadAccessGenerator.OPERATOR_GROUP_KEY.equals(kv[0])) {
                    String og = kv[1];
                    if (og != null) {
                        URI ouri = new URI(og);
                        groupConfig.put(ReadAccessGenerator.OPERATOR_GROUP_KEY, ouri);
                    }
                } else if (ReadAccessGenerator.STAFF_GROUP_KEY.equals(kv[0])) {
                    String sg = kv[1];
                    if (sg != null) {
                        URI suri = new URI(sg);
                        groupConfig.put(ReadAccessGenerator.STAFF_GROUP_KEY, suri);
                    }
                }
            }
            for (Map.Entry<String,Object> me : groupConfig.entrySet()) {
                log.debug("generate config for " + collection + ": " + me.getKey() + " = " + me.getValue());
            }
            this.acGenerator = new ReadAccessGenerator(collection, groupConfig);
        } catch (IOException ex) {
            throw new RuntimeException("failed to read config from " + config, ex);
        } catch (Exception ex) {
            throw new RuntimeException("CONFIG: invalid config " + config, ex);
        } 
    }

    public int getIngested() {
        return this.ingested;
    }

    private void init(int nthreads) {
        if (src.getResourceType() == HarvesterResource.SOURCE_DB) {
            Map<String, Object> srcConfig = getConfigDAO(src);
            ConnectionConfig srcConnectionConfig = new ConnectionConfig(null, null,
                    src.getUsername(), src.getPassword(), HarvesterResource.POSTGRESQL_DRIVER, src.getJdbcUrl());
            final String srcDS = "jdbc/obsHarvestSrc";
            try {
                DBUtil.createJNDIDataSource(srcDS, srcConnectionConfig);
            } catch (NamingException e) {
                throw new IllegalStateException(String.format("Error creating source JNDI datasource for %s reason: %s",
                        src, e.getMessage()));
            }
            srcConfig.put("jndiDataSourceName", srcDS);
            this.srcObservationDAO = new ObservationDAO();
            srcObservationDAO.setConfig(srcConfig);
            ready = true;
        } else if (src.getResourceType() == HarvesterResource.SOURCE_URI) {
            this.srcObservationService = new RepoClient(src.getResourceID(), nthreads);
        } else {
            throw new IllegalStateException("BUG: unexpected HarvestResource resource type: " + src);
        }

        // for now, dest is always a database
        Map<String, Object> destConfig = getConfigDAO(dest);
        ConnectionConfig destConnectionConfig = new ConnectionConfig(null, null,
                dest.getUsername(), dest.getPassword(), HarvesterResource.POSTGRESQL_DRIVER, dest.getJdbcUrl());
        final String destDS = "jdbc/obsHarvestDest";
        try {
            DBUtil.createJNDIDataSource(destDS, destConnectionConfig);
        } catch (NamingException e) {
            throw new IllegalStateException(String.format("Error creating destination JNDI datasource for %s reason: %s",
                    dest, e.getMessage()));
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
        
        if (srcObservationService != null) {
            if (srcObservationService.isObsAvailable()) {
                ready = true;
            } else {
                log.error("Not available obs endpoint in " + srcObservationService.toString());
            }
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
        log.info("START: " + Observation.class.getSimpleName());

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

        log.info("DONE: " + entityClass.getSimpleName() + "\n");
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
                    obsList = srcObservationService.getList(collection, startDate, endDate, batchSize + 1);
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
            log.debug("found: " + entityList.size());

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
                log.debug("starting transaction");
                destObservationDAO.getTransactionManager().startTransaction();
                boolean ok = false;
                log.debug("skipped=" + skipped
                        + " o=" + o
                        + " ow.entity=" + ow.entity
                        + " ow.entity.error=" + (ow.entity != null || ow.entity.error != null));
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

                        // optionally augment the observation
                        if (computePlaneMetadata) {
                            log.debug("computePlaneMetadata: " + o.getURI());
                            for (Plane p : o.getPlanes()) {
                                ComputeUtil.computeTransientState(o, p);
                            }
                        }

                        if (acGenerator != null) {
                            log.debug("generateReadAccessTuples: " + o.getURI());
                            acGenerator.generateTuples(o);
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
                            log.info("delete: " + hs + " " + format(hs.getLastModified()));
                            harvestSkipDAO.delete(hs);
                        }
                    } else if (skipped) {
                        // o == null
                        if (srcObservationDAO != null || ow.entity.error instanceof ResourceNotFoundException) {
                            // observation not obtainable from source == missed deletion
                            ObservationURI uri = new ObservationURI(hs.getSkipID());
                            log.info("delete: " + uri);
                            destObservationDAO.delete(uri);
                            log.info("delete: " + hs + " " + format(hs.getLastModified()));
                            harvestSkipDAO.delete(hs);
                        } else {
                            // defer to the main catch for error handling
                            throw new HarvestReadException(ow.entity.error);
                        }
                    } else if (ow.entity.error != null) {
                        // o == null when harvesting from service: try to make progress on failures
                        if (state != null && ow.entity.observationState.maxLastModified != null) {
                            state.curLastModified = ow.entity.observationState.maxLastModified;
                            state.curID = null; // unknown
                        }
                        if (srcObservationDAO != null || ow.entity.error instanceof ResourceNotFoundException) {
                            ObservationURI uri = new ObservationURI(hs.getSkipID());
                            log.info("delete: " + uri);
                            destObservationDAO.delete(uri);
                            if (hs != null) {
                                log.info("delete: " + hs + " " + format(hs.getLastModified()));
                                harvestSkipDAO.delete(hs);
                            }
                        } else {
                            throw new HarvestReadException(ow.entity.error);
                        }
                    }

                    log.debug("committing transaction");
                    destObservationDAO.getTransactionManager().commitTransaction();
                    log.debug("commit: OK");

                    ok = true;
                    ret.ingested++;
                } catch (Throwable oops) {
                    log.debug("exception during harvest", oops);
                    skipMsg = null;
                    String str = oops.toString();
                    if (oops instanceof HarvestReadException) {
                        // unwrap HarvestReadException from above
                        oops = oops.getCause();
                        // unwrap intervening RuntimeException(s)
                        while (oops.getCause() != null && oops instanceof RuntimeException) {
                            oops = oops.getCause();
                        }
                        log.error("HARVEST PROBLEM - failed to read observation: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                        ret.handled++;
                    } else if (oops instanceof IllegalStateException) {
                        if (oops.getMessage().contains("XML failed schema validation")) {
                            log.error("CONTENT PROBLEM - XML failed schema validation: " + oops.getMessage());
                            ret.handled++;
                        } else if (oops.getMessage().contains("failed to read")) {
                            log.error("CONTENT PROBLEM - " + oops.getMessage(), oops.getCause());
                            ret.handled++;
                        }
                    } else if (oops instanceof IllegalArgumentException) {
                        log.error("CONTENT PROBLEM - invalid observation: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                        if (oops.getCause() != null) {
                            log.error("cause: " + oops.getCause());
                        }
                        ret.handled++;
                    } else if (oops instanceof MismatchedChecksumException) {
                        log.error("CONTENT PROBLEM - mismatching checksums: " + ow.entity.observationState.getURI());
                        ret.handled++;
                    } else if (str.contains("duplicate key value violates unique constraint \"i_observationuri\"")) {
                        log.error("CONTENT PROBLEM - duplicate observation: " + ow.entity.observationState.getURI());
                        ret.handled++;
                    } else if (oops instanceof TransientException) {
                        log.error("CONTENT PROBLEM - " + oops.getMessage());
                        ret.handled++;
                    } else if (oops instanceof Error) {
                        log.error("FATAL - probably installation or environment", oops);
                        ret.abort = true;
                    } else if (oops instanceof NullPointerException) {
                        log.error("BUG", oops);
                        ret.abort = true;
                    } else if (oops instanceof BadSqlGrammarException) {
                        log.error("BUG", oops);
                        BadSqlGrammarException bad = (BadSqlGrammarException) oops;
                        SQLException sex1 = bad.getSQLException();
                        if (sex1 != null) {
                            log.error("CAUSE", sex1);
                            SQLException sex2 = sex1.getNextException();
                            log.error("NEXT CAUSE", sex2);
                        }
                        ret.abort = true;
                    } else if (oops instanceof DataAccessResourceFailureException) {
                        log.error("SEVERE PROBLEM - probably out of space in database", oops);
                        ret.abort = true;
                    } else if (str.contains("spherepoly_from_array")) {
                        log.error("CONTENT PROBLEM - failed to persist: " + ow.entity.observationState.getURI() + " - " + oops.getMessage());
                        oops = new IllegalArgumentException("invalid polygon (spoly): " + oops.getMessage(), oops);
                        ret.handled++;
                    } else {
                        log.error("unexpected exception", oops);
                    }
                    // message for HarvestSkipURI record
                    skipMsg = oops.getMessage();
                } finally {
                    if (!ok) {
                        destObservationDAO.getTransactionManager().rollbackTransaction();
                        log.debug("rollback: OK");
                        timeTransaction += System.currentTimeMillis() - t;

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
                            destObservationDAO.getTransactionManager().rollbackTransaction();
                            log.debug("rollback HarvestSkipURI: OK");
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
        } finally {
            timeTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + timeState + "ms");
            log.debug("time to run ObservationListQuery: " + timeQuery + "ms");
            log.debug("time to run transactions: " + timeTransaction + "ms");
        }
        return ret;
    }

    private static class HarvestReadException extends Exception {

        public HarvestReadException(Exception cause) {
            super(cause);
        }
    }

    private void validateChecksum(Observation o) throws MismatchedChecksumException {
        if (o.getAccMetaChecksum() == null) {
            return; // no check
        }
        try {
            URI calculatedChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("validateChecksum: " + o.getURI() + " -- " + o.getAccMetaChecksum() + " vs " + calculatedChecksum);
            if (!calculatedChecksum.equals(o.getAccMetaChecksum())) {
                throw new MismatchedChecksumException("Observation.accMetaChecksum mismatch");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 digest algorithm not available");
        }
    }

    private String computeTreeSize(Observation o) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int numA = 0;
        int numP = 0;
        for (Plane p : o.getPlanes()) {
            numA += p.getArtifacts().size();
            for (Artifact a : p.getArtifacts()) {
                numP += a.getParts().size();
            }
        }
        sb.append(o.getPlanes().size()).append(",");
        sb.append(numA).append(",");
        sb.append(numP).append("]");
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
            List<ObservationResponse> listResponses = srcObservationService.get(listUris);
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
