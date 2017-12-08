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

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.CaomWCSValidator;
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.BadSqlGrammarException;

/**
 *
 * @author pdowler
 */
public class ObservationHarvester extends Harvester {

    private static Logger log = Logger.getLogger(ObservationHarvester.class);

    private boolean service = false;

    private RepoClient srcObservationService;
    private ObservationDAO srcObservationDAO;
    private ObservationDAO destObservationDAO;

    private boolean skipped;
    private Date maxDate;
    private boolean doCollisionCheck = false;
    private boolean computePlaneMetadata = false;
    private boolean nochecksum = false;

    HarvestSkipURIDAO harvestSkipDAO = null;

    public ObservationHarvester(HarvestResource src, HarvestResource dest, Integer batchSize,
            boolean full, boolean dryrun, boolean nochecksum, int nthreads)
            throws IOException, URISyntaxException {
        super(Observation.class, src, dest, batchSize, full, dryrun);
        this.nochecksum = nochecksum;
        init(nthreads);
    }

    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public void setDoCollisionCheck(boolean doCollisionCheck) {
        this.doCollisionCheck = doCollisionCheck;
    }

    public void setComputePlaneMetadata(boolean computePlaneMetadata) {
        this.computePlaneMetadata = computePlaneMetadata;
    }

    public boolean getComputePlaneMetadata() {
        return computePlaneMetadata;
    }

    private void init(int nthreads) throws IOException, URISyntaxException {
        if (src.getDatabaseServer() != null) {
            Map<String, Object> config1 = getConfigDAO(src);
            this.srcObservationDAO = new ObservationDAO();
            srcObservationDAO.setConfig(config1);
        } else {
            this.srcObservationService = new RepoClient(src.getResourceID(), nthreads);
        }

        // for now, dest is always a database
        Map<String, Object> config2 = getConfigDAO(dest);
        this.destObservationDAO = new ObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setOrigin(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    private String format(UUID id) {
        if (id == null) {
            return "null";
        }
        return Long.toString(id.getLeastSignificantBits());
    }

    @Override
    public void run() {
        log.info("START: " + Observation.class.getSimpleName());

        boolean go = true;
        while (go) {
            Progress num = doit();

            if (num.found > 0) {
                log.debug("***************** finished batch: " + num + " *******************");
            }

            if (num.abort) {
                log.error("batched aborted");
            }
            go = (num.found > 0 && !num.abort && !num.done);
            if (batchSize != null && num.found < batchSize.intValue() / 2) {
                go = false;
            }
            full = false; // do not start at beginning again
            if (dryrun) {
                go = false; // no state update -> infinite loop
            }
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
    private boolean firstIteration = true;

    private Progress doit() {
        Progress ret = new Progress();

        long t = System.currentTimeMillis();
        long timeState = -1;
        long timeQuery = -1;
        long timeTransaction = -1;

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null) {
            expectedNum = batchSize.intValue();
        }
        try {
            System.gc(); // hint
            t = System.currentTimeMillis();

            HarvestState state = null;

            if (!skipped) {
                state = harvestStateDAO.get(source, Observation.class.getSimpleName());
                log.debug("state " + state);
            }

            timeState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            if (full && firstIteration) {
                startDate = null;
            } else if (!skipped) {
                log.debug("recalculate startDate");
                startDate = state.curLastModified;
            }
            log.debug("startDate " + startDate);
            // else: skipped: keep startDate across multiple batches since we
            // don't persist harvest
            // state
            firstIteration = false;

            log.debug("skipped: " + (skipped));

            Date end = maxDate;
            List<SkippedWrapperURI<ObservationResponse>> entityList = null;
            //List<SkippedWrapperURI<ObservationState>> entityListState = null;

            if (skipped) {
                entityList = getSkipped(startDate);
                //entityListState = getSkippedState(startDate);
            } else {
                Date fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60000L);
                if (end == null) {
                    end = fiveMinAgo;
                } else {
                    log.debug("harvest limit: min( " + format(fiveMinAgo) + " " + format(end) + " )");
                    if (end.getTime() > fiveMinAgo.getTime()) {
                        end = fiveMinAgo;
                    }
                }

                log.info("harvest window: " + format(startDate) + " :: " + format(end) + " [" + batchSize + "]");
                List<ObservationResponse> obsList = null;
                //List<ObservationState> stateList = null;
                if (srcObservationDAO != null) {
                    obsList = srcObservationDAO.getList(src.getCollection(), startDate, end, batchSize + 1);
                } else {
                    obsList = srcObservationService.getList(src.getCollection(), startDate, end, batchSize + 1);
                }
                entityList = wrap(obsList);
                //entityListState = wrapState(stateList);
            }

            if (entityList.size() >= expectedNum) {
                try {
                    detectLoop(entityList);
                } catch (RuntimeException rex) {
                    if (!skipped) {
                        Integer tmpBatchSize = (int) (1.5 * batchSize);
                        log.info("(loop) temporary harvest window: " + format(startDate) + " :: " + format(end) + " [" + tmpBatchSize + "]");

                        List<ObservationResponse> obsList = null;
                        //List<ObservationState> stateList = null;
                        if (srcObservationDAO != null) {
                            obsList = srcObservationDAO.getList(src.getCollection(), startDate, end, tmpBatchSize);
                        } else {
                            obsList = srcObservationService.getList(src.getCollection(), startDate, end, tmpBatchSize);
                        }

                        entityList = wrap(obsList);
                        detectLoop(entityList);
                    } else {
                        throw rex;
                    }
                }
            }

            // avoid re-processing the last successful one stored in HarvestState
            if (!entityList.isEmpty() && !skipped) {
                ListIterator<SkippedWrapperURI<ObservationResponse>> iter = entityList.listIterator();
                Observation curBatchLeader = iter.next().entity.observation;
                if (curBatchLeader != null) {
                    log.debug("currentBatch: " + curBatchLeader.getURI() + " " + format(curBatchLeader.getMaxLastModified()));
                    log.debug("harvestState: " + format(state.curID) + " " + format(state.curLastModified));
                    if (curBatchLeader.getID().equals(state.curID)
                            && curBatchLeader.getMaxLastModified().equals(state.curLastModified)) {
                        //entityListState.remove(0);
                        iter.remove();
                        expectedNum--;
                    }

                }
            }

            ret.found = entityList.size();
            log.debug("found: " + entityList.size());
            //log.debug("found os: " + entityListState.size());

            timeQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            ListIterator<SkippedWrapperURI<ObservationResponse>> iter1 = entityList.listIterator();
            //int i = 0;
            while (iter1.hasNext()) {
                SkippedWrapperURI<ObservationResponse> ow = iter1.next();
                Observation o = null;
                if (ow.entity != null) {
                    o = ow.entity.observation;
                }
                HarvestSkipURI hs = ow.skip;
                iter1.remove(); // allow garbage collection during loop

                String lastMsg = null;
                String skipMsg = null;

                if (!dryrun) {
                    if (destObservationDAO.getTransactionManager().isOpen()) {
                        throw new RuntimeException("BUG: found open trasnaction at start of next observation");
                    }
                    log.debug("starting transaction");
                    destObservationDAO.getTransactionManager().startTransaction();
                }
                boolean ok = false;
                try {
                    // o could be null in skip mode cleanup
                    if (o != null) {
                        String treeSize = computeTreeSize(o);
                        log.info("put: " + o.getClass().getSimpleName() + " " + o.getURI() + " " + format(o.getMaxLastModified()) + " " + treeSize);
                    } else if (hs != null) {
                        log.info("error put: " + hs.cname + " " + hs.skipID + " " + format(hs.lastModified));

                    }
                    if (!dryrun) {
                        if (skipped) {
                            startDate = hs.lastModified;
                        }

                        if (o != null) {
                            if (state != null) {
                                state.curLastModified = o.getMaxLastModified();
                                state.curID = o.getID();
                            }

                            // try to avoid DataIntegrityViolationException
                            // due to missed deletion of an observation
                            if (srcObservationDAO != null) { // need uuid -> URI query in src
                                UUID curID = destObservationDAO.getID(o.getURI());
                                if (curID != null && !curID.equals(o.getID())) {
                                    ObservationURI oldSrc = srcObservationDAO.getURI(curID);
                                    if (oldSrc == null) {
                                        // missed harvesting a deletion
                                        log.info("delete: " + o.getClass().getSimpleName() + " " + format(curID) + " (ObservationURI conflict avoided)");
                                        destObservationDAO.delete(curID);
                                    }
                                    // else: the put below with throw a valid
                                    // exception because source
                                    // is not enforcing
                                    // unique ID and URI
                                }
                            }
                            
                            if (doCollisionCheck) {
                                Observation cc = destObservationDAO.getShallow(o.getID());
                                log.debug("collision check: " + o.getURI() + " " + format(o.getMaxLastModified()) + " vs " + format(cc.getMaxLastModified()));
                                if (!cc.getMaxLastModified().equals(o.getMaxLastModified())) {
                                    throw new IllegalStateException("detected harvesting collision: " + o.getURI()
                                            + " maxLastModified: " + format(o.getMaxLastModified()));
                                }
                            }

                            // advance the date on success or failure
                            if (skipped) {
                                startDate = hs.lastModified;
                            }

                            CaomValidator.validate(o);
                            
                            for (Plane p : o.getPlanes()) {
                                for (Artifact a : p.getArtifacts()) {
                                    CaomWCSValidator.validate(a);
                                }
                            }

                            if (computePlaneMetadata) {
                                log.debug("computePlaneMetadata: " + o.getObservationID());
                                for (Plane p : o.getPlanes()) {
                                    ComputeUtil.computeTransientState(o, p);
                                }
                            }

                            if (!nochecksum) {
                                validateChecksum(o);
                            }

                            // everything is OK
                            destObservationDAO.put(o);
                            
                            if (!skipped) {
                                 harvestStateDAO.put(state);
                            }
                            
                            if (hs == null) {
                                // normal harvest mode: try to cleanup skip records immediately
                                hs = harvestSkipDAO.get(source, cname, o.getURI().getURI());
                            }

                            if (hs != null) {
                                log.info("delete: " + hs + " " + format(hs.lastModified));
                                harvestSkipDAO.delete(hs);
                            } 
                        } else if (skipped && ow.entity == null) {
                            log.info("delete: " + hs + " " + format(hs.lastModified));
                            harvestSkipDAO.delete(hs);
                        } else if (ow.entity.error != null) {
                            // try to make progress on failures
                            if (state != null && ow.entity.observationState.maxLastModified != null) {
                                state.curLastModified = ow.entity.observationState.maxLastModified;
                                state.curID = null; //unknown
                            }
                            throw ow.entity.error;
                        }

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                } catch (Throwable oops) {
                    lastMsg = oops.getMessage();
                    String str = oops.toString();
                    if (oops instanceof IllegalStateException) {
                        if (oops.getMessage().contains("XML failed schema validation")) {
                            log.error("CONTENT PROBLEM - XML failed schema validation: " + oops.getMessage());
                            ret.handled++;
                        } else if (oops.getMessage().contains("failed to read")) {
                            log.error("CONTENT PROBLEM - " + oops.getMessage(), oops.getCause());
                            ret.handled++;
                        }
                    } else if (oops instanceof IllegalArgumentException) {
                        log.error("CONTENT PROBLEM - validation failure: " + ow.entity.observationState.getURI() 
                            + " - " + oops.getMessage());
                        ret.handled++;
                    } else if (oops instanceof ChecksumError) {
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
                            log.error("UNDETECTED illegal polygon: " + o.getURI());
                            ret.handled++;
                    } else {
                        log.error("unexpected exception", oops);
                    }
                } finally {
                    if (!ok && !dryrun) {
                        if (o != null) {
                            log.warn("failed to insert " + o + ": " + lastMsg);
                            skipMsg = o + ": " + lastMsg;
                        } else {
                            log.warn("failed to insert " + ow.entity.observationState.getURI().getURI() + ": " + lastMsg);
                            skipMsg = ow.entity.observationState.getURI().getURI() + ": " + lastMsg;
                        }
                        lastMsg = null;
                        destObservationDAO.getTransactionManager().rollbackTransaction();
                        log.warn("rollback: OK");
                        timeTransaction += System.currentTimeMillis() - t;

                        try {
                            log.debug("starting HarvestSkipURI transaction");
                            boolean putSkip = true;
                            HarvestSkipURI skip = null;
                            if (o != null) {
                                skip = harvestSkipDAO.get(source, cname, o.getURI().getURI());
                            } else {
                                skip = harvestSkipDAO.get(source, cname, ow.entity.observationState.getURI().getURI());
                            }
                            log.debug("skip == " + skip);

                            if (skip == null) {
                                if (o != null) {
                                    skip = new HarvestSkipURI(source, cname, o.getURI().getURI(), skipMsg);
                                } else {
                                    skip = new HarvestSkipURI(source, cname, ow.entity.observationState.getURI().getURI(), skipMsg);
                                }
                            } else {
                                log.debug("skipMsg == " + skipMsg);
                                log.debug("skip.errorMessage == " + skip.errorMessage);

                                if (skipMsg != null && !skipMsg.equals(skip.errorMessage)) {
                                    skip.errorMessage = skipMsg; // possible
                                    // update
                                } else {
                                    log.debug("no change in status: " + hs);
                                    putSkip = false; // avoid timestamp
                                    // update
                                }
                            }

                            destObservationDAO.getTransactionManager().startTransaction();

                            if (!skipped) {
                                // track the harvest state progress
                                harvestStateDAO.put(state);
                            }

                            // track the fail
                            if (putSkip) {
                                log.info("put: " + skip);
                                harvestSkipDAO.put(skip);
                            }

                            // delete previous version of observation (if any)
                            destObservationDAO.delete(ow.entity.observationState.getURI());

                            log.debug("committing HarvestSkipURI transaction");
                            destObservationDAO.getTransactionManager().commitTransaction();
                            log.debug("commit HarvestSkipURI: OK");
                        } catch (Throwable oops) {
                            log.warn("failed to insert HarvestSkipURI", oops);
                            destObservationDAO.getTransactionManager().rollbackTransaction();
                            log.warn("rollback HarvestSkipURI: OK");
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

    private void validateChecksum(Observation o) throws ChecksumError {
        if (o.getAccMetaChecksum() == null) {
            return; // no check
        }
        try {
            URI calculatedChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

            log.debug("validateChecksum: " + o.getURI() + " -- " + o.getAccMetaChecksum() + " vs " + calculatedChecksum);
            if (!calculatedChecksum.equals(o.getAccMetaChecksum())) {
                throw new ChecksumError(("checksum mismatch: " + o.getAccMetaChecksum() + " != " + calculatedChecksum));
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
            if (start.skip.lastModified.equals(end.skip.lastModified)) {
                throw new RuntimeException("detected infinite harvesting loop: " + HarvestSkipURI.class.getSimpleName()
                        + " at " + format(start.skip.lastModified));
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
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName()
                    + " at " + format(start.entity.observation.getMaxLastModified()));
        }
    }

    private List<SkippedWrapperURI<ObservationResponse>> wrap(List<ObservationResponse> obsList) {
        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(obsList.size());
        for (ObservationResponse wr : obsList) {
            ret.add(new SkippedWrapperURI<ObservationResponse>(wr, null));
        }
        return ret;
    }

    /*
     * private List<SkippedWrapperURI<ObservationState>>
     * wrapState(List<ObservationState> obsList)
     * {
     * List<SkippedWrapperURI<ObservationState>> ret = new
     * ArrayList<SkippedWrapperURI<ObservationState>>(obsList.size());
     * for (ObservationState o : obsList)
     * {
     * ret.add(new SkippedWrapperURI<ObservationState>(o, null));
     * }
     * return ret;
     * }
     */
    private List<SkippedWrapperURI<ObservationResponse>> getSkipped(Date start) {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkipDAO.get(source, cname, start, null);

        List<SkippedWrapperURI<ObservationResponse>> ret = new ArrayList<SkippedWrapperURI<ObservationResponse>>(skip.size());
        for (HarvestSkipURI hs : skip) {
            log.debug("getSkipped: " + hs.getSkipID());
            ObservationURI ouri = new ObservationURI(hs.getSkipID());
            ObservationResponse wr;
            if (srcObservationDAO != null) {
                wr = srcObservationDAO.getAlt(ouri);
            } else {
                wr = srcObservationService.get(ouri);
            }
            log.debug("response: " + wr);

            //if (wr != null)
            ret.add(new SkippedWrapperURI<ObservationResponse>(wr, hs));
        }
        return ret;
    }

    /*
     * private List<SkippedWrapperURI<ObservationState>> getSkippedState(Date
     * start)
     * {
     * log.info("harvest window (skip): " + format(start) + " [" + batchSize +
     * "]" + " source = " + source + " cname = " + cname);
     * List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);
     *
     * List<SkippedWrapperURI<ObservationState>> ret = new
     * ArrayList<SkippedWrapperURI<ObservationState>>(skip.size());
     * for (HarvestSkipURI hs : skip)
     * {
     * ObservationState o = null;
     * log.debug("getSkipped: " + hs.getSkipID());
     * log.debug("start: " + start);
     *
     * ObservationResponse wr = srcObservationService.get(src.getCollection(),
     * hs.getSkipID(), start);
     *
     * if (wr != null && wr.getObservationState() != null)
     * o = wr.getObservationState();
     *
     * if (o != null)
     * {
     * ret.add(new SkippedWrapperURI<ObservationState>(o, hs));
     * }
     * }
     * return ret;
     * }
     */
    @Override
    protected void initHarvestState(DataSource ds, Class c) {
        super.initHarvestState(ds, c);
        this.harvestSkipDAO = new HarvestSkipURIDAO(ds, dest.getDatabase(), dest.getSchema(), batchSize);
    }
}
