/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.caom2.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.PreconditionFailedException;
import java.net.URI;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.db.mappers.ObservationSkeletonExtractor;
import org.opencadc.caom2.db.skel.ArtifactSkeleton;
import org.opencadc.caom2.db.skel.ChunkSkeleton;
import org.opencadc.caom2.db.skel.ObservationSkeleton;
import org.opencadc.caom2.db.skel.PartSkeleton;
import org.opencadc.caom2.db.skel.PlaneSkeleton;
import org.opencadc.caom2.db.skel.Skeleton;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.util.ObservationState;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Persistence layer operations.
 *
 * @author pdowler
 */
public class ObservationDAO extends AbstractCaomEntityDAO<Observation> {
    private static final Logger log = Logger.getLogger(ObservationDAO.class);

    private PlaneDAO planeDAO;

    public ObservationDAO(boolean origin) {
        super(origin);
    }

    @Override
    public void setConfig(Map<String, Object> config) {
        super.setConfig(config);
        this.planeDAO = new PlaneDAO(this);
    }

    public ObservationState lock(UUID id) {
        return getState(id, null, true);
    }

    // use case: repo get-by-uri
    public ObservationState getState(URI uri) {
        return getState(null, uri, false);
    }
    
    public ObservationState getState(UUID id) {
        return getState(id, null, false);
    }
    
    private ObservationState getState(UUID id, URI uri, boolean forUpdate) {
        checkInit();
        if (uri == null && id == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        String idStr = (uri != null ? uri.toString() : id.toString());
        log.debug("getState: " + idStr);
        long t = System.currentTimeMillis();

        try {
            SQLGenerator.ObservationStateGet get = new SQLGenerator.ObservationStateGet(gen);
            get.setIdentifier(id, uri);
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            ObservationState ret = get.execute(jdbc);
            return ret;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getState: " + idStr + " " + dt + "ms");
        }
    }

    // use case: repo sync API
    Observation get(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }
        ObservationState s = getState(uri);
        if (s == null) {
            return null;
        }
        return get(s.getID());
    }

    @Override
    public Observation get(UUID id) {
        checkInit();
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        log.debug("GET: " + id);
        long t = System.currentTimeMillis();

        boolean txnOpen = false;
        try {
            String sql = gen.getSelectSQL(id, SQLGenerator.MAX_DEPTH, false);
            if (log.isDebugEnabled()) {
                log.debug("GET: " + Util.formatSQL(sql));
            }

            try {
                log.debug("starting transaction");
                getTransactionManager().startTransaction();
                txnOpen = true;
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                // jcmt example with 4016 rows because 2 planes and 2008 artifacts/plane
                // fetch-size 10: :183265ms
                // fetch-size 100: 181775ms
                jdbc.setFetchSize(100);
                Observation ret = (Observation) jdbc.query(sql, gen.getObservationExtractor());
                getTransactionManager().commitTransaction();
                txnOpen = false;
                return ret;
            } catch (DataAccessResourceFailureException | OutOfMemoryError oops) {
                // result set exceeded available buffer size
                // log at debug since this is handled and plausible for a working system
                log.debug("failed to get observation", oops);
                Throwable cause = oops;
                if (oops.getCause() != null) {
                    cause = oops.getCause();
                }
                throw new IllegalStateException("failed to get observation: " + id
                    + " cause: " + cause.getMessage());
            }
        } catch (RuntimeException ex) {
            if (txnOpen) {
                try {
                    log.debug("failed to get " + id);
                    getTransactionManager().rollbackTransaction();
                    log.debug("rollback: OK");
                } catch (Exception ex2) {
                    log.debug("transaction was automatically rolled back by driver");
                }
            }
            txnOpen = false;
            throw ex;
        } finally {
            if (txnOpen) {
                try {
                    log.error("BUG - open transaction in finally");
                    getTransactionManager().rollbackTransaction();
                    log.error("BUG - rollback: OK");
                } catch (Exception ex2) {
                    log.error("BUG - transaction was automatically rolled back by driver");
                }
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + id + " " + dt + "ms");
        }
    }

    /**
     * Store an observation.
     *
     * @param obs
     */
    public void put(Observation obs) {
        put(obs, null);
    }
    
    /**
     * Store an observation. If the optional accMetaChecksum argument is not null and does not
     * match the accMetaChecksum of the currently observation, the update is rejected.
     * 
     * @param obs the observation
     * @param expectedMetaChecksum optional metadata checksum
     * @throws PreconditionFailedException if the accMetaChecksum does not match 
     */
    public void put(Observation obs, URI expectedMetaChecksum) throws PreconditionFailedException {
        if (readOnly) {
            throw new UnsupportedOperationException("put in readOnly mode");
        }
        checkInit();
        if (obs == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("PUT: " + obs.getURI() + ", planes: " + obs.getPlanes().size());
        long t = System.currentTimeMillis();

        boolean txnOpen = false;
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            
            // get current timestamp from server so that lastModified is closer to
            // monatonically increasing than if we use client machine clock
            Date now = getCurrentTime(jdbc);
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            log.debug("current time: " + df.format(now));

            // NOTE: this is by ID which means to update the caller must get(uri) then put(o)
            //       and if they do not get(uri) they can get a duplicate observation error
            //       if they violate unique keys
            //String skelSQL = gen.getSelectSQL(obs.getID(), 1, true);
            //log.debug("PUT: " + skelSQL);
            
            long tt = System.currentTimeMillis();
            final ObservationSkeleton dirtyRead = getSkelImpl(obs.getID(), jdbc, false, false); // obs only
            final long dirtyReadTime = System.currentTimeMillis() - tt;
            
            log.debug("starting transaction");
            tt = System.currentTimeMillis();
            getTransactionManager().startTransaction();
            txnOpen = true;
            final long txnStartTime = System.currentTimeMillis() - tt;
            
            // obtain row lock on observation update
            ObservationSkeleton cur = null;
            long lockSkelTime = 0L;
            if (dirtyRead != null) {
                // reacquire with lock
                cur = getSkelImpl(obs.getID(), jdbc, true, true);
                lockSkelTime = System.currentTimeMillis() - tt;
            
                // check conditional update
                if (expectedMetaChecksum != null) {
                    if (cur == null) {
                        // deleted by another actor since dirtyRead
                        throw new PreconditionFailedException("update blocked: current entity does not exist");
                    } else if (!expectedMetaChecksum.equals(cur.accMetaChecksum)) {
                        throw new PreconditionFailedException("update blocked: current entity is : " + cur.accMetaChecksum);
                    }
                }
            } else if (expectedMetaChecksum != null) {
                throw new PreconditionFailedException("update blocked: current entity does not exist");
            }
            
            // update metadata checksums, maybe modified timestamps
            updateEntity(obs, cur, now);

            // delete obsolete children
            tt = System.currentTimeMillis();
            List<Pair<Plane>> pairs = new ArrayList<Pair<Plane>>();
            if (cur != null) {
                // delete the skeletons that are not in obs.getPlanes()
                for (PlaneSkeleton ps : cur.planes) {
                    Plane p = Util.findPlane(obs.getPlanes(), ps.id);
                    if (p == null) {
                        log.debug("PUT: caused delete: " + ps.id);
                        planeDAO.delete(ps, jdbc);
                    }
                }
                // pair up planes and skeletons for insert/update
                for (Plane p : obs.getPlanes()) {
                    PlaneSkeleton ps = Util.findPlaneSkel(cur.planes, p.getID());
                    pairs.add(new Pair<Plane>(ps, p)); // null ok
                }
            } else {
                for (Plane p : obs.getPlanes()) {
                    pairs.add(new Pair<Plane>(null, p));
                }
            }
            final long deletePlanesTime = System.currentTimeMillis() - tt;
            
            tt = System.currentTimeMillis();
            super.put(cur, obs, null, jdbc);
            final long putObservationTime = System.currentTimeMillis() - tt;
            
            // insert/update children
            tt = System.currentTimeMillis();
            for (Pair<Plane> p : pairs) {
                planeDAO.put(p.cur, p.val, obs, jdbc);
            }
            final long putPlanesTime = System.currentTimeMillis() - tt;
            
            log.debug("committing transaction");
            tt = System.currentTimeMillis();
            getTransactionManager().commitTransaction();
            log.debug("commit: OK");
            final long txnCommitTime = System.currentTimeMillis() - tt;
            txnOpen = false;
            
            log.debug("transaction start=" + txnStartTime
                    + " dirtyRead=" + dirtyReadTime
                    + " select=" + lockSkelTime
                    + " del-planes=" + deletePlanesTime
                    + " put-obs=" + putObservationTime
                    + " put-planes=" + putPlanesTime
                    + " commit=" + txnCommitTime);
        } catch (DataIntegrityViolationException e) {
            log.debug("failed to insert " + obs + ": ", e);
            try {
                getTransactionManager().rollbackTransaction();
                log.debug("rollback: OK");
            } catch (Exception tex) {
                log.error("failed to rollback", tex);
            }
            txnOpen = false;
            throw e;
        } catch (PreconditionFailedException ex) {
            log.debug("failed to update " + obs + ": ", ex);
            try {
                getTransactionManager().rollbackTransaction();
                log.debug("rollback: OK");
            } catch (Exception tex) {
                log.error("failed to rollback", tex);
            }
            txnOpen = false;
            throw ex;
        } catch (Exception e) {
            log.error("failed to insert " + obs + ": ", e);
            try {
                getTransactionManager().rollbackTransaction();
                log.debug("rollback: OK");
            } catch (Exception tex) {
                log.error("failed to rollback", tex);
            }
            txnOpen = false;
            throw e;
        } finally {
            if (txnOpen) {
                log.error("BUG - open transaction in finally");
                try {
                    getTransactionManager().rollbackTransaction();
                    log.debug("rollback: OK");
                } catch (Exception tex) {
                    log.error("BADNESS: failed to rollback in finally", tex);
                }
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + obs.getURI() + " " + dt + "ms");
        }
    }

    private ObservationSkeleton getSkelImpl(UUID id, JdbcTemplate jdbc, boolean complete, boolean lock) {
        return getSkelNav(id, jdbc, complete, lock);
    }

    //private ObservationSkeleton getSkelJoin(UUID id, JdbcTemplate jdbc) {
    //    String skelSQL = gen.getSelectSQL(id, SQLGenerator.MAX_DEPTH, true);
    //    log.debug("getSkel: " + skelSQL);
    //    ObservationSkeleton ret = (ObservationSkeleton) jdbc.query(skelSQL, new ObservationSkeletonExtractor());
    //    return ret;
    //}
    
    private ObservationSkeleton getSkelNav(UUID id, JdbcTemplate jdbc, boolean complete, boolean lock) {
        ObservationSkeletonExtractor ose = new ObservationSkeletonExtractor();
        String skelSQL = gen.getSelectSQL(ObservationSkeleton.class, id, true, lock); // by PK
        log.debug("getSkel: " + skelSQL);
        List<ObservationSkeleton> skels = jdbc.query(skelSQL, ose.observationMapper);
        if (skels == null || skels.isEmpty()) {
            return null;
        }
        ObservationSkeleton ret = skels.get(0);
        if (!complete) {
            return ret;
        }
        String planeSkelSQL = gen.getSelectSQL(PlaneSkeleton.class, id, false, false); // by FK
        log.debug("getSkel: " + planeSkelSQL);
        List<PlaneSkeleton> planes = jdbc.query(planeSkelSQL, ose.planeMapper);
        for (PlaneSkeleton ps : planes) {
            ret.planes.add(ps);
            String artifactSkelSQL = gen.getSelectSQL(ArtifactSkeleton.class, ps.id, false, false); // by FK
            log.debug("getSkel: " + artifactSkelSQL);
            List<ArtifactSkeleton> artifacts = jdbc.query(artifactSkelSQL, ose.artifactMapper);
            for (ArtifactSkeleton as : artifacts) {
                ps.artifacts.add(as);
                String partSkelSQL = gen.getSelectSQL(PartSkeleton.class, as.id, false, false); // by FK
                log.debug("getSkel: " + partSkelSQL);
                List<PartSkeleton> parts = jdbc.query(partSkelSQL, ose.partMapper);
                for (PartSkeleton pas : parts) {
                    as.parts.add(pas);
                    String chunkSkelSQL = gen.getSelectSQL(ChunkSkeleton.class, pas.id, false, false); // by FK
                    log.debug("getSkel: " + chunkSkelSQL);
                    List<ChunkSkeleton> chunks = jdbc.query(chunkSkelSQL, ose.chunkMapper);
                    pas.chunks.addAll(chunks);
                }
            }
        }
        return ret;
    }

    public void delete(UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("id arg cannot be null");
        }
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        checkInit();
        // null check in public methods above
        log.debug("DELETE: " + id);
        long t = System.currentTimeMillis();

        boolean txnOpen = false;
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            final ObservationSkeleton dirtyRead = getSkelImpl(id, jdbc, false, false);

            log.debug("starting transaction");
            getTransactionManager().startTransaction();
            txnOpen = true;

            // obtain row lock on observation update
            ObservationSkeleton skel = null;
            if (dirtyRead != null) {
                // req-acquire current state with lock
                skel = getSkelImpl(id, jdbc, true, true);
            }
            
            if (skel != null) {
                delete(skel, jdbc);
            } else {
                log.debug("DELETE: not found: " + id);
            }

            log.debug("committing transaction");
            getTransactionManager().commitTransaction();
            log.debug("commit: OK");
            txnOpen = false;
        } catch (DataAccessException e) {
            log.debug("failed to delete " + id + ": ", e);
            getTransactionManager().rollbackTransaction();
            log.debug("rollback: OK");
            txnOpen = false;
            throw e;
        } finally {
            if (txnOpen) {
                log.error("BUG - open transaction in finally");
                getTransactionManager().rollbackTransaction();
                log.error("rollback: OK");
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " + id + " " + dt + "ms");
        }
    }

    @Override
    protected void deleteChildren(Skeleton s, JdbcTemplate jdbc) {
        ObservationSkeleton o = (ObservationSkeleton) s;
        if (o.planes.size() > 0) {
            // delete children
            for (PlaneSkeleton p : o.planes) {
                planeDAO.deleteChildren(p, jdbc);
                
                // delete planes by PK so we also clean up provenance join table
                EntityDelete op = gen.getEntityDelete(Plane.class, true);
                op.setID(p.id);
                op.execute(jdbc);
            }
        } else {
            log.debug("no children: " + o.id);
        }
    }

    // use case: backwards compatible repo list API
    public ResourceIterator<ObservationState> iterator(String collection, 
            Date minLastModified, Date maxLastModified, Integer batchSize) {
        checkInit();
        long t = System.currentTimeMillis();

        try {
            ObservationStateIteratorQuery iter = new ObservationStateIteratorQuery(gen, collection, null);
            iter.setMinLastModified(minLastModified);
            iter.setMaxLastModified(maxLastModified);
            iter.setBatchSize(batchSize);
            return iter.query(dataSource);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("iterator: " + dt + "ms");
        }
        throw new RuntimeException("BUG: should be unreachable");
    }

    // use case: caom2-meta-validate aka icewind validate mode
    public ResourceIterator<ObservationState> iterator(String namespace, String uriBucketPrefix) {
        checkInit();
        long t = System.currentTimeMillis();

        try {
            ObservationStateIteratorQuery iter = new ObservationStateIteratorQuery(gen, null, namespace);
            iter.setUriBucket(uriBucketPrefix);
            return iter.query(dataSource);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("iterator: " + dt + "ms");
        }
        throw new RuntimeException("BUG: should be unreachable");
    }

    // update CaomEntity state:
    // always compute and assign: metaChecksum, accMetaChecksum
    // assign if metaChecksum changes: lastModified
    // assign if lastModified changed or a child's maxLastModified changes
    private void updateEntity(Observation entity, ObservationSkeleton s, Date now) {
        if (origin && s != null) {
            // keep timestamps from database
            CaomUtil.assignLastModified(entity, s.lastModified, "lastModified");
            CaomUtil.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        // check for added or modified
        for (Plane plane : entity.getPlanes()) {
            PlaneSkeleton skel = null;
            if (s != null) {
                for (PlaneSkeleton ss : s.planes) {
                    if (plane.getID().equals(ss.id)) {
                        skel = ss;
                    }
                }
            }
            updateEntity(plane, skel, now);
        }

        // new or changed
        MessageDigest digest = getDigest();
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        CaomUtil.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Plane entity, PlaneSkeleton s, Date now) {
        if (origin && s != null) {
            CaomUtil.assignLastModified(entity, s.lastModified, "lastModified");
            CaomUtil.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        for (Artifact artifact : entity.getArtifacts()) {
            ArtifactSkeleton skel = null;
            if (s != null) {
                for (ArtifactSkeleton ss : s.artifacts) {
                    if (artifact.getID().equals(ss.id)) {
                        skel = ss;
                    }
                }
            }
            updateEntity(artifact, skel, now);
        }

        // new or changed
        MessageDigest digest = getDigest();
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        CaomUtil.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Artifact entity, ArtifactSkeleton s, Date now) {
        if (origin && s != null) {
            CaomUtil.assignLastModified(entity, s.lastModified, "lastModified");
            CaomUtil.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        for (Part part : entity.getParts()) {
            PartSkeleton skel = null;
            if (s != null) {
                for (PartSkeleton ss : s.parts) {
                    if (part.getID().equals(ss.id)) {
                        skel = ss;
                    }
                }
            }
            updateEntity(part, skel, now);
        }

        // new or changed
        MessageDigest digest = getDigest();
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        CaomUtil.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Part entity, PartSkeleton s, Date now) {
        if (origin && s != null) {
            CaomUtil.assignLastModified(entity, s.lastModified, "lastModified");
            CaomUtil.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        for (Chunk chunk : entity.getChunks()) {
            ChunkSkeleton skel = null;
            if (s != null) {
                for (ChunkSkeleton ss : s.chunks) {
                    if (chunk.getID().equals(ss.id)) {
                        skel = ss;
                    }
                }
            }
            updateEntity(chunk, skel, now);
        }

        if (origin) {
            // Chunk.compareTo uses the entity ID so rebuild Set
            List<Chunk> tmp = new ArrayList<Chunk>();
            tmp.addAll(entity.getChunks());
            entity.getChunks().clear();
            entity.getChunks().addAll(tmp);
        }

        // new or changed
        MessageDigest digest = getDigest();
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        CaomUtil.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Chunk entity, ChunkSkeleton s, Date now) {
        if (origin && s != null) {
            CaomUtil.assignLastModified(entity, s.lastModified, "lastModified");
            CaomUtil.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        // new or changed
        MessageDigest digest = getDigest();
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        CaomUtil.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            CaomUtil.assignLastModified(entity, now, "maxLastModified");
        }
    }
}
