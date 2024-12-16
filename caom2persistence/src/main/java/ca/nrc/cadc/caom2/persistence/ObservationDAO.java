/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.persistence.skel.ArtifactSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ChunkSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ObservationSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PartSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PlaneSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.PreconditionFailedException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Persistence layer operations.
 *
 * @author pdowler
 */
public class ObservationDAO extends AbstractCaomEntityDAO<Observation> {

    private static final Logger log = Logger.getLogger(ObservationDAO.class);

    private PlaneDAO planeDAO;
    private DeletedEntityDAO deletedDAO;

    public ObservationDAO() {
    }

    @Override
    public Map<String, Class> getParams() {
        Map<String, Class> ret = super.getParams();
        ret.put("schemaPrefixHack", Boolean.class);
        return ret;
    }

    @Override
    public void setConfig(Map<String, Object> config) {
        super.setConfig(config);
        this.planeDAO = new PlaneDAO(gen, forceUpdate, readOnly);
        this.deletedDAO = new DeletedEntityDAO(gen, forceUpdate, readOnly);
    }

    public ObservationState getState(UUID id) {
        return getStateImpl(null, id);
    }
    
    public ObservationState getState(ObservationURI uri) {
        return getStateImpl(uri, null);
    }
    
    private ObservationState getStateImpl(ObservationURI uri, UUID id) {
        checkInit();
        if (uri == null && id == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        log.debug("GET: " + uri + " | " + id);
        long t = System.currentTimeMillis();

        try {
            String sql = null;
            if (uri != null) {
                sql = gen.getSelectSQL(uri, 1, false);
            } else { 
                sql = gen.getSelectSQL(id, 1, false);
            }
            log.debug("GET: " + sql);
            
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            //ObservationSkeleton skel = (ObservationSkeleton) jdbc.query(sql, new ObservationSkeletonExtractor());
            Observation obs = (Observation) jdbc.query(sql, gen.getObservationExtractor());
            if (obs != null) {
                ObservationState ret = new ObservationState(obs.getURI());
                ret.id = obs.getID();
                ret.accMetaChecksum = obs.getAccMetaChecksum();
                ret.maxLastModified = obs.getMaxLastModified();
                
                //ret.id = skel.id;
                //ret.accMetaChecksum = skel.accMetaChecksum;
                //ret.maxLastModified = skel.maxLastModified;
                return ret;
            }
            return null;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + uri + " | " + id + " " + dt + "ms");
        }
    }
  
    /**
     * Get list of observation states in ascending order.
     *
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return
     */
    public List<ObservationState> getObservationList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize) {
        return getObservationList(collection, minLastModified, maxLastModified, batchSize, true);
    }

    /**
     * Get list of observation states in the specified timestamp order.
     *
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @param ascendingOrder
     * @return
     */
    public List<ObservationState> getObservationList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize, boolean ascendingOrder) {
        checkInit();
        log.debug("getObservationStates: " + collection + " " + batchSize);

        // input check since this is a string
        CaomValidator.assertValidPathComponent(ObservationDAO.class, "collection", collection);

        long t = System.currentTimeMillis();

        try {
            String sql = gen.getSelectSQL(ObservationState.class, minLastModified, maxLastModified, batchSize, ascendingOrder, collection);

            if (log.isDebugEnabled()) {
                log.debug("GET: " + Util.formatSQL(sql));
            }

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List result = jdbc.query(sql, gen.getObservationStateMapper());
            return result;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getObservationStates: " + collection + " " + batchSize + " " + dt + "ms");
        }
    }

    /**
     * Get a wrapped complete observation or error.
     * 
     * @param uri
     * @return 
     */
    public ObservationResponse getObservationResponse(ObservationURI uri) {
        ObservationState s = new ObservationState(uri);
        ObservationResponse ret = getObservationResponse(s, SQLGenerator.MAX_DEPTH);
        if (ret.observation != null) {
            s.id = ret.observation.getID();
            s.accMetaChecksum = ret.observation.getAccMetaChecksum();
            s.maxLastModified = ret.observation.getMaxLastModified();
        }
        return ret;
    }
    
    /**
     * Get a wrapped complete Observation or error.
     * 
     * @param s
     * @return wrapped observation
     */
    public ObservationResponse getObservationResponse(ObservationState s) {
        return getObservationResponse(s, SQLGenerator.MAX_DEPTH);
    }
    
    /**
     * Get a wrapped Observation to the specified depth or error.
     * @param s
     * @param depth
     * @return 
     */
    public ObservationResponse getObservationResponse(ObservationState s, int depth) {
        long t = System.currentTimeMillis();

        try {
            ObservationResponse ret = new ObservationResponse(s);
            try {
                ret.observation = get(s.getURI());
            } catch (Exception ex) {
                ret.error = new IllegalStateException(ex.getMessage());
            }
            return ret;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getObservationResponse: " + s.getURI() + " depth=" + depth + " " + dt + "ms");
        }
    }
    
    /**
     * Get a list of wrapped observations from the specified collection and optional timestamp range.
     * This method can load a large number of observations into memory. It is better to use getObservationList
     * and iterate through the state(s) to access observations via getObservationResponse(ObservationState) 
     * or get(UUID).
     * 
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return 
     */
    @Deprecated
    public List<ObservationResponse> getList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize) {
        return getList(collection, minLastModified, maxLastModified, batchSize, SQLGenerator.MAX_DEPTH);
    }
    
    /**
     * Get list of wrapped observations to non-standard depth. 
     * 
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @param depth
     * @return 
     */
    @Deprecated
    public List<ObservationResponse> getList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize, int depth) {
        long t = System.currentTimeMillis();

        try {
            List<ObservationState> states = getObservationList(collection, minLastModified, maxLastModified, batchSize);
            List<ObservationResponse> ret = new ArrayList<ObservationResponse>(states.size());

            for (ObservationState s : states) {
                ObservationResponse r = getObservationResponse(s, depth);
                ret.add(r);
            }
            return ret;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getList: " + collection + " " + batchSize + " " + dt + "ms");
        }
    }
    
    /**
     * Get a stored observation by URI.
     *
     * @param uri
     * @return the complete observation
     */
    Observation get(ObservationURI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }
        return get(uri, null, SQLGenerator.MAX_DEPTH);
    }

    @Override
    public Observation get(UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        return get(null, id, SQLGenerator.MAX_DEPTH);
    }

    private Observation get(ObservationURI uri, UUID id, int depth) {
        checkInit();
        if (uri == null && id == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        String idStr = (uri != null ? uri.toString() : id.toString());
        log.debug("GET: " + idStr);
        long t = System.currentTimeMillis();

        try {
            String sql;
            if (uri != null) {
                sql = gen.getSelectSQL(uri, depth, false);
            } else {
                sql = gen.getSelectSQL(id, depth, false);
            }

            if (log.isDebugEnabled()) {
                log.debug("GET: " + Util.formatSQL(sql));
            }

            try {
                log.debug("starting transaction");
                getTransactionManager().startTransaction();
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                // jcmt example with 4016 rows because 2 planes and 2008 artifacts/plane
                // fetch-size 10: :183265ms
                // fetch-size 100: 181775ms
                jdbc.setFetchSize(100);
                Observation ret = (Observation) jdbc.query(sql, gen.getObservationExtractor());
                getTransactionManager().commitTransaction();
                return ret;
            } catch (DataAccessResourceFailureException | OutOfMemoryError oops) {
                // result set exceeded available buffer size
                // log at debug since this is handled and plausible for a working system
                log.debug("failed to get observation", oops);
                Throwable cause = oops;
                if (oops.getCause() != null) {
                    cause = oops.getCause();
                }
                throw new IllegalStateException("failed to get observation: " + idStr
                    + " cause: " + cause.getMessage());
            }
        } catch (RuntimeException ex) {
            if (getTransactionManager().isOpen()) {
                try {
                    log.debug("failed to get " + idStr);
                    getTransactionManager().rollbackTransaction();
                    log.debug("rollback: OK");
                } catch (Exception ex2) {
                    log.debug("transaction was automatically rolled back by driver");
                }
            }
            throw ex;
        } finally {
            if (getTransactionManager().isOpen()) {
                try {
                    log.error("BUG - open transaction in finally");
                    getTransactionManager().rollbackTransaction();
                    log.error("BUG - rollback: OK");
                } catch (Exception ex2) {
                    log.error("BUG - transaction was automatically rolled back by driver");
                }
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + idStr + " " + dt + "ms");
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
            final ObservationSkeleton dirtyRead = getSkelImpl(obs.getID(), jdbc, false); // obs only
            final long dirtyReadTime = System.currentTimeMillis() - tt;
            
            log.debug("starting transaction");
            tt = System.currentTimeMillis();
            getTransactionManager().startTransaction();
            txnOpen = true;
            final long txnStartTime = System.currentTimeMillis() - tt;
            
            // obtain row lock on observation update
            ObservationSkeleton cur = null;
            long lockTime = 0L;
            long selectSkelTime = 0L;
            if (dirtyRead != null) {
                
                String lock = gen.getUpdateLockSQL(obs.getID());
                log.debug("LOCK SQL: " + lock);
                tt = System.currentTimeMillis();
                jdbc.update(lock);
                lockTime = System.currentTimeMillis() - tt;
                
                // req-acquire current state after obtaining lock
                tt = System.currentTimeMillis();
                //skelSQL = gen.getSelectSQL(obs.getID(), SQLGenerator.MAX_DEPTH, true);
                cur = getSkelImpl(obs.getID(), jdbc, true);
                selectSkelTime = System.currentTimeMillis() - tt;
            
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
            LinkedList<CaomEntity> parents = new LinkedList<CaomEntity>();
            parents.push(obs);
            for (Pair<Plane> p : pairs) {
                planeDAO.put(p.cur, p.val, parents, jdbc);
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
                    + " lock=" + lockTime
                    + " select=" + selectSkelTime
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

    private ObservationSkeleton getSkelImpl(UUID id, JdbcTemplate jdbc, boolean complete) {
        return getSkelNav(id, jdbc, complete);
    }

    private ObservationSkeleton getSkelJoin(UUID id, JdbcTemplate jdbc) {
        String skelSQL = gen.getSelectSQL(id, SQLGenerator.MAX_DEPTH, true);
        log.debug("getSkel: " + skelSQL);
        ObservationSkeleton ret = (ObservationSkeleton) jdbc.query(skelSQL, new ObservationSkeletonExtractor());
        return ret;
    }
    
    private ObservationSkeleton getSkelNav(UUID id, JdbcTemplate jdbc, boolean complete) {
        ObservationSkeletonExtractor ose = new ObservationSkeletonExtractor();
        String skelSQL = gen.getSelectSQL(ObservationSkeleton.class, id, true); // by PK
        log.debug("getSkel: " + skelSQL);
        List<ObservationSkeleton> skels = jdbc.query(skelSQL, ose.observationMapper);
        if (skels == null || skels.isEmpty()) {
            return null;
        }
        ObservationSkeleton ret = skels.get(0);
        if (!complete) {
            return ret;
        }
        String planeSkelSQL = gen.getSelectSQL(PlaneSkeleton.class, id, false); // by FK
        log.debug("getSkel: " + planeSkelSQL);
        List<PlaneSkeleton> planes = jdbc.query(planeSkelSQL, ose.planeMapper);
        for (PlaneSkeleton ps : planes) {
            ret.planes.add(ps);
            String artifactSkelSQL = gen.getSelectSQL(ArtifactSkeleton.class, ps.id, false); // by FK
            log.debug("getSkel: " + artifactSkelSQL);
            List<ArtifactSkeleton> artifacts = jdbc.query(artifactSkelSQL, ose.artifactMapper);
            for (ArtifactSkeleton as : artifacts) {
                ps.artifacts.add(as);
                String partSkelSQL = gen.getSelectSQL(PartSkeleton.class, as.id, false); // by FK
                log.debug("getSkel: " + partSkelSQL);
                List<PartSkeleton> parts = jdbc.query(partSkelSQL, ose.partMapper);
                for (PartSkeleton pas : parts) {
                    as.parts.add(pas);
                    String chunkSkelSQL = gen.getSelectSQL(ChunkSkeleton.class, pas.id, false); // by FK
                    log.debug("getSkel: " + chunkSkelSQL);
                    List<ChunkSkeleton> chunks = jdbc.query(chunkSkelSQL, ose.chunkMapper);
                    pas.chunks.addAll(chunks);
                }
            }
        }
        return ret;
    }

    /**
     * Delete a stored observation by URI.
     *
     * @param uri
     */
    @Deprecated
    public void delete(ObservationURI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri arg cannot be null");
        }
        deleteImpl(null, uri);
    }

    public void delete(UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("id arg cannot be null");
        }
        deleteImpl(id, null);
    }

    private void deleteImpl(UUID id, ObservationURI uri) {
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
            String sql = null;
            if (id != null) {
                sql = gen.getSelectSQL(id, SQLGenerator.MAX_DEPTH, true);
            } else {
                sql = gen.getSelectSQL(uri, SQLGenerator.MAX_DEPTH, true);
            }
            log.debug("DELETE: " + sql);
            ObservationSkeleton dirtyRead;
            if (id != null) {
                dirtyRead = getSkelImpl(id, jdbc, false); // obs only
            } else {
                dirtyRead = (ObservationSkeleton) jdbc.query(sql, new ObservationSkeletonExtractor());
            }

            log.debug("starting transaction");
            getTransactionManager().startTransaction();
            txnOpen = true;

            // obtain row lock on observation update
            ObservationSkeleton skel = null;
            if (dirtyRead != null) {
                String lock = gen.getUpdateLockSQL(dirtyRead.id);
                log.debug("LOCK SQL: " + lock);
                jdbc.update(lock);
                
                // req-acquire current state after obtaining lock
                if (id != null) {
                    skel = getSkelImpl(id, jdbc, true);
                } else {
                    skel = (ObservationSkeleton) jdbc.query(sql, gen.getSkeletonExtractor(ObservationSkeleton.class));
                }
            }
            
            if (skel != null) {
                if (uri == null) {
                    uri = getState(id).getURI(); // null state not possible
                }
                if (id == null) {
                    id = skel.id;
                }
                
                DeletedObservation de = new DeletedObservation(id, uri);
                deletedDAO.put(de, jdbc);
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
            // delete children of planes
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

    // update CaomEntity state:
    // assign ID if skeleton is null (new insert)
    // always compute and assign: metaChecksum, accMetaChecksum
    // assign if metaChecksum changes: lastModified
    // assign if lastModified changed or a child's maxLastModified changes
    private void updateEntity(Observation entity, ObservationSkeleton s, Date now) {
        if (origin && s == null) {
            CaomUtil.assignID(entity, gen.generateID(entity.getID()));
        }

        if (origin && s != null) {
            // keep timestamps from database
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
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
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            Util.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            Util.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Plane entity, PlaneSkeleton s, Date now) {
        if (origin && s == null) {
            CaomUtil.assignID(entity, gen.generateID(entity.getID()));
        }

        if (origin && s != null) {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
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
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            Util.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            Util.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Artifact entity, ArtifactSkeleton s, Date now) {
        if (origin && s == null) {
            CaomUtil.assignID(entity, gen.generateID(entity.getID()));
        }

        if (origin && s != null) {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
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
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            Util.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            Util.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Part entity, PartSkeleton s, Date now) {
        if (origin && s == null) {
            CaomUtil.assignID(entity, gen.generateID(entity.getID()));
        }

        if (origin && s != null) {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
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
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            Util.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            Util.assignLastModified(entity, now, "maxLastModified");
        }
    }

    private void updateEntity(Chunk entity, ChunkSkeleton s, Date now) {
        if (origin && s == null) {
            CaomUtil.assignID(entity, gen.generateID(entity.getID()));
        }

        if (origin && s != null) {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        // new or changed
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");

        boolean delta = false;
        if (s == null || s.metaChecksum == null) {
            delta = true;
        } else {
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        }
        if (delta && (origin || entity.getLastModified() == null)) {
            Util.assignLastModified(entity, now, "lastModified");
        }

        boolean accDelta = false;
        if (s == null || s.accMetaChecksum == null) {
            accDelta = true;
        } else {
            accDelta = !entity.getAccMetaChecksum().equals(s.accMetaChecksum);
        }
        if (accDelta && (origin || entity.getMaxLastModified() == null)) {
            Util.assignLastModified(entity, now, "maxLastModified");
        }
    }
}
