/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.access.ArtifactAccess;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class ReadAccessDAO extends AbstractCaomEntityDAO<ReadAccess> {

    private static final Logger log = Logger.getLogger(ReadAccessDAO.class);

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);
    
    public ReadAccessDAO() {
    }

    /** 
     * Create DAO to participate in transactions with another DAO.
     * @param copyConfig 
     */
    public ReadAccessDAO(ObservationDAO copyConfig) {
        this.origin = copyConfig.origin;
        this.dataSource = copyConfig.dataSource;
        this.txnManager = copyConfig.txnManager;
        this.gen = copyConfig.gen;
        this.forceUpdate = copyConfig.forceUpdate;
        this.readOnly = copyConfig.readOnly;
        try {
            this.digest = MessageDigest.getInstance(copyConfig.digest.getAlgorithm());
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("BUG: failed to copy MessageDigest config", ex);
        }
    }
    
    //public List<ArtifactAccess> getArtifactAccess(ObservationURI obsURI) {
    //    List<ArtifactAccess> ret = new ArrayList<>();
    // TODO: query for all artifact++ in the observation and populate AA list
    //    return ret;
    //}
    
    //public List<ArtifactAccess> getArtifactAccess(PublisherID pubID) {
    //    List<ArtifactAccess> ret = new ArrayList<>();
    // TODO: query for all artifact++ in the plane and populate AA list
    //    return ret;
    //}
    
    public RawArtifactAccess getArtifactAccess(URI artifactURI) {
        checkInit();
        if (artifactURI == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("getArtifactAccess: " + artifactURI);
        long t = System.currentTimeMillis();

        try {
            String pa = gen.getAlias(Plane.class);
            String aa = gen.getAlias(Artifact.class);
            
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(aa).append(".uri").append(",");
            sb.append(aa).append(".releaseType").append(",");
            sb.append(pa).append(".metaRelease").append(",");
            sb.append(pa).append(".dataRelease").append(",");
            sb.append(pa).append(".metaReadAccessGroups").append(",");
            sb.append(pa).append(".dataReadAccessGroups");
            sb.append(" FROM ");
            sb.append(gen.getFrom(Plane.class, 2, false));
            sb.append(" WHERE ").append(aa).append(".uri = ?");
            String sql = sb.toString();
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object[] args = new Object[1];
            args[0] = artifactURI.toASCIIString();
            Object result = jdbc.query(sql, args, new ArtifactAccessMapper());
            if (result == null) {
                return null;
            }
            if (result instanceof List) {
                List obs = (List) result;
                if (obs.isEmpty()) {
                    return null;
                }
                if (obs.size() > 1) {
                    throw new RuntimeException("BUG: get ArtifactAccess " + artifactURI + " query returned " + obs.size() + " rows");
                }
                Object o = obs.get(0);
                if (o instanceof RawArtifactAccess) {
                    RawArtifactAccess ret = (RawArtifactAccess) o;
                    return ret;
                } else {
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
                }
            }
            throw new RuntimeException("BUG: query returned an unexpected list type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getArtifactAccess: " + artifactURI + " " + dt + "ms");
        }
    }
    
    public static class RawArtifactAccess {
        public URI uri;
        public ReleaseType releaseType;
        public Date metaRelease;
        public Date dataRelease;
        public final List<String> metaReadAccessGroups = new ArrayList<>();
        public final List<String> dataReadAccessGroups = new ArrayList<>();
    }
    
    private class ArtifactAccessMapper implements RowMapper {
        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            final RawArtifactAccess ret = new RawArtifactAccess();
            ret.uri = Util.getURI(rs, 1);
            
            String rts = rs.getString(2);
            ret.releaseType = ReleaseType.toValue(rts);
            ret.metaRelease = Util.getDate(rs, 3, utcCalendar);
            ret.dataRelease = Util.getDate(rs, 4, utcCalendar);
            
            String mgs = rs.getString(5);
            String dgs = rs.getString(6);
            
            if (StringUtil.hasText(mgs)) {
                String[] ss = mgs.replaceAll("'", "").split(" ");
                for (String gname : ss) {
                    ret.metaReadAccessGroups.add(gname);
                }
            }
            log.warn("raw: " + mgs + " -> " + ret.metaReadAccessGroups.size());
            
            if (StringUtil.hasText(dgs)) {
                String[] ss = dgs.replaceAll("'", "").split(" ");
                for (String gname : ss) {
                    ret.dataReadAccessGroups.add(gname);
                }
            }
            log.warn("raw: " + dgs + " -> " + ret.dataReadAccessGroups.size());
            
            return ret;
        }
        
    }
    
    // need to expose this for caom2ac which has to cleanup tuples for caom2 
    // assets that became public
    public String getTable(Class c) {
        return gen.getTable(c);
    }

    /**
     * Get up to batchSize objects sorted by lastModified. This implementation
     * performs a direct query and returns the specified number of instances.
     *
     * @param c
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return
     */
    public List<ReadAccess> getList(Class<ReadAccess> c, Date minLastModified, Date maxLastModified, Integer batchSize) {
        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            String sql = gen.getSelectSQL(c, minLastModified, maxLastModified, batchSize);
            log.debug("GET SQL: " + sql);

            Object result = jdbc.query(sql, gen.getReadAccessMapper(c));
            if (result == null) {
                return new ArrayList<ReadAccess>(0);
            }

            if (result instanceof List) {
                List res = (List) result;
                List<ReadAccess> ret = new ArrayList<ReadAccess>(res.size());
                ret.addAll(res);
                return ret;
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
    }
    
    public ReadAccess get(Class<? extends ReadAccess> c, UUID assetID, URI groupID) {
        checkInit();
        if (c == null || assetID == null || groupID == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        log.debug("GET: " + c.getSimpleName() + " " + assetID + "," + groupID);
        long t = System.currentTimeMillis();

        try {
            String sql = gen.getSelectSQL(c, assetID, groupID);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object result = jdbc.query(sql, gen.getReadAccessMapper(c));
            if (result == null) {
                return null;
            }
            if (result instanceof List) {
                List obs = (List) result;
                if (obs.isEmpty()) {
                    return null;
                }
                if (obs.size() > 1) {
                    throw new RuntimeException("BUG: get " + c.getSimpleName() + " "
                            + assetID + "," + groupID + " query returned " + obs.size() + " ReadAccess tuples");
                }
                Object o = obs.get(0);
                if (o instanceof ReadAccess) {
                    ReadAccess ret = (ReadAccess) obs.get(0);
                    return ret;
                } else {
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
                }
            }
            throw new RuntimeException("BUG: query returned an unexpected list type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + c.getSimpleName() + " " + assetID + "," + groupID + " " + dt + "ms");
        }
    }

    public ReadAccess get(Class<? extends ReadAccess> c, UUID id) {
        checkInit();
        if (c == null || id == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        log.debug("GET: " + c.getSimpleName() + " " + id);
        long t = System.currentTimeMillis();

        try {
            String sql = gen.getSelectSQL(c, id);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object result = jdbc.query(sql, gen.getReadAccessMapper(c));
            if (result == null) {
                return null;
            }
            if (result instanceof List) {
                List obs = (List) result;
                if (obs.isEmpty()) {
                    return null;
                }
                if (obs.size() > 1) {
                    throw new RuntimeException("BUG: get " + c.getSimpleName() + " " + id + " query returned " + obs.size() + " ReadAccess tuples");
                }
                Object o = obs.get(0);
                if (o instanceof ReadAccess) {
                    ReadAccess ret = (ReadAccess) obs.get(0);
                    return ret;
                } else {
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
                }
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + c.getSimpleName() + " " + id + " " + dt + "ms");
        }
    }

    public void put(ReadAccess ra)
            throws DuplicateEntityException {
        checkInit();
        if (ra == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("PUT: " + ra);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Class skel = gen.getSkeletonClass(ra.getClass());
            String sql = gen.getSelectSQL(skel, ra.getID());
            log.debug("PUT: " + sql);
            Skeleton cur = (Skeleton) jdbc.query(sql, gen.getSkeletonExtractor(skel));

            updateEntity(ra, cur);

            super.put(cur, ra, null, jdbc);
        } catch (DataIntegrityViolationException ex) {
            if (ex.toString().contains("duplicate key")) {
                throw new DuplicateEntityException(ra.toString(), ex);
            }
            throw ex;
        } catch (TransientDataAccessResourceException ex) {
            // found this with jTDS driver
            if (ex.toString().contains("duplicate key")) {
                throw new DuplicateEntityException(ra.toString(), ex);
            }
            throw ex;
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + ra + " " + dt + "ms");
        }
    }

    public void delete(Class<? extends ReadAccess> c, UUID id) {
        checkInit();
        if (c == null || id == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        log.debug("DELETE: " + c.getSimpleName() + " " + id);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            // get current tuple
            ReadAccess cur = get(c, id);
            if (cur != null) {
                EntityDelete op = gen.getEntityDelete(c, true);
                op.setID(id);
                op.setValue(cur);
                op.execute(jdbc);
            }
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " + c.getSimpleName() + " " + id + " " + dt + "ms");
        }
    }

    private void updateEntity(ReadAccess ra, Skeleton s) {
        if (origin && s == null) {
            CaomUtil.assignID(ra, UUID.randomUUID());
        }
        
        digest.reset();
        Util.assignMetaChecksum(ra, ra.computeMetaChecksum(digest), "metaChecksum");

        if (!origin) {
            return;
        }

        int nsc = ra.getStateCode();
        boolean delta = false;
        if (s == null) {
            delta = true;
        } else if (s.metaChecksum != null) {
            delta = !ra.getMetaChecksum().equals(s.metaChecksum);
        } else {
            delta = (s.stateCode != nsc); // fallback
        }
        if (delta) {
            Util.assignLastModified(ra, new Date(), "lastModified");
        }
    }
}
