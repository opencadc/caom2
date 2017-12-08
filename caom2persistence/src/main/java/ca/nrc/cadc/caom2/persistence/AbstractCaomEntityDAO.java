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
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import ca.nrc.cadc.caom2.util.MaxLastModifiedComparator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
abstract class AbstractCaomEntityDAO<T extends CaomEntity> extends AbstractDAO {

    private static final Logger log = Logger.getLogger(AbstractCaomEntityDAO.class);
    protected boolean origin = true;

    protected MessageDigest digest;

    protected AbstractCaomEntityDAO() {
        try {
            this.digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("FATAL: no MD5 digest algorithm available", ex);
        }
    }

    // constructor for utility classes that share the same settings instead of being
    // configured
    protected AbstractCaomEntityDAO(SQLGenerator gen, boolean forceUpdate, boolean readOnly) {
        this();
        this.gen = gen;
        this.forceUpdate = forceUpdate;
        this.readOnly = readOnly;
    }

    /**
     * Set origin flag. When true, the persistence classes behave as origin metadata
     * repositories: they are responsible for assigning entity IDs and modification
     * timestamps when entities are put. When false (not an origin) the provided
     * IDs and timestamps are used directly.
     * 
     * @param origin 
     */
    public void setOrigin(boolean origin) {
        this.origin = origin;
    }

    public T get(UUID id) {
        throw new UnsupportedOperationException();
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
    public List<T> getList(Class<T> c, Date minLastModified, Date maxLastModified, Integer batchSize) {
        if (Observation.class.isAssignableFrom(c)) {
            return getList(c, minLastModified, maxLastModified, batchSize, SQLGenerator.MAX_DEPTH);
        }

        if (Artifact.class.isAssignableFrom(c)) {
            Class<? extends Artifact> rac = (Class<? extends Artifact>) c;
            return getArtifactList(minLastModified, maxLastModified, batchSize);
        }

        if (ReadAccess.class.isAssignableFrom(c)) {
            Class<? extends ReadAccess> rac = (Class<? extends ReadAccess>) c;
            return getReadAccessList(rac, minLastModified, maxLastModified, batchSize);
        }

        throw new UnsupportedOperationException("unexpected class for getList: " + c.getName());
    }

    /**
     * Get batch of Observations. This implementation finds a range of
     * lastModified that should yield the requested batchSize and then performs
     * a date-range query with the specified depth (if needed) to get the target
     * instances.
     *
     * @param c
     * @param minlastModified
     * @param batchSize
     * @param depth
     * @return
     */
    protected List<T> getList(Class<T> c, Date minlastModified, Date maxLastModified, Integer batchSize, int depth) {
        checkInit();

        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            // find the range of timestamps that gives batchSize entities
            Date endDate = maxLastModified;
            String sql = gen.getSelectLastModifiedRangeSQL(c, minlastModified, maxLastModified, batchSize);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            Object o = jdbc.query(sql, gen.getTimestampRowMapper());
            if (o instanceof List) {
                List mlm = (List) o;
                if (mlm.isEmpty()) {
                    // no observations > minLastModified
                    return new ArrayList<T>(0);
                }
                if (mlm.size() >= batchSize) {
                    endDate = (Date) mlm.get(mlm.size() - 1); // last == max value
                }
            }

            // now query for the specified range of dates
            sql = gen.getObservationSelectSQL(c, minlastModified, endDate, depth);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            Object result = jdbc.query(sql, gen.getObservationExtractor());

            if (result == null) {
                return new ArrayList<T>(0);
            }

            if (result instanceof List) {
                List obs = (List) result;
                List<T> ret = new ArrayList<T>(obs.size());
                ret.addAll(obs);
                // sort list by maxLastModified
                Collections.sort(ret, new MaxLastModifiedComparator());
                return ret;
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
    }

    protected List<T> getReadAccessList(Class<? extends ReadAccess> rac, Date minLastModified, Date maxLastModified, Integer batchSize) {
        checkInit();

        RowMapper rm = gen.getReadAccessMapper(rac);
        return getListImpl(rm, rac, minLastModified, maxLastModified, batchSize);
    }

    protected List<T> getArtifactList(Date minLastModified, Date maxLastModified, Integer batchSize) {
        checkInit();

        RowMapper rm = gen.getArtifactMapper();
        return getListImpl(rm, Artifact.class, minLastModified, maxLastModified, batchSize);
    }

    private List<T> getListImpl(RowMapper rm, Class c, Date minLastModified, Date maxLastModified, Integer batchSize) {
        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            String sql = gen.getSelectSQL(c, minLastModified, maxLastModified, batchSize);
            log.debug("GET SQL: " + sql);

            Object result = jdbc.query(sql, rm);
            if (result == null) {
                return new ArrayList<T>(0);
            }

            if (result instanceof List) {
                List res = (List) result;
                List<T> ret = new ArrayList<T>(res.size());
                ret.addAll(res);
                return ret;
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
    }

    protected void put(Skeleton cur, T val, LinkedList<CaomEntity> parents, JdbcTemplate jdbc) {
        put(cur, val, parents, jdbc, false);
    }

    protected void put(Skeleton cur, T val, LinkedList<CaomEntity> parents, JdbcTemplate jdbc, boolean force) {
        if (readOnly) {
            throw new UnsupportedOperationException("put in readOnly mode");
        }
        checkInit();

        // transition from stateCode to metaChecksum
        boolean delta = false;
        String cmp = " [new]";
        if (cur != null) {
            delta = !val.getMetaChecksum().equals(cur.metaChecksum);
            cmp = " " + cur.metaChecksum + " vs " + val.getMetaChecksum();

            // change in accMetaChecksum means maxLastModified changed
            // this correctly maintains accMetaChecksum and maxLastModified
            if (!delta && val.getAccMetaChecksum() != null) {
                delta = !val.getAccMetaChecksum().equals(cur.accMetaChecksum);
                cmp = cmp + " -- " + cur.accMetaChecksum + " vs " + val.getAccMetaChecksum();
            }
            log.debug("PUT: " + val.getClass().getSimpleName() + cmp);
        } else {
            log.debug("PUT: " + val.getClass().getSimpleName() + cmp);
        }

        boolean isUpdate = (cur != null);

        // insert || forceUpdate mode || caller force || state changed
        if (cur == null || forceUpdate || force || delta) {
            if (isUpdate) {
                log.debug("PUT update: " + val.getClass().getSimpleName() + " " + val.getID());
            } else {
                log.debug("PUT insert: " + val.getClass().getSimpleName() + " " + val.getID());
            }
            EntityPut<T> op = gen.getEntityPut(val.getClass(), isUpdate);
            op.setValue(val, parents);
            op.execute(jdbc);
        } else {
            log.debug("PUT skip: " + val.getClass().getSimpleName() + " " + val.getID());
        }
    }

    protected void delete(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        deleteChildren(ce, jdbc);
        deleteSelf(ce, jdbc);
    }

    protected void deleteSelf(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        checkInit();
        // delete by PK
        //String sql = gen.getDeleteSQL(ce.targetClass, ce.id, true);
        //log.debug("delete: " + sql);
        //jdbc.update(sql);
        EntityDelete op = gen.getEntityDelete(ce.targetClass, true);
        op.setID(ce.id);
        op.execute(jdbc);
    }

    protected void deleteChildren(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        log.debug("deleteChildren no-op: " + ce.targetClass.getSimpleName());
    }

    protected class Pair<T> {

        public Skeleton cur;
        public T val;

        Pair(Skeleton s, T v) {
            this.cur = s;
            this.val = v;
        }
    }
}
