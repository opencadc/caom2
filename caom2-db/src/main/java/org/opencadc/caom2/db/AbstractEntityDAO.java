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
************************************************************************
*/

package org.opencadc.caom2.db;

import org.opencadc.caom2.util.CaomUtil;
import java.security.MessageDigest;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.persist.Entity;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Abstract entity DAO.
 * 
 * @author pdowler
 * @param <T>
 */
public abstract class AbstractEntityDAO<T extends Entity> extends AbstractDAO {
    private static final Logger log = Logger.getLogger(AbstractEntityDAO.class);

    protected AbstractEntityDAO(boolean origin) {
        super(origin);
    }
    
    protected AbstractEntityDAO(AbstractDAO dao) {
        super(dao);
    }

    protected T get(Class clz, UUID id, boolean lockForUpdate) {
        if (clz == null || id == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        checkInit();
        log.debug("GET: " + id + "," + lockForUpdate);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityGet<T> get = (EntityGet<T>) gen.getEntityGet(clz, lockForUpdate);
            get.setID(id);
            return get.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + id + "," + lockForUpdate + " " + dt + "ms");
        }
        throw new RuntimeException("BUG: handleInternalFail did not throw");
    }

    public void put(T val) {
        put(val, false, false);
    }
    
    protected void put(T val, boolean extendedUpdate, boolean timestampUpdate) {
        put(val, extendedUpdate, timestampUpdate, false);
    }

    protected void put(T val, boolean extendedUpdate, boolean timestampUpdate, boolean tsUpdateOnInsertOnly) {
        if (val == null) {
            throw new IllegalArgumentException("entity cannot be null");
        }
        checkInit();
        log.debug("PUT: " + val.getID() + " force=" + extendedUpdate);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            String sql = gen.getSelectSQL(val.getClass(), val.getID());
            Entity cur = null;
            // TODO
            
            Date now = getCurrentTime(jdbc);
            boolean update = cur != null;
            boolean delta = updateEntity(val, cur, now, timestampUpdate, tsUpdateOnInsertOnly);
            if (delta || extendedUpdate) {
                EntityPut put = gen.getEntityPut(val.getClass(), update);
                put.setValue(val, null);
                put.execute(jdbc);
            } else {
                log.debug("no change: " + cur);
            }
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + val.getID() + " " + dt + "ms");
        }
    }
    
    protected void delete(Class entityClass, UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        checkInit();
        log.debug("DELETE: " + id);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            EntityDelete del = gen.getEntityDelete(entityClass, true);
            del.setID(id);
            del.execute(jdbc);
        } catch (BadSqlGrammarException ex) {
            handleInternalFail(ex);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " + id + " " + dt + "ms");
        }
    }

    // assign metaChecksum and update lastModified
    private boolean updateEntity(T entity, Entity cur, Date now, boolean timestampUpdate, boolean tsUpdateOnInsertOnly) {
        log.debug("updateEntity: " + entity);
        MessageDigest digest = getDigest();
        
        CaomUtil.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        
        boolean delta = false;
        if (cur == null) {
            delta = true;
        } else {
            // metadata change
            delta = !entity.getMetaChecksum().equals(cur.getMetaChecksum());
        }
        
        if (cur != null && entity.getLastModified() == null) {
            // preserve current timestamp: this happens when duplicate Deleted*Event are created
            // and that should be idempotent
            CaomUtil.assignLastModified(entity, cur.getLastModified(), "lastModified");
        }
        
        boolean forceUpateLastModified = (entity.getLastModified() == null); // new
        forceUpateLastModified = forceUpateLastModified || (origin && delta);
        forceUpateLastModified = forceUpateLastModified || timestampUpdate;
        forceUpateLastModified = forceUpateLastModified || (tsUpdateOnInsertOnly && cur == null);
        
        if (forceUpateLastModified) {
            CaomUtil.assignLastModified(entity, now, "lastModified");
        }
        
        if (cur != null && !cur.getLastModified().equals(entity.getLastModified())) {
            // timestamp update
            delta = true;
        }

        return delta;
    }
}
