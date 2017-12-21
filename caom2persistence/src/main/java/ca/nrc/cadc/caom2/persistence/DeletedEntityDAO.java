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

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservation;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
public class DeletedEntityDAO extends AbstractDAO {

    private static final Logger log = Logger.getLogger(DeletedEntityDAO.class);

    /**
     * Constructor for stand-alone query support. Use of this constructor
     * creates an instance that can be used to to get a list of DeletedEntity
     * of the specified type.
     */
    public DeletedEntityDAO() {
        
    }
    
    // package access for use by ObservationDAO
    DeletedEntityDAO(SQLGenerator gen, boolean forceUpdate, boolean readOnly) {
        this.gen = gen;
        this.forceUpdate = forceUpdate;
        this.readOnly = readOnly;
    }

    /**
     * This is intended to be called from ObservationDAO.delete().
     * @param de
     * @param jdbc 
     */
    void put(DeletedEntity de, JdbcTemplate jdbc) {
        checkInit();
        if (de == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("PUT: " + de);
        long t = System.currentTimeMillis();

        try {
            if (de.getLastModified() == null) {
                // if not null, the entity was harvested so keep original timestamp
                Util.assignDeletedLastModified(de, new Date(), "lastModified");
            }
            DeletedEntityPut op = gen.getDeletedEntityPut(de.getClass(), false); // insert only
            op.setValue(de);
            op.execute(jdbc);
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + de + " " + dt + "ms");
        }
    }
    
    //package access for unit test
    void put(DeletedEntity de) {
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        put(de, jdbc);
    }
    
    DeletedEntity get(Class<? extends DeletedEntity> c, UUID id) {
        checkInit();
        
        log.debug("GET: " + id);
        long t = System.currentTimeMillis();

        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            String sql = gen.getSelectSQL(c, id);
            log.debug("GET SQL: " + sql);

            Object result = jdbc.query(sql, gen.getDeletedEntityMapper(c));
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
                            + id + " query returned " + obs.size() + " DeletedEntity(s)");
                }
                Object o = obs.get(0);
                if (o instanceof DeletedEntity) {
                    DeletedEntity ret = (DeletedEntity) obs.get(0);
                    return ret;
                } else {
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
                }
            }
            throw new RuntimeException("BUG: query returned an unexpected list type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + id + " " + dt + "ms");
        }
    }
    
    /** 
     * get list of deleted ReadAccess tuples of the specified type.
     * 
     * @param c
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return 
     */
    public List<DeletedEntity> getList(Class<? extends DeletedEntity> c, Date minLastModified, Date maxLastModified, Integer batchSize) {
        checkInit();

        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            String sql = gen.getSelectSQL(c, minLastModified, maxLastModified, batchSize);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            Object result = jdbc.query(sql, gen.getDeletedEntityMapper(c));
            if (result == null) {
                return new ArrayList<DeletedEntity>(0);
            }

            if (result instanceof List) {
                List obs = (List) result;
                List<DeletedEntity> ret = new ArrayList<DeletedEntity>(obs.size());
                ret.addAll(obs);
                return ret;
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
    }
    
    /**
     * Get list of deleted observations.
     * 
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return 
     */
    public List<DeletedObservation> getList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize) {
        checkInit();

        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();
        try {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);

            String sql = gen.getSelectSQL(DeletedObservation.class, minLastModified, maxLastModified, batchSize, true, collection);
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            Object result = jdbc.query(sql, gen.getDeletedEntityMapper(DeletedObservation.class));
            if (result == null) {
                return new ArrayList<DeletedObservation>(0);
            }

            if (result instanceof List) {
                List obs = (List) result;
                List<DeletedObservation> ret = new ArrayList<DeletedObservation>(obs.size());
                ret.addAll(obs);
                return ret;
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
    }
}
