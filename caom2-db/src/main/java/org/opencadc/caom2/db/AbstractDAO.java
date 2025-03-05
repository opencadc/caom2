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
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package org.opencadc.caom2.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.db.mappers.TimestampRowMapper;
import java.lang.reflect.Constructor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base DAO class with common setup.
 * 
 * @author pdowler
 */
public class AbstractDAO {

    private static final Logger log = Logger.getLogger(AbstractDAO.class);

    protected SQLGenerator gen;
    protected boolean forceUpdate;
    protected boolean readOnly;

    protected boolean origin;
    protected DataSource dataSource;
    protected TransactionManager txnManager;

    protected AbstractDAO(boolean origin) {
        this.origin = origin;
    }

    protected AbstractDAO(AbstractDAO dao) {
        this(dao.origin);
        this.gen = dao.getSQLGenerator();
        this.dataSource = dao.getDataSource();
    }

    // toggle from intTest code
    void setOrigin(boolean origin) {
        this.origin = origin;
    }

    // try to unwrap spring jdbc exception consistently and throw a RuntimeException with
    // a decent message
    protected void handleInternalFail(BadSqlGrammarException ex) throws RuntimeException {
        Throwable cause = ex.getCause();
        if (cause != null) {
            if (cause.getMessage().contains("permission")) {
                throw new RuntimeException("CONFIG: " + cause.getMessage(), cause);
            }
            throw new RuntimeException("BUG: " + cause.getMessage(), cause);
        }
        throw new RuntimeException("BUG: " + ex.getMessage(), ex);
    }

    protected MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("FATAL: no MD5 digest algorithm available", ex);
        }
    }

    /**
     * Get the DataSource in use by the DAO. This is intended so that
     * applications can include other SQL statements along with DAO operations
     * in a single transaction.
     *
     * @return the DataSource
     */
    public DataSource getDataSource() {
        checkInit();
        return dataSource;
    }

    SQLGenerator getSQLGenerator() {
        checkInit();
        return gen;
    }

    /**
     * Get the TransactionManager that controls transactions using this DAOs
     * DataSource.
     *
     * @return the TransactionManager
     */
    public TransactionManager getTransactionManager() {
        checkInit();
        if (txnManager == null) {
            this.txnManager = new DatabaseTransactionManager(dataSource);
        }
        return txnManager;
    }

    public void setConfig(Map<String, Object> config) {
        
        
        
        String jndiDataSourceName = (String) config.get("jndiDataSourceName");
        if (jndiDataSourceName == null) {
            throw new IllegalArgumentException("missing required config: jndiDataSourceName");
        }
        try {
            this.dataSource = DBUtil.findJNDIDataSource(jndiDataSourceName);
            log.warn("found: " + jndiDataSourceName);
        } catch (NamingException ex) {
            throw new IllegalArgumentException("cannot find JNDI DataSource: " + jndiDataSourceName);
        }

        String database = (String) config.get("database");
        String schema = (String) config.get("schema");
        
        // TODO: make this db-specific plugin configurable again
        
        //Class<?> genClass = (Class<?>) config.get(StatementUtil.class.getName());
        //if (genClass == null) {
        //    throw new IllegalArgumentException(StatementUtil.class.getName() + " must be specified in config");
        //}
        //try {
        //    StatementUtil su = (StatementUtil) genClass.newInstance();
        //} catch (Exception ex) {
        //    throw new RuntimeException("CONFIG: failed to instantiate " + genClass);
        //}
        
        this.gen = new SQLGenerator(database, schema);
    }

    protected void checkInit() {
        if (gen == null || dataSource == null) {
            throw new IllegalStateException("setConfig never called or failed");
        }
    }

    protected Date getCurrentTime(JdbcTemplate jdbc) {
        checkInit();
        String tsSQL = gen.getCurrentTimeSQL();

        // database server uses local time
        Date now = jdbc.queryForObject(tsSQL, new TimestampRowMapper(Calendar.getInstance(DateUtil.LOCAL)));
        return now;
    }
}
