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

import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class AbstractDAO {

    private static final Logger log = Logger.getLogger(AbstractDAO.class);

    protected SQLGenerator gen;
    protected boolean forceUpdate;
    protected boolean readOnly;

    protected DataSource dataSource;
    protected TransactionManager txnManager;

    protected AbstractDAO() {
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

    public Map<String, Class> getParams() {
        Map<String, Class> ret = new TreeMap<String, Class>();
        ret.put("jndiDataSourceName", String.class);
        ret.put("server", String.class); // fallback if no jndiDataSourceName
        ret.put("database", String.class);
        ret.put("schema", String.class);
        ret.put("forceUpdate", Boolean.class);
        ret.put("disableHashJoin", Boolean.class);
        ret.put(SQLGenerator.class.getName(), Class.class);
        return ret;
    }

    public void setConfig(Map<String, Object> config) {
        String jndiDataSourceName = (String) config.get("jndiDataSourceName");
        String server = (String) config.get("server");
        String database = (String) config.get("database");

        String schema = (String) config.get("schema");

        Class<?> genClass = (Class<?>) config.get(SQLGenerator.class.getName());
        if (genClass == null) {
            throw new IllegalArgumentException(SQLGenerator.class.getName() + " must be specified in config");
        }
        try {
            if (jndiDataSourceName != null) {
                this.dataSource = new DataSourceWrapper(database, DBUtil.findJNDIDataSource(jndiDataSourceName));
            } else {
                DBConfig dbrc = new DBConfig();
                ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
                // for some reason, we need to suppress close when wrapping with delegating DS
                DataSourceWrapper dsw = new DataSourceWrapper(database, DBUtil.getDataSource(cc, true, true));
                Boolean disableHashJoin = (Boolean) config.get("disableHashJoin");
                if (disableHashJoin != null) {
                    log.debug("disableHashJoin: " + disableHashJoin);
                    dsw.setDisableHashJoin(disableHashJoin);
                }
                this.dataSource = dsw;
            }
        } catch (NamingException ex) {
            throw new IllegalArgumentException("cannot find JNDI DataSource: "
                    + jndiDataSourceName);
        } catch (IOException ex) {
            throw new IllegalArgumentException("cannot find ConnectionConfig for "
                    + server + " " + database);
        }

        Boolean force = (Boolean) config.get("forceUpdate");
        if (force != null) {
            this.forceUpdate = force.booleanValue();
        }

        try {
            Constructor<?> ctor = genClass.getConstructor(String.class, String.class);
            this.gen = (SQLGenerator) ctor.newInstance(database, schema);
        } catch (Exception ex) {
            throw new RuntimeException("failed to instantiate SQLGenerator: " + genClass.getName(), ex);
        }
    }

    protected void checkInit() {
        if (gen == null) {
            throw new IllegalStateException("setConfig never called or failed");
        }
    }

}
