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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.torkeep;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import java.util.Map;
import java.util.TreeMap;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.caom2.db.PostgreSQLGenerator;
import org.opencadc.caom2.db.SQLGenerator;
import org.opencadc.caom2.db.version.InitDatabase;

public class TorkeepInitAction extends InitAction {

    public TorkeepInitAction() {
        super();
    }

    private static final Logger log = Logger.getLogger(TorkeepInitAction.class);

    // datasource name from the context.xml
    static final String JNDI_DATASOURCE = "jdbc/torkeep-caom2";
    static final String JNDI_CONFIG_KEY = "torkeep.TorkeepInitServlet.torkeep-runtime-config";
    static final String SCHEMA = "caom2";

    private TorkeepConfig torkeepConfig;

    @Override
    public void doInit() {
        initConfig();
        initDatabase();
        initJNDI();
    }

    static Map<String, Object> getDAOConfig() {
        Map<String,Object> ret = new TreeMap<>();
        ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
        ret.put("jndiDataSourceName", JNDI_DATASOURCE);
        ret.put("schema", SCHEMA);
        return ret;
    }

    private void initConfig() {
        log.info("initConfig: START");
        this.torkeepConfig = new TorkeepConfig();
        log.info("initConfig: OK");
    }

    private void initDatabase() {
        try {
            log.info("initDatabase: START");
            DataSource ds = DBUtil.findJNDIDataSource(JNDI_DATASOURCE);
            // in PG we do not need to specify database name as it is set in DataSource JDBC URL
            InitDatabase init = new InitDatabase(ds, null, SCHEMA);
            init.doInit();
            log.info("initDatabase: OK");
        } catch (NamingException ex) {
            throw new RuntimeException("CONFIG: failed to connect to database", ex);
        }
    }

    private void initJNDI() {
        log.info("initJNDI: START");
        final Context initContext;
        try {
            initContext = new InitialContext();
        } catch (NamingException e) {
            throw new IllegalStateException("failed to find JNDI InitialContext", e);
        }
        String jndiKey = JNDI_CONFIG_KEY;
        log.debug("jndiKey: " + jndiKey);
        try {
            log.debug("unbinding possible existing " + jndiKey);
            initContext.unbind(jndiKey);
        } catch (NamingException e) {
            log.debug("no previously bound " + jndiKey + ", continuing");
        }
        try {
            initContext.bind(jndiKey, this.torkeepConfig);
            log.warn("doInit: collectionsConfig stored via JNDI: " + jndiKey);
        } catch (NamingException e) {
            throw new IllegalStateException("CONFIG: failed to bind collectionsConfig", e);
        }
        log.info("initJNDI: OK");
    }

}
