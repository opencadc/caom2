/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package ca.nrc.cadc.tap.impl;

import ca.nrc.cadc.util.Log4jInit;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class InitCaomTapSchemaContentTest {
    private static final Logger log = Logger.getLogger(InitCaomTapSchemaContentTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.INFO);
    }

    private String schema = "tap_schema";
    
    @Test
    public void testParseCreateDDL() {
        try {
            InitCaomTapSchemaContent init = new InitCaomTapSchemaContent(new TestDataSource(), null, schema, true);
            for (String fname : InitCaomTapSchemaContent.BASE_EXTRA_SQL) {
                log.info("process file: " + fname);
                List<String> statements = init.parseDDL(fname, schema);
                Assert.assertNotNull(statements);
                Assert.assertFalse(statements.isEmpty());
                for (String s : statements) {
                    String[] tokens = s.split(" ");
                    String cmd = tokens[0];
                    
                    if ("select".equalsIgnoreCase(cmd)) {
                        String table = "notFound";
                        for (int i = 1; i < tokens.length; i++) {
                            if ("from".equalsIgnoreCase(tokens[i])) {
                                table = tokens[i + 1];
                                break;
                            }
                        }
                        if (table != null && table.startsWith("tap_schema.")) {
                            // OK
                        } else {
                            Assert.fail("[select] accesssing table outside tap_schema: " + table);
                        }
                    } else if ("update".equalsIgnoreCase(cmd)) {
                        String table = tokens[1];
                        if (table != null && table.startsWith("tap_schema.")) {
                            // OK
                        } else {
                            Assert.fail("[update] accesssing table outside tap_schema: " + table);
                        }
                    } else if ("insert".equalsIgnoreCase(cmd) || "delete".equalsIgnoreCase(cmd)) {
                        // tokens[1] is into or from
                        String table = tokens[2];
                        if (table != null && table.startsWith("tap_schema.")) {
                            // OK
                        } else {
                            Assert.fail("[insert|delete] accesssing table outside tap_schema: " + table);
                        }
                    } else {
                        Assert.fail("unexpected command: " + cmd + " [" + s + "]");
                    }
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testParseUpgradeDDL() {
        try {
            InitCaomTapSchemaContent init = new InitCaomTapSchemaContent(new TestDataSource(), null, schema, true);
            for (String fname : InitCaomTapSchemaContent.BASE_EXTRA_SQL) {
                log.info("process file: " + fname);
                List<String> statements = init.parseDDL(fname, schema);
                Assert.assertNotNull(statements);
                Assert.assertFalse(statements.isEmpty());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private static class TestDataSource implements DataSource {
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return null;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return null;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {

        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {

        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return 0;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }
    }
}
