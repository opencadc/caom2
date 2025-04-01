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
************************************************************************
*/

package org.opencadc.caom2.db.version;

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
public class InitDatabaseTest {
    private static final Logger log = Logger.getLogger(InitDatabaseTest.class);

    private static final String SCHEMA = "caom2";
    private static final DataSource DUMMY_DS = new DummyDataSource();
    
    static {
        Log4jInit.setLevel("org.opencadc.caom2", Level.INFO);
    }
    
    public InitDatabaseTest() { 
    }
    
    @Test
    public void testParseCreateDDL() {
        try {
            InitDatabase init = new InitDatabase(DUMMY_DS, null, SCHEMA);
            for (String fname : InitDatabase.CREATE_SQL) {
                log.info("process file: " + fname);
                List<String> statements = init.parseDDL(fname, SCHEMA);
                Assert.assertNotNull(statements);
                Assert.assertFalse(statements.isEmpty());
                for (String s : statements) {
                    String[] tokens = s.split(" ");
                    String cmd = tokens[0];
                    String type = tokens[1];
                    String next = tokens[2];
                    if ("create".equalsIgnoreCase(cmd)) {
                        if ("table".equalsIgnoreCase(type) || "view".equalsIgnoreCase(type) 
                                || "index".equalsIgnoreCase(type)
                                || ("unique".equalsIgnoreCase(type) && "index".equalsIgnoreCase(next))) {
                            // OK
                        } else {
                            Assert.fail("[create] unexpected type: " + s);
                        }
                    } else if ("drop".equalsIgnoreCase(cmd)) {
                        if ("view".equalsIgnoreCase(type)) {
                            // OK
                        } else {
                            Assert.fail("[drop] dangerous drop: " + s);
                        }
                    } else if ("cluster".equalsIgnoreCase(cmd)) {
                        // OK
                    } else if ("grant".equalsIgnoreCase(cmd)) {
                        if ("select".equalsIgnoreCase(type) || "usage".equalsIgnoreCase(type)) {
                            // OK
                        } else {
                            Assert.fail("[grant] unexpected type: " + s);
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
            InitDatabase init = new InitDatabase(DUMMY_DS, null, SCHEMA);
            for (String fname : InitDatabase.UPGRADE_SQL) {
                log.info("process file: " + fname);
                List<String> statements = init.parseDDL(fname, SCHEMA);
                Assert.assertNotNull(statements);
                Assert.assertFalse(statements.isEmpty());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private static class DummyDataSource implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection getConnection(String string, String string1) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLogWriter(PrintWriter writer) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLoginTimeout(int i) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T unwrap(Class<T> type) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> type) throws SQLException {
            throw new UnsupportedOperationException();
        }
    };
}
