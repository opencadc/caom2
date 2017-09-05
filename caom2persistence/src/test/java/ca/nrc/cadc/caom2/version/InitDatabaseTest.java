/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.version;


import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
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
public class InitDatabaseTest 
{
    private static final Logger log = Logger.getLogger(InitDatabaseTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.version", Level.INFO);
    }
    
    private DataSource dataSource;
    private String database;
    private String schema;
    
    public InitDatabaseTest() 
    {
        try
        {
            database = "cadctest";
            schema = "caom2";
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig("CAOM2_PG_TEST", database);
            dataSource = DBUtil.getDataSource(cc);
        }
        catch(Exception ex)
        {
            log.error("failed to init DataSource", ex);
        }
    }
    
    // NOTE: tests are currently commented out because the other Postgresql*Test(s)
    // all use InitDatabase.doInit and one of them will have done this anyway; this
    // test will have some value if/when the TODOs are implemented.
    
    @Test
    public void testNewInstall()
    {
        try
        {
            // TODO: nuke all tables and re-create
            // for now: create || upgrade || idempotent
            InitDatabase init = new InitDatabase(dataSource, database, schema);
            init.doInit();
            
            // TODO: verify that tables were created with test queries
            
            // TODO: verify that init is idempotent
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpgradeInstall()
    {
        try
        {
            // TODO: create previous version  of tables and upgrade... sounds complicated
            // for now: create || upgrade || idempotent
            InitDatabase init = new InitDatabase(dataSource, database, schema);
            init.doInit();
            
            // TODO: verify that tables were created with test queries
            
            // TODO: verify that init is idempotent
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testParseCreateDDL() 
    {
        try
        {
            int[] numStatementsPerFile = new int[]
            {
                1, 7, 8, 3, 4, 4, 2, 2, 2, 6, 8, 27, 1, 2, 1
            };
            Assert.assertEquals("BUG: testParseCreateDDL setup", numStatementsPerFile.length, InitDatabase.CREATE_SQL.length);
            
            for (int i = 0; i<numStatementsPerFile.length; i++)
            {
                String fname = InitDatabase.CREATE_SQL[i];
                log.info("process file: " + fname);
                List<String> statements = InitDatabase.parseDDL(fname);
                Assert.assertEquals(fname + " statements", numStatementsPerFile[i], statements.size());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testParseUpgradeDDL() 
    {
        try
        {
            int[] numStatementsPerFile = new int[]
            {
                15, 1, 2, 1
            };
            Assert.assertEquals("BUG: testParseUpgradeDDL setup", numStatementsPerFile.length, InitDatabase.UPGRADE_SQL.length);
            
            for (int i = 0; i<numStatementsPerFile.length; i++)
            {
                String fname = InitDatabase.UPGRADE_SQL[i];
                log.info("process file: " + fname);
                List<String> statements = InitDatabase.parseDDL(fname);
                Assert.assertEquals(fname + " statements", numStatementsPerFile[i], statements.size());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
