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


import ca.nrc.cadc.caom2.persistence.UtilTest;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ModelVersionDAOTest
{
    private static final Logger log = Logger.getLogger(ModelVersionDAOTest.class);

    static String schema = "caom2";

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.version", Level.INFO);

        String testSchema = UtilTest.getTestSchema();
        if (testSchema != null)
        {
            schema = testSchema;
        }
    }

    private static final String MODEL = "FOO";
    private static final String V1 = "1.0";
    private static final String V2 = "2.0";

    private DataSource dataSource;
    private String database;


    public ModelVersionDAOTest()
    {
        try
        {
            database = "cadctest";
            //schema = System.getProperty("user.name");

            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig("CAOM2_PG_TEST", database);
            dataSource = DBUtil.getDataSource(cc);

        }
        catch(Exception ex)
        {
            log.error("failed to init DataSource", ex);
        }
    }

    @Test
    public void testRountrip()
    {
        try
        {
            ModelVersionDAO dao = new ModelVersionDAO(dataSource, database, schema);

            // get && cleanup if necessary
            ModelVersion mv = dao.get(MODEL);
            if (mv != null)
            {
                String sql = "delete from " + schema + ".ModelVersion where model = '"+MODEL+"'";
                log.info("cleanup: " + sql);
                dataSource.getConnection().createStatement().execute(sql);
            }

            // get null
            mv = dao.get(MODEL);
            Assert.assertNotNull(mv);
            Assert.assertNull(mv.version);   // new
            Assert.assertNull(mv.lastModified); // new

            // insert
            mv.version = V1;
            dao.put(mv);

            ModelVersion inserted = dao.get(MODEL);
            Assert.assertNotNull(inserted);
            Assert.assertEquals(mv.getModel(), inserted.getModel());
            Assert.assertEquals(V1, inserted.version);
            Assert.assertNotNull(inserted.lastModified);

            Thread.sleep(50l);

            // update
            mv.version = V2;
            dao.put(mv);

            ModelVersion updated = dao.get(MODEL);
            Assert.assertNotNull(updated);
            Assert.assertEquals(mv.getModel(), updated.getModel());
            Assert.assertEquals(V2, updated.version);
            Assert.assertTrue(inserted.lastModified.getTime() < updated.lastModified.getTime());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

    }

}
