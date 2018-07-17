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
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class SQLGeneratorTest
{
    private static final Logger log = Logger.getLogger(SQLGeneratorTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    String[] tables =
    {
        "cadctest.caom2.Observation",
        "cadctest.caom2.Plane",
        "cadctest.caom2.Artifact",
        "cadctest.caom2.Part",
        "cadctest.caom2.Chunk",

        "cadctest.caom2.DeletedObservation"
    };
    String[] pk = { "obsID", "planeID", "artifactID", "partID", "chunkID", 
        "readAccessID", "readAccessID", "readAccessID",
        "id", "id", "id", "id" };
    String[] fk = { null, "obsID", "planeID", "artifactID", "partID", 
        null, null, null,
        null, null, null };
    Class[] clz =
    {
        Observation.class,
        Plane.class,
        Artifact.class,
        Part.class,
        Chunk.class,

        DeletedObservation.class,
    };

    SQLGenerator gen = new DummyBaseSQLGenerator();
    private class DummyBaseSQLGenerator extends SQLGenerator
    {

        public DummyBaseSQLGenerator()
        {
            super("cadctest", "caom2");
            super.init();
        }

        @Override
        protected String literal(UUID value)
        {
            return value.toString(); // syntax doesn't matter / not checked
        }
        
        
        
    }

    //@Test
    public void testTemplate()
    {
        try
        {

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSelectObservationSQL()
    {
        try
        {
            
            ObservationURI uri = new ObservationURI("FOO", "NoSuchObservation");
            for (int i=1; i<=5; i++)
            {
                String sql = gen.getSelectSQL(uri, i);
                Assert.assertNotNull(sql);
                log.debug("SQL [" + sql.length() + "] " + sql);
                
                for (int t=0; t<i; t++)
                    Assert.assertTrue(tables[t], sql.contains(tables[t]));
                for (int t=i; t<5; t++)
                    Assert.assertFalse(tables[t], sql.contains(tables[t]));
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSelectArtifactSQL()
    {
        try
        {
            
            URI uri = new URI("cadc:artifactid");
            String sql = gen.getSelectArtifactSQL();
            Assert.assertNotNull(sql);
            log.debug("SQL [" + sql.length() + "] " + sql);
            Assert.assertFalse("SQL INJECTION", sql.contains(uri.toString()));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSelectReadAccessSQL()
    {
        try
        {
            UUID id = new UUID(0L, 666L);
            for (int i=5; i<=7; i++)
            {
                String sql = gen.getSelectSQL(clz[i], id);
                Assert.assertNotNull(sql);
                log.debug("SQL [" + sql.length() + "] " + sql);
                Assert.assertTrue(tables[i], sql.contains(tables[i]));
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSelectDeletedSQL()
    {
        try
        {
            UUID id = new UUID(0L, 666L);
            for (int i=7; i<=10; i++)
            {
                String sql = gen.getSelectSQL(clz[i], id);
                Assert.assertNotNull(sql);
                log.debug("SQL [" + sql.length() + "] " + sql);
                Assert.assertTrue(tables[i], sql.contains(tables[i]));
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testDeleteByPK()
    {
        
        try
        {
            UUID id = new UUID(0L, 666L);
            for (int i=0; i<clz.length; i++)
            {
                String sql = gen.getDeleteSQL(clz[i], id, true);
                Assert.assertNotNull(sql);
                log.debug("SQL [" + sql.length() + "] " + sql);
                Assert.assertTrue("contains PK column", sql.contains(pk[i]));
                for (int t=0; t<clz.length; t++)
                {
                    String tab = tables[t] + " ";
                    if (i == t) // for delete by PK
                        Assert.assertTrue("contains: " + tab, sql.contains(tab));
                    else
                        Assert.assertFalse("does not contain: " + tab, sql.contains(tab));
                }

            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testDeleteByFK()
    {

        try
        {
            UUID id = new UUID(0L, 666L);
            try
            {
                String sql = gen.getDeleteSQL(Observation.class, id, false);
                Assert.fail("expected IllegalArgumentException, got: " + sql);
            }
            catch(IllegalArgumentException expected)
            {
                log.debug("caught expected: " + expected);
            }
            for (int i=5; i<clz.length; i++)
                try
                {
                    String sql = gen.getDeleteSQL(clz[i], id, false);
                    Assert.fail("expected IllegalArgumentException, got: " + sql);
                }
                catch(IllegalArgumentException expected)
                {
                    log.debug("caught expected: " + expected);
                }
            for (int i=1; i<5; i++)
            {
                String sql = gen.getDeleteSQL(clz[i], id, false);
                Assert.assertNotNull(sql);
                log.debug("SQL [" + sql.length() + "] " + sql);
                Assert.assertTrue("contains FK column", sql.contains(fk[i]));
                for (int t=0; t<clz.length; t++)
                {
                    String tab = tables[t] + " ";
                    if (i == t) // for delete by FK
                        Assert.assertTrue("contains: " + tab, sql.contains(tab));
                    else
                        Assert.assertFalse("does not contain: " + tab, sql.contains(tab));
                }
            }
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSelectMinMaxLastModifiedSQL()
    {
        try
        {
            Date d2 = new Date();
            Date d1 = new Date(d2.getTime() - 3600*1000L); // one hour ago
            String sql = gen.getObservationSelectSQL(Observation.class, d1, d2, SQLGenerator.MAX_DEPTH);
            log.debug("SQL: " + sql);
            sql = sql.toLowerCase();
            DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

            String exp1 = "observation.maxlastmodified >= '" + df.format(d1) + "'";
            String exp2 = "observation.maxlastmodified <= '" + df.format(d2) + "'";
            Assert.assertTrue(sql.contains(exp1));
            Assert.assertTrue(sql.contains(exp2));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
