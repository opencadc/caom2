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
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.util.Log4jInit;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
public class PostgresqlReadAccessDAOTest extends AbstractReadAccessDAOTest
{
    static String schema = "caom2";

    static
    {
        log = Logger.getLogger(PostgresqlReadAccessDAOTest.class);
        Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.INFO);

        String testSchema = UtilTest.getTestSchema();
        if (testSchema != null)
        {
            schema = testSchema;
        }
    }

    public PostgresqlReadAccessDAOTest()
        throws Exception
    {
        super(PostgreSQLGenerator.class, "CAOM2_PG_TEST", "cadctest", schema, false, false);
        this.entityClasses = new Class[]
        {
            ObservationMetaReadAccess.class,
            PlaneMetaReadAccess.class,
            PlaneDataReadAccess.class
        };

        InitDatabase init = new InitDatabase(super.dao.getDataSource(), "cadctest", schema);
        init.doInit();
    }

    @Test
    public void testNoAssetFail()
    {
        UUID assetID = genID();
        try
        {
            URI groupID =  new URI("ivo://cadc.nrc.ca/gms?FOO777");
            ReadAccess expected;

            for (Class c : entityClasses)
            {
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                expected = (ReadAccess) ctor.newInstance(assetID, groupID);
                try
                {
                    dao.put(expected);
                    org.junit.Assert.fail("put did not throw, expected: DataIntegrityViolationException"
                        + " but successfully put " + expected);
                }
                catch(DataIntegrityViolationException ex)
                {
                    log.debug("caught expected exception: " + ex);
                }
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            org.junit.Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testExtendedUpdate()
    {
        Observation obs = new SimpleObservation("FOO", "bar-" + UUID.randomUUID());
        Plane pl = new Plane("bar1");

        Artifact ar = new Artifact(URI.create("ad:FOO/bar1.fits"), ProductType.SCIENCE, ReleaseType.DATA);
        Part pp = new Part(0);
        Chunk ch = new Chunk();
        pp.getChunks().add(ch);
        ar.getParts().add(pp);
        pl.getArtifacts().add(ar);
        obs.getPlanes().add(pl);
        
        try
        {
            JdbcTemplate jdbc = new JdbcTemplate(dao.getDataSource());

            obsDAO.put(obs);
            UUID obsID = obs.getID();
            UUID planeID = obs.getPlanes().iterator().next().getID();

            URI group1 =  new URI("ivo://cadc.nrc.ca/gms?FOO-333");
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                ReadAccess expected = (ReadAccess) ctor.newInstance(assetID, group1);
                dao.put(expected);

                Class[] assetClass = getAssetClasses(expected);
                for (Class ac : assetClass)
                {
                    String vecSQL = getGroupVectorSQL(ac, expected);
                    log.info("checkUpdate: " + vecSQL);
                    String vec = (String) jdbc.queryForObject(vecSQL, String.class);
                    log.info("checkUpdate: " + expected.getClass().getSimpleName() + " " + vec);
                    String[] groups = vec.split(" ");
                    Assert.assertEquals("insert 1", 1, groups.length);
                    // vector should be in alphabetic order with tick marks
                    Assert.assertEquals("insert 1", "'"+expected.getGroupName()+"'", groups[0]);
                }
            }

            URI group2 =  new URI("ivo://cadc.nrc.ca/gms?FOO-444");
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                ReadAccess expected1 = (ReadAccess) ctor.newInstance(assetID, group1);
                ReadAccess expected2 = (ReadAccess) ctor.newInstance(assetID, group2);

                dao.put(expected2);

                Class[] assetClass = getAssetClasses(expected2);
                for (Class ac : assetClass)
                {
                    String vecSQL = getGroupVectorSQL(ac, expected2);
                    log.info("checkUpdate: " + vecSQL);
                    String vec = (String) jdbc.queryForObject(vecSQL, String.class);
                    log.info("checkUpdate: " + expected2.getClass().getSimpleName() + " " + vec);
                    String[] groups = vec.split(" ");
                    Assert.assertEquals(2, groups.length);
                    // vector should be in alphabetic order with tick marks
                    Assert.assertEquals("insert 2", "'"+expected1.getGroupName()+"'", groups[0]);
                    Assert.assertEquals("insert 2", "'"+expected2.getGroupName()+"'", groups[1]);
                }
            }

            // test removal
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                ReadAccess cur = dao.get(c, assetID, group1);
                Assert.assertNotNull("found group1 tuple", cur);
                dao.delete(c, cur.getID());

                // now only group 2 should be in the vector
                ReadAccess expected = (ReadAccess) ctor.newInstance(assetID, group2);

                Class[] assetClass = getAssetClasses(cur);
                for (Class ac : assetClass)
                {
                    String vecSQL = getGroupVectorSQL(ac, cur);
                    log.info("checkUpdate: " + vecSQL);
                    String vec = (String) jdbc.queryForObject(vecSQL, String.class);
                    log.info("checkUpdate: " + cur.getClass().getSimpleName() + " " + vec);
                    String[] groups = vec.split(" ");
                    Assert.assertEquals("delete 1", 1, groups.length);
                    // vector should be in alphabetic order with tick marks
                    Assert.assertEquals("delete 1", "'"+expected.getGroupName()+"'", groups[0]);
                }
            }


        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            org.junit.Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            //obsDAO.delete(obs.getID());
        }
    }

    @Override
    protected void checkDelete(String s, ReadAccess expected, ReadAccess actual)
    {
        super.checkDelete(s, expected, actual);

        Class[] assetClass = getAssetClasses(expected);
        for (Class ac : assetClass)
        {
            JdbcTemplate jdbc = new JdbcTemplate(dao.getDataSource());
            String sql = getAssetCountSQL(ac, expected);
            log.debug(s + " " + expected.getClass().getSimpleName() + "\n" + sql);
            int count = jdbc.queryForInt(sql);
            Assert.assertEquals(s + ".checkDelete for " + ac.getSimpleName(), 0, count);
        }

    }

    @Override
    protected void checkPut(String s, ReadAccess expected, ReadAccess actual)
    {
        super.checkPut(s, expected, actual);

        Class[] assetClass = getAssetClasses(expected);
        for (Class ac : assetClass)
        {
            JdbcTemplate jdbc = new JdbcTemplate(dao.getDataSource());
            String sql = getAssetCountSQL(ac, expected);
            log.debug(s + " " + expected.getClass().getSimpleName() + "\n" + sql);
            int count = jdbc.queryForInt(sql);
            Assert.assertEquals(s + ".checkPut for " + ac.getSimpleName(), 1, count);
        }
    }

    private String getAssetCountSQL(Class ac, ReadAccess expected)
    {
        BaseSQLGenerator gen = (BaseSQLGenerator) dao.getSQLGenerator();
        String assetTable = gen.getTable(ac);
        String kCol = gen.getPrimaryKeyColumn(ac);
        if (PlaneMetaReadAccess.class.equals(expected.getClass())
                && !Plane.class.equals(ac))
        {
            // HACK: see PostgresqlSQLGenerator.getUpdateAssetSQL
            kCol = gen.getPrimaryKeyColumn(Plane.class);
        }
        String assetLiteral = dao.gen.literal(expected.getAssetID());

        String raCol = gen.getReadAccessCol(expected.getClass());
        StringBuilder sb = new StringBuilder();
        sb.append("select count(*) from ").append(assetTable);
        sb.append(" where ").append(kCol).append(" = ").append(assetLiteral);
        sb.append(" and ").append(raCol).append(" @@ '").append(expected.getGroupName()).append("'::tsquery");
        String sql = sb.toString();
        return sql;
    }

    private String getGroupVectorSQL(Class ac, ReadAccess expected)
    {
        BaseSQLGenerator gen = (BaseSQLGenerator) dao.getSQLGenerator();
        String assetTable = gen.getTable(ac);
        String kCol = gen.getPrimaryKeyColumn(ac);
        if (PlaneMetaReadAccess.class.equals(expected.getClass())
                && !Plane.class.equals(ac))
        {
            // HACK: see PostgresqlSQLGenerator.getUpdateAssetSQL
            kCol = gen.getPrimaryKeyColumn(Plane.class);
        }

        String assetLiteral = dao.gen.literal(expected.getAssetID());

        String raCol = gen.getReadAccessCol(expected.getClass());
        StringBuilder sb = new StringBuilder();
        sb.append("select ").append(raCol).append(" from ").append(assetTable);
        sb.append(" where ").append(kCol).append(" = ").append(assetLiteral);
        String sql = sb.toString();
        return sql;
    }

    private Class[] getAssetClasses(ReadAccess expected)
    {
        Class[] assetClass = null;
        if (expected instanceof ObservationMetaReadAccess)
        {
            assetClass = new Class[] { Observation.class };
        }
        else if (expected instanceof PlaneDataReadAccess)
        {
            assetClass = new Class[] { Plane.class };
        }
        else if (expected instanceof PlaneMetaReadAccess)
        {
            assetClass = new Class[] { Plane.class, Artifact.class, Part.class, Chunk.class };
        }

        if (assetClass == null)
            throw new IllegalStateException("unexpected ReadAccess type: " + expected.getClass().getName());

        return assetClass;
    }

}
