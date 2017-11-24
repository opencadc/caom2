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
import ca.nrc.cadc.caom2.CaomIDGenerator;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public abstract class AbstractReadAccessDAOTest
{
    protected static Logger log;

    boolean deletionTrack;
    boolean useLongForUUID;
    ReadAccessDAO dao;
    ObservationDAO obsDAO;
    DatabaseTransactionManager txnManager;

    protected Class[] entityClasses;

    protected AbstractReadAccessDAOTest(Class genClass, String server, String database, String schema, boolean useLongForUUID, boolean deletionTrack)
        throws Exception
    {
        this.useLongForUUID = useLongForUUID;
        this.deletionTrack = deletionTrack;
        try
        {
            Map<String,Object> config = new TreeMap<String,Object>();
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
            DBUtil.createJNDIDataSource("jdbc/testcaom2", cc);
            //config.put("server", server);
            config.put("database", database);
            config.put("jndiDataSourceName", "jdbc/testcaom2");
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);

            this.dao = new ReadAccessDAO();
            dao.setConfig(config);
            
            this.obsDAO = new ObservationDAO();
            obsDAO.setConfig(config);
            
            this.txnManager = new DatabaseTransactionManager(obsDAO.getDataSource());
        }
        catch(Exception ex)
        {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    protected UUID genID()
    {
        if (useLongForUUID)
        {
            Long lsb = CaomIDGenerator.getInstance().generateID();
            return new UUID(0L, lsb);
        }
        return UUID.randomUUID();
    }
    @Before
    public void setup()
        throws Exception
    {
        log.info("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        for (Class c : entityClasses)
        {
            String cn = c.getSimpleName();
            String s = gen.getTable(c);

            String sql = "delete from " + s;
            log.info("setup: " + sql);
            log.info("dataSource: " + ds);
            log.info("dataSource.connection: " + ds.getConnection());
            ds.getConnection().createStatement().execute(sql);
            if (deletionTrack)
            {
                sql = sql.replace(cn, "Deleted"+cn);
                log.info("setup: " + sql);
                ds.getConnection().createStatement().execute(sql);
            }
        }
        log.info("clearing old tables... OK");
    }

    @Test
    public void testGetNotFound()
    {
        try
        {
            UUID id = genID();
            for (int i=0; i<entityClasses.length; i++)
            {
                ReadAccess ra = dao.get(entityClasses[i], id);
                Assert.assertNull(entityClasses[i].getSimpleName(), ra);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // overridable in subclass to extend checks
    protected void checkPut(String s, ReadAccess expected, ReadAccess actual)
    {
        Assert.assertNotNull(s, actual);
        Assert.assertEquals(s+".assetID", expected.getAssetID(), actual.getAssetID());
        Assert.assertEquals(s+".groupID", expected.getGroupID(), actual.getGroupID());
        Assert.assertEquals(s+".metaChecksum", expected.getMetaChecksum(), actual.getMetaChecksum());
        testEqual(s+".lastModified", expected.getLastModified(), actual.getLastModified());
    }
    
    // overridable in subclass to extend checks
    protected void checkDelete(String s, ReadAccess expected, ReadAccess actual)
    {
        Assert.assertNull(actual);
    }
    
    protected void doPutGetDelete(ReadAccess expected)
        throws Exception
    {
        String s = expected.getClass().getSimpleName();
        dao.put(expected);
        ReadAccess actual = dao.get(expected.getClass(), expected.getID());
        
        checkPut(s, expected, actual);
        
        ReadAccess actual2 = dao.get(expected.getClass(), expected.getAssetID(), expected.getGroupID());
        checkPut(s, expected, actual2);
        Assert.assertEquals(expected.getID(), actual2.getID());
        
        // idempotent put (a no-net-op update) but test that skeleton extractor works
        dao.put(expected);
        ReadAccess actual3 = dao.get(expected.getClass(), expected.getID());
        checkPut(s, expected, actual3);
        Assert.assertEquals(expected.getID(), actual3.getID());
        Assert.assertEquals(actual2.getLastModified(), actual3.getLastModified());
        
        dao.delete(expected.getClass(), expected.getID());
        actual = dao.get(expected.getClass(), expected.getID());
        
        checkDelete(s, expected, actual);
    }

    @Test
    public void testPutGetDelete()
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
            //obsDAO.getTransactionManager().startTransaction();
            
            obsDAO.put(obs);
            UUID obsID = obs.getID();
            UUID planeID = obs.getPlanes().iterator().next().getID();
            
            ReadAccess expected;
            
            URI groupID =  new URI("ivo://cadc.nrc.ca/gms?FOO-777");
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                expected = (ReadAccess) ctor.newInstance(assetID, groupID);
                doPutGetDelete(expected);
            }
            
            groupID =  new URI("ivo://cadc.nrc.ca/gms?FOO-999");
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                expected = (ReadAccess) ctor.newInstance(assetID, groupID);
                doPutGetDelete(expected);
            }
            
            obsDAO.delete(obs.getID());
            
            //obsDAO.getTransactionManager().commitTransaction();
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
            //obsDAO.getTransactionManager().rollbackTransaction();
        }
    }
    
    @Test
    public void testRejectDuplicate()
    {
        // random ID is OK since we are testing observation only
        Observation obs = new SimpleObservation("FOO", "bar-"+UUID.randomUUID());
        Plane pl = new Plane("bar1");
        obs.getPlanes().add(pl);
        
        try
        {
            obsDAO.put(obs);
            UUID obsID = obs.getID();
            UUID planeID = obs.getPlanes().iterator().next().getID();
            
            for (Class c : entityClasses)
            {
                UUID assetID = planeID;
                if (ObservationMetaReadAccess.class.equals(c)) {
                    assetID = obsID;
                }
                URI groupID =  URI.create("ivo://cadc.nrc.ca/gms?FOO666");
                Constructor ctor = c.getConstructor(UUID.class, URI.class);
                ReadAccess expected = (ReadAccess) ctor.newInstance(assetID, groupID);
        
                // make sure it isn't there
                ReadAccess cur = dao.get(c, assetID, groupID);
                Assert.assertNull(cur);
                
                dao.put(expected);
                
                ReadAccess dupe = (ReadAccess) ctor.newInstance(assetID, groupID);
                try
                {
                    dao.put(dupe);
                }
                catch(DuplicateEntityException ex)
                {
                    log.info("testRejectDuplicate: caught expected DuplicateEntityException");
                }
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            try { obsDAO.delete(obs.getID()); }
            catch(Exception ex)
            {
                log.error("unexpected cleanup exception", ex);
            }
        }
    }
    
    @Test
    public void testGetList()
    {
        // random ID is OK since we are testing observation only
        Observation obs = new SimpleObservation("FOO", "bar-"+UUID.randomUUID());
        
        try
        {
            obsDAO.put(obs);
            UUID assetID = obs.getID();
            
            ReadAccess ra;
            URI groupID;
            Class c = ObservationMetaReadAccess.class;
            Constructor ctor = c.getConstructor(UUID.class, URI.class);
            for (int i=0; i<3; i++)
            {
                groupID = new URI("ivo://cadc.nrc.ca/gms?FOO" + i);
                ra = (ReadAccess) ctor.newInstance(assetID, groupID);
                dao.put(ra);
            }
            List<ReadAccess> ras = dao.getList(c, null, null, null);
            Assert.assertNotNull(ras);
            Assert.assertEquals(3, ras.size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            obsDAO.delete(obs.getID());
        }
    }
    
    // for comparing lastModified: Sybase isn't reliable to ms accuracy when using UTC
    protected void testEqual(String s, Date expected, Date actual)
    {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        log.debug("testEqual(Date,Date): " + expected.getTime() + " vs " + actual.getTime());
        Assert.assertTrue(s, Math.abs(expected.getTime() - actual.getTime()) < 3L);
    }
}
