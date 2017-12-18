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

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyBand;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Quality;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.Requirements;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Status;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.TargetType;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * Base class for all DAO tests. Subclasses must call the protected ctor to setup
 * server, database, schema, and some options.
 * 
 * @author pdowler
 */
public abstract class AbstractObservationDAOTest
{
    protected static Logger log;

    // round-trip timestamp comparisons must not be more lossy than this
    public static final long TIME_TOLERANCE = 3L;
    
    public static final Long TEST_LONG = new Long(123456789L);
    public static final List<String> TEST_KEYWORDS = new ArrayList<String>();
    public static Date TEST_DATE;
        
    
    static
    {
        TEST_KEYWORDS.add("abc");
        TEST_KEYWORDS.add("x=1");
        TEST_KEYWORDS.add("foo=bar");
        TEST_KEYWORDS.add("foo:42");
        try
        {
            TEST_DATE = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC).parse("1999-01-02 12:13:14.567");
        }
        catch(Exception oops)
        {
            log.error("BUG", oops);
        }
        Log4jInit.setLevel("ca.nrc.cadc.caom2.eprsistence", Level.INFO);
    }

    boolean deletionTrack;
    boolean useLongForUUID;
    ObservationDAO dao;
    DeletedEntityDAO ded;
    TransactionManager txnManager;

    Class[] ENTITY_CLASSES =
    {
        Chunk.class, Part.class, Artifact.class, Plane.class, Observation.class
    };

    protected AbstractObservationDAOTest(Class genClass, String server, String database, String schema, 
            boolean useLongForUUID, boolean deletionTrack)
        throws Exception
    {
        this.useLongForUUID = useLongForUUID;
        this.deletionTrack = deletionTrack;
        try
        {
            Map<String,Object> config = new TreeMap<String,Object>();
            config.put("server", server);
            config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            this.dao = new ObservationDAO();
            dao.setConfig(config);
            this.txnManager = dao.getTransactionManager();
            
            ded = new DeletedEntityDAO();
            ded.setConfig(config);
        }
        catch(Exception ex)
        {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }
    
    @Before
    public void setup()
        throws Exception
    {
        log.info("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        for (Class c : ENTITY_CLASSES)
        {
            String cn = c.getSimpleName();
            String s = gen.getTable(c);
            
            String sql = "delete from " + s;
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
            if (deletionTrack && Observation.class.equals(c))
            {
                sql = sql.replace(cn, "DeletedObservation");
                log.debug("setup: " + sql);
                ds.getConnection().createStatement().execute(sql);
            }
        }
        log.info("clearing old tables... OK");
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
    public void testGetObservationStateList()
    {
        try
        {
            String collection = "FOO";
            
            Thread.sleep(10);
            
            Observation obs = new SimpleObservation(collection, "bar1");
            dao.put(obs);
            Observation o = dao.getShallow(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date start = new Date(o.getMaxLastModified().getTime() - 2*TIME_TOLERANCE); // before 1
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar2");
            dao.put(obs);
            o = dao.getShallow(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date mid = new Date(o.getMaxLastModified().getTime() + 2*TIME_TOLERANCE); // after 2
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar3");
            dao.put(obs);
            Assert.assertTrue(dao.exists(obs.getURI()));
            log.info("created: " + obs);
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar4");
            dao.put(obs);
            o = dao.getShallow(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date end = new Date(o.getMaxLastModified().getTime() + 2*TIME_TOLERANCE); // after 4
            Thread.sleep(10);
            
            Integer batchSize = 100;
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            
            List<ObservationState> result = dao.getObservationList(collection, start, end, batchSize);
            for (int i=0; i<result.size(); i++)
            {
                ObservationState os = result.get(i);
                log.info("found: " + df.format(os.maxLastModified) + " " + os);
                ObservationURI exp = new ObservationURI(collection, "bar"+(i+1)); // 1 2 3 4
                Assert.assertEquals(exp, os.getURI());
            }
            Assert.assertEquals("start-end", 4, result.size());
            
            result = dao.getObservationList(collection, start, end, batchSize, false); // descending order
            for (int i=0; i<result.size(); i++)
            {
                ObservationState os = result.get(i);
                log.info("start-end found: " + df.format(os.maxLastModified) + " " +  os);
                ObservationURI exp = new ObservationURI(collection, "bar"+(4-i)); // 4 3 2 1
                Assert.assertEquals(exp, os.getURI());
            }
            Assert.assertEquals("start-end", 4, result.size());
            
            result = dao.getObservationList(collection, start, mid, batchSize);
            for (ObservationState os : result) {
                log.info("start-mid found: " + df.format(os.maxLastModified) + " " + os);
            }
            Assert.assertEquals("start-mid", 2, result.size());
            
            result = dao.getObservationList(collection, mid, end, batchSize);
            
            for (ObservationState os : result) {
                log.info("mid-end found: " + df.format(os.maxLastModified) + " " + os);
            }
            Assert.assertEquals(2, result.size());
            
            try
            {
                result = dao.getObservationList(null, start, end, batchSize);
                Assert.fail("expected IllegalArgumentException for null collection, got results");
            }
            catch(IllegalArgumentException ex)
            {
                log.info("caught expected exception: " + ex);
            }
            
            result = dao.getObservationList(collection, null, end, batchSize);
            Assert.assertEquals("-end", 4, result.size());
            
            result = dao.getObservationList(collection, start, null, batchSize);
            Assert.assertEquals("start-", 4, result.size());
            
            result = dao.getObservationList(collection, null, null, batchSize);
            
            result = dao.getObservationList(collection, start, null, null);
        }            
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetObservationStateAndObservationList()
    {
        try
        {
            String collection = "FOO";
            
            Observation obs = new SimpleObservation(collection, "bar1");
            dao.put(obs);
            obs = dao.get(obs.getURI());
            Assert.assertNotNull(obs);
            Date start = new Date(obs.getMaxLastModified().getTime() - 5L);
            log.info("created: " + obs);
            
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar2");
            dao.put(obs);
            obs = dao.get(obs.getURI());
            Assert.assertNotNull(obs);
            log.info("created: " + obs);
            Date mid = new Date(obs.getMaxLastModified().getTime() + 5L);
            
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar3");
            dao.put(obs);
            Assert.assertTrue(dao.exists(obs.getURI()));
            log.info("created: " + obs);
            Thread.sleep(10);
            
            obs = new SimpleObservation(collection, "bar4");
            dao.put(obs);
            obs = dao.get(obs.getURI());
            Assert.assertNotNull(obs);
            log.info("created: " + obs);
            Date end = new Date(obs.getMaxLastModified().getTime() + 5L);
            
            Integer batchSize = 100;
            
            List<ObservationResponse> result = dao.getList(collection, start, end, batchSize);
            for (ObservationResponse os : result)
            {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(4, result.size());
            
            result = dao.getList(collection, start, mid, batchSize);
            for (ObservationResponse os : result)
            {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(2, result.size());
            
            result = dao.getList(collection, mid, end, batchSize);
            for (ObservationResponse os : result)
            {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(2, result.size());
            
            try
            {
                String str = null;
                result = dao.getList(str, start, end, batchSize);
                Assert.fail("expected IllegalArgumentException for null collection, got results");
            }
            catch(IllegalArgumentException ex)
            {
                log.info("caught expected exception: " + ex);
            }
            
            result = dao.getList(collection, null, end, batchSize);
            Assert.assertEquals(4, result.size());
            
            result = dao.getList(collection, start, null, batchSize);
            Assert.assertEquals(4, result.size());
            
            result = dao.getList(collection, null, null, batchSize);
            
            result = dao.getList(collection, start, null, null);
        }            
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    public void testGetDeleteNonExistentObservation()
    {
        try
        {
            ObservationURI uri = new ObservationURI("TEST", "NonExistentObservation");
            Observation obs = dao.get(uri);
            Assert.assertNull(uri.toString(), obs);
            
            UUID notFound = dao.getID(uri);
            Assert.assertNull(uri.toString(), notFound);
            
            UUID uuid = UUID.randomUUID();
            ObservationURI nuri = dao.getURI(uuid);
            Assert.assertNull(uuid.toString(), nuri);
            Observation nobs = dao.get(uuid);
            Assert.assertNull(uuid.toString(), nobs);
            
            // should return without failing
            dao.delete(uri);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutGetDelete()
    {
        try
        {
            Observation orig = getTestObservation(false, 5, false, true);
            UUID externalID = orig.getID();
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertFalse(dao.exists(orig.getURI()));
            //txnManager.commitTransaction();

            // PUT
            //txnManager.startTransaction();
            dao.put(orig);
            //txnManager.commitTransaction();
            
            // this is so we can detect incorrect timestamp round trips
            // caused by assigning something other than what was stored
            Thread.sleep(2 * TIME_TOLERANCE);
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertTrue(dao.exists(orig.getURI()));
            //txnManager.commitTransaction();

            // GET by URI
            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found by URI", retrieved);
            testEqual(orig, retrieved);

            // GET by ID
            retrieved = dao.get(orig.getID());
            Assert.assertNotNull("found by ID", retrieved);
            testEqual(orig, retrieved);
            
            // DELETE by ID
            //txnManager.startTransaction();
            dao.delete(orig.getID());
            //txnManager.commitTransaction();
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertFalse("exists", dao.exists(orig.getURI()));
            //txnManager.commitTransaction();
            
            log.info("check deletion track: " + orig.getID());
            
            DeletedEntity de = ded.get(DeletedObservation.class, orig.getID());
            Assert.assertNotNull("deletion tracker", de);
            Assert.assertEquals("deleted.id", orig.getID(), de.getID());
            Assert.assertNotNull("deleted.lastModified", de.getLastModified());
            
            DeletedObservation doe = (DeletedObservation) de;
            Assert.assertEquals("deleted.uri", orig.getURI(), doe.getURI());
            
            Assert.assertFalse("open transaction", txnManager.isOpen());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

     @Test
    public void testNonOriginPut()
    {
        try
        {
            dao.setOrigin(false);
            Observation orig = getTestObservation(false, 5, false, true);
            UUID externalID = orig.getID();
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertFalse(dao.exists(orig.getURI()));
            //txnManager.commitTransaction();

            // PUT
            //txnManager.startTransaction();
            dao.put(orig);
            //txnManager.commitTransaction();
            
            // this is so we can detect incorrect timestamp round trips
            // caused by assigning something other than what was stored
            Thread.sleep(2 * TIME_TOLERANCE);
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertTrue(dao.exists(orig.getURI()));
            //txnManager.commitTransaction();

            // GET by URI
            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found by URI", retrieved);
            testEqual(orig, retrieved);

            // GET by ID
            retrieved = dao.get(orig.getID());
            Assert.assertNotNull("found by ID", retrieved);
            testEqual(orig, retrieved);
            
            // non-origin: make sure UUID did not change
            Assert.assertEquals("non-origin UUID", externalID, retrieved.getID());
            
            // DELETE by ID
            //txnManager.startTransaction();
            dao.delete(orig.getID());
            //txnManager.commitTransaction();
            
            // EXISTS
            //txnManager.startTransaction();
            Assert.assertFalse("exists", dao.exists(orig.getURI()));
            //txnManager.commitTransaction();
            
            log.info("check deletion track: " + orig.getID());
            
            DeletedEntity de = ded.get(DeletedObservation.class, orig.getID());
            Assert.assertNotNull("deletion tracker", de);
            Assert.assertEquals("deleted.id", orig.getID(), de.getID());
            Assert.assertNotNull("deleted.lastModified", de.getLastModified());
            
            DeletedObservation doe = (DeletedObservation) de;
            Assert.assertEquals("deleted.uri", orig.getURI(), doe.getURI());
            
            Assert.assertFalse("open transaction", txnManager.isOpen());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally {
            dao.setOrigin(true);
        }
    }

    @Test
    public void testPutSimpleObservation()
    {
        boolean[] completeness = new boolean[] { false, true };
        boolean[] science = new boolean[] { false, true };
        
        try
        {
            int minDepth = 1;
            int maxDepth = 5;
            for (boolean full : completeness)
            {
                for (boolean sci : science)
                {
                    for (int i=minDepth; i<=maxDepth; i++)
                    {
                        log.info("testPutSimpleObservation: full=" + full + ", sci=" + sci + ", depth=" + i);
                        Observation orig = getTestObservation(full, i, false, sci);

                        //txnManager.startTransaction();
                        dao.put(orig);
                        //txnManager.commitTransaction();

                        // this is so we can detect incorrect timestamp round trips
                        // caused by assigning something other than what was stored
                        Thread.sleep(2*TIME_TOLERANCE);
                        
                        Observation retrieved = dao.get(orig.getURI());
                        Assert.assertNotNull("found", retrieved);
                        testEqual(orig, retrieved);
                        
                        //txnManager.startTransaction();
                        dao.delete(orig.getURI());
                        //txnManager.commitTransaction();

                        Observation deleted = dao.get(orig.getURI());
                        Assert.assertNull("deleted", deleted);
                    }
                }
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutCompositeObservation()
    {
        try
        {
            int minDepth = 1;
            int maxDepth = 5;
            for (boolean full : new boolean[] { false, true })
            {
                for (int i=minDepth; i<=maxDepth; i++)
                {
                    log.info("testPutCompositeObservation: full=" + full + ", depth=" + i);
                    Observation orig = getTestObservation(full, i, true, true);

                    //txnManager.startTransaction();
                    dao.put(orig);
                    //txnManager.commitTransaction();

                    // this is so we can detect incorrect timestamp round trips
                    // caused by assigning something other than what was stored
                    Thread.sleep(2*TIME_TOLERANCE);
                        
                    Observation retrieved = dao.get(orig.getURI());
                    Assert.assertNotNull("found", retrieved);
                    testEqual(orig, retrieved);

                    //txnManager.startTransaction();
                    dao.delete(orig.getURI());
                    //txnManager.commitTransaction();

                    Observation deleted = dao.get(orig.getURI());

                    Assert.assertNull("deleted", deleted);
                }
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateSimpleObservation()
    {
        try
        {
            int minDepth = 1;
            int maxDepth = 5;
            boolean full = true;
            for (int i=minDepth; i<=maxDepth; i++)
            {
                log.info("testUpdateSimpleObservation: full=" + full + ", depth=" + i);
                Observation orig = getTestObservation(full, i, true, true);

                //txnManager.startTransaction();
                dao.put(orig);
                //txnManager.commitTransaction();

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2*TIME_TOLERANCE);
                        
                Observation ret1 = dao.get(orig.getURI());

                Assert.assertNotNull("found", ret1);
                log.info("testUpdateSimpleObservation/created: orig vs ret1");
                testEqual(orig, ret1);

                // the lastModified timestamps are maintained by the DAO, so lets set them to null here
                Util.assignLastModified(ret1, null, "lastModified");
                for (Plane p : ret1.getPlanes())
                {
                    Util.assignLastModified(p, null, "lastModified");
                    for (Artifact a : p.getArtifacts())
                    {
                        Util.assignLastModified(a, null, "lastModified");
                        for (Part pa : a.getParts())
                        {
                            Util.assignLastModified(pa, null, "lastModified");
                            for (Chunk c : pa.getChunks())
                                Util.assignLastModified(c, null, "lastModified");
                        }
                    }
                }
                
                // now modify the objects
                ret1.proposal.getKeywords().add("something=new");
                if (i > 1)
                {
                    Plane p = ret1.getPlanes().iterator().next();
                    p.calibrationLevel = CalibrationLevel.PRODUCT;
                    if (i > 2)
                    {
                        Artifact a = p.getArtifacts().iterator().next();
                        a.contentType = "application/foo";
                        a.contentLength = 123456789L;
                        if (i > 3)
                        {
                            Part part = a.getParts().iterator().next();
                            part.productType = ProductType.PREVIEW;
                            if (i > 4)
                            {
                                Chunk c = part.getChunks().iterator().next();
                                c.observable = new ObservableAxis(new Slice(new Axis("flux", "J"), new Long(2)));
                                c.observable.independent = new Slice(new Axis("wavelength", "nm"), new Long(1));
                            }
                        }
                    }
                }
                //txnManager.startTransaction();
                dao.put(ret1);
                //txnManager.commitTransaction();

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2*TIME_TOLERANCE);
                        
                Observation ret2 = dao.get(orig.getURI());
                Assert.assertNotNull("found", ret2);
                log.info("testUpdateSimpleObservation/updated: ret1 vs ret2");
                testEqual(ret1, ret2);

                if (i > 1)
                {
                    // test setting lastModified on observation when it is only force-updated for maxLastModified
                    // the lastModified timestamps are maintained by the DAO, so lets set them to null here
                    Util.assignLastModified(ret1, null, "lastModified");
                    for (Plane p : ret1.getPlanes())
                    {
                        Util.assignLastModified(p, null, "lastModified");
                        for (Artifact a : p.getArtifacts())
                        {
                            Util.assignLastModified(a, null, "lastModified");
                            for (Part pa : a.getParts())
                            {
                                Util.assignLastModified(pa, null, "lastModified");
                                for (Chunk c : pa.getChunks())
                                    Util.assignLastModified(c, null, "lastModified");
                            }
                        }
                    }
                    Plane pp = ret1.getPlanes().iterator().next();
                    pp.calibrationLevel = CalibrationLevel.RAW_INSTRUMENTAL;
                    //txnManager.startTransaction();
                    dao.put(ret1);
                    //txnManager.commitTransaction();

                    // this is so we can detect incorrect timestamp round trips
                    // caused by assigning something other than what was stored
                    Thread.sleep(2*TIME_TOLERANCE);
                
                    Observation ret3 = dao.get(orig.getURI());
                    Assert.assertNotNull("found", ret3);
                    
                    // make sure the DAO assigned lastModified values everywhere
                    Assert.assertNotNull(ret1.getLastModified());
                    for (Plane p : ret1.getPlanes())
                    {
                        Assert.assertNotNull(p.getLastModified());
                        for (Artifact a : p.getArtifacts())
                        {
                            Assert.assertNotNull(a.getLastModified());
                            for (Part pa : a.getParts())
                            {
                                Assert.assertNotNull(pa.getLastModified());
                                for (Chunk c : pa.getChunks())
                                    Assert.assertNotNull(c.getLastModified());
                            }
                        }
                    }

                    log.info("testUpdateSimpleObservation/updated-timestamps: ret1 vs ret3");
                    testEqual(ret1, ret3); // this makes sure the lastModified values assigned in the put match the ones in the DB
                }

                //txnManager.startTransaction();
                dao.delete(orig.getURI());
                //txnManager.commitTransaction();

                Observation deleted = dao.get(orig.getURI());

                Assert.assertNull("deleted", deleted);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateSimpleObservationAddRemovePlane()
    {
        try
        {
            int minDepth = 1;
            int maxDepth = 5;
            boolean full = true;
            for (int i=minDepth; i<=maxDepth; i++)
            {
                log.info("testUpdateSimpleObservationAddRemovePlane: full=" + full + ", depth=" + i);
                Observation orig = getTestObservation(full, i, false, true);
                int numPlanes = orig.getPlanes().size();
                
                log.debug("put: orig");
                //txnManager.startTransaction();
                dao.put(orig);
                //txnManager.commitTransaction();
                log.debug("put: orig DONE");
                
                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2*TIME_TOLERANCE);

                log.debug("get: orig");
                Observation ret1 = dao.get(orig.getURI());
                log.debug("get: orig DONE");
                Assert.assertNotNull("found", ret1);
                Assert.assertEquals(numPlanes, ret1.getPlanes().size());
                testEqual(orig, ret1);

                Plane newPlane = getTestPlane(full, "newPlane", i, false);
                ret1.getPlanes().add(newPlane);
                
                log.debug("put: added");
                //txnManager.startTransaction();
                dao.put(ret1);
                //txnManager.commitTransaction();
                log.debug("put: added DONE");

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2*TIME_TOLERANCE);
                
                log.debug("get: added");
                Observation ret2 = dao.get(orig.getURI());
                log.debug("get: added DONE");
                Assert.assertNotNull("found", ret2);
                Assert.assertEquals(numPlanes+1, ret1.getPlanes().size());
                testEqual(ret1, ret2);
                
                ret2.getPlanes().remove(newPlane);
                
                log.debug("put: removed");
                //txnManager.startTransaction();
                dao.put(ret2);
                //txnManager.commitTransaction();
                log.debug("put: removed DONE");
                
                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2*TIME_TOLERANCE);
                
                log.debug("get: removed");
                Observation ret3 = dao.get(orig.getURI());
                log.debug("get: removed DONE");
                Assert.assertNotNull("found", ret3);
                Assert.assertEquals(numPlanes, ret3.getPlanes().size());
                testEqual(orig, ret3);
                        
                //txnManager.startTransaction();
                dao.delete(orig.getURI());
                //txnManager.commitTransaction();

                Observation deleted = dao.get(orig.getURI());
                Assert.assertNull("deleted", deleted);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetObservationList()
    {
        try
        {
            log.info("testGetObservationList");
            Integer batchSize = new Integer(3);

            Observation o1 = new SimpleObservation(AbstractObservationDAOTest.class.getSimpleName(), "obs1");
            Observation o2 = new SimpleObservation(AbstractObservationDAOTest.class.getSimpleName(), "obsA");
            Observation o3 = new SimpleObservation(AbstractObservationDAOTest.class.getSimpleName(), "obs2");
            Observation o4 = new SimpleObservation(AbstractObservationDAOTest.class.getSimpleName(), "obsB");
            Observation o5 = new SimpleObservation(AbstractObservationDAOTest.class.getSimpleName(), "obs3");

            //txnManager.startTransaction();
            dao.put(o1);
            Thread.sleep(10L);
            dao.put(o2);
            Thread.sleep(10L);
            dao.put(o3);
            Thread.sleep(10L);
            dao.put(o4);
            Thread.sleep(10L);
            dao.put(o5);
            //txnManager.commitTransaction();

            List<Observation> obs;

            // get first batch
            obs = dao.getList(Observation.class, null, null, batchSize);
            Assert.assertNotNull(obs);
            Assert.assertEquals(3, obs.size());
            Assert.assertEquals(o1.getURI(), obs.get(0).getURI());
            Assert.assertEquals(o2.getURI(), obs.get(1).getURI());
            Assert.assertEquals(o3.getURI(), obs.get(2).getURI());

            // get next batch
            obs = dao.getList(Observation.class, o3.getMaxLastModified(), null, batchSize);
            Assert.assertNotNull(obs);
            Assert.assertEquals(3, obs.size()); // o3 gets picked up by the >=
            Assert.assertEquals(o3.getURI(), obs.get(0).getURI());
            Assert.assertEquals(o4.getURI(), obs.get(1).getURI());
            Assert.assertEquals(o5.getURI(), obs.get(2).getURI());

            //txnManager.startTransaction();
            dao.delete(o1.getURI());
            dao.delete(o2.getURI());
            dao.delete(o3.getURI());
            dao.delete(o4.getURI());
            dao.delete(o5.getURI());
            //txnManager.commitTransaction();

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutObservationDeleteChildren()
    {
        try
        {
            log.info("testPutObservationDeleteChildren");
            Observation orig = getTestObservation(false, 2, false, true);

            //txnManager.startTransaction();
            dao.put(orig);
            //txnManager.commitTransaction();

            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found", retrieved);
            log.info("retrieved: " + retrieved.getPlanes().size());
            testEqual(orig, retrieved);

            Plane rem = orig.getPlanes().iterator().next();
            orig.getPlanes().remove(rem);

            //txnManager.startTransaction();
            dao.put(orig);
            //txnManager.commitTransaction();

            Observation smaller = dao.get(orig.getURI());
            Assert.assertNotNull("found", smaller);
            log.info("smaller: " + smaller.getPlanes().size());
            testEqual(orig, smaller);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateMaxLastModified()
    {
        try
        {
            log.info("testUpdateMaxLastModified");
            Observation orig = getTestObservation(false, 2, false, true);

            //txnManager.startTransaction();
            dao.put(orig);
            //txnManager.commitTransaction();

            Thread.sleep(10L);
            
            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found", retrieved);
            testEqual(orig, retrieved);

            Date expected = retrieved.getMaxLastModified();

            for (Plane p : orig.getPlanes())
            {
                p.calibrationLevel = CalibrationLevel.PRODUCT;
            }

            Thread.sleep(10L);

            //txnManager.startTransaction();
            dao.put(orig); // maxLastModified increases here due to changes in the planes
            //txnManager.commitTransaction();

            Observation changed = dao.get(orig.getURI());
            Assert.assertNotNull("found", changed);

            long t1 = expected.getTime();
            long t2 = changed.getMaxLastModified().getTime();
            Assert.assertTrue("maxLastModified increased from update", (t2-t1) > 10L);
            
            Thread.sleep(10L);
            
            Date d1 = changed.getMaxLastModified();
            Plane firstPlane = changed.getPlanes().iterator().next();
            boolean del = changed.getPlanes().remove(firstPlane);
            Assert.assertTrue("deleted plane", del);
            
            //txnManager.startTransaction();
            dao.put(changed); // maxLastModified increases here due to delete of plane
            //txnManager.commitTransaction();
            
            Observation o2 = dao.get(orig.getURI());
            Assert.assertEquals("num planes after delete", changed.getPlanes().size(), o2.getPlanes().size());
            Date d2 = o2.getMaxLastModified();
            t1 = d1.getTime();
            t2 = d2.getTime();
            Assert.assertTrue("maxLastModified increased from delete", (t2-t1) > 10L);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateOptimisationFields()
    {
        try
        {
            log.info("testUpdateOptimisationFields");
            Observation orig = getTestObservation(false, 5, false, true);
            for (Plane p : orig.getPlanes())
                Assert.assertNull("original metaRelease is null", p.metaRelease); // verify starting state
            
            dao.put(orig);

            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found", retrieved);
            testEqual(orig, retrieved);

            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 3600*1000L); // +1 hour
            
            for (Plane p : orig.getPlanes())
            {
                p.metaRelease = d1; // assign value; change metaChecksum
            }
            dao.put(orig); // maxLastModified increases here due to changes in the planes

            Observation changed = dao.get(orig.getURI());
            Assert.assertNotNull("found", changed);
            testEqual(orig, changed);
            
            checkOptimizations(orig, d1);
            
            // update Plane.metaRelease to d2
            for (Plane p : orig.getPlanes())
            {
                p.metaRelease = d2; // update value; change metaChecksum
            }
            dao.put(orig); // maxLastModified increases here due to changes in the planes
            
            // check for metaRelease in artifact, part, chunk = d2
            checkOptimizations(orig, d2);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            if ( txnManager.isOpen() )
                try { txnManager.rollbackTransaction(); }
                catch(Throwable t)
                {
                    log.error("failed to rollback transaction", t);
                }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    protected void checkOptimizations(Observation o, Date expectedMetaRelease)
    {
        log.info("checkOptimizations: no checks implemented");
    }

    // for comparing lastModified: Sybase isn't reliable to ms accuracy when using UTC
    protected void testEqual(String s, Date expected, Date actual)
    {
        if (expected == null)
        {
            Assert.assertNull(s, actual);
        }
        else
        {
            Assert.assertNotNull(s, actual);
            long dt = Math.abs(expected.getTime() - actual.getTime());
            Assert.assertTrue(s + ": " + expected.getTime() + " vs " + actual.getTime(), (dt <= TIME_TOLERANCE));
        }
    }
    
    // for comparing release dates: compare second
    protected void testEqualSeconds(String s, Date expected, Date actual)
    {
        if (expected == null)
        {
            Assert.assertNull(s, actual);
        }
        else
        {
            Assert.assertNotNull(s, actual);
            long esec = expected.getTime() / 1000L;
            long asec = actual.getTime() / 1000L;
            Assert.assertEquals(s, esec, asec);
        }
    }

    private void debugStateCodes(CaomEntity expected, CaomEntity actual)
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
        log.warn("====================== EXPECTED");
        int esc = getStateCode(expected);
        log.warn("====================== ACTUAL");
        int asc = getStateCode(actual);
        log.warn("====================== DONE");
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    private int getStateCode(CaomEntity e)
    {
        try
        {
            Method m = CaomEntity.class.getDeclaredMethod("checksum");
            m.setAccessible(true);
            // TODO: if we could get the return here, would not need to access private field below
            m.invoke(e);

            Field f = CaomEntity.class.getDeclaredField("stateCode");
            f.setAccessible(true);
            return f.getInt(e);
        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to find stateCode field", t);
        }
    }
    
    private void testEntity(CaomEntity expected, CaomEntity actual)
    {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);
        String cn = expected.getClass().getSimpleName();
        
        Assert.assertEquals(cn+".ID", expected.getID(), actual.getID());
    }
    
    private void testEntityChecksums(CaomEntity expected, CaomEntity actual)
    {
        log.debug("testEqual: " + expected + " == " + actual);
        String cn = expected.getClass().getSimpleName();
        
        // read from database should always have checksums
        Assert.assertNotNull(cn+".metaChecksum", actual.getMetaChecksum());
        Assert.assertNotNull(cn+".accMetaChecksum", actual.getAccMetaChecksum());
        
        // all checksums are computed and assigned on expected as a side effect of calling put
        Assert.assertEquals(cn+".metaChecksum", expected.getMetaChecksum(), actual.getMetaChecksum());
        Assert.assertEquals(cn+".accMetaChecksum", expected.getAccMetaChecksum(), actual.getAccMetaChecksum());

        try
        {
            // above verifies that checksum was written to and read from database
            // below checks that all values included in the checksum were faithfully written/read
            // in case some other asser is not catching it - fail  means there is a bug in the
            // comparisons this should catch it
            URI mcs = actual.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(cn + " recomputed metaChecksum", actual.getMetaChecksum(), mcs);
        }
        catch(Exception ex)
        {
            log.error(cn + " failed to compute metaChecksum", ex);
            Assert.fail(cn + " failed to compute metaChecksum: " + ex);
        }
        
        //Assert.assertEquals(cn+".getStateCode", expected.getStateCode(), actual.getStateCode());
        
        testEqual(cn+".lastModified", expected.getLastModified(), actual.getLastModified());
        
        // some tests rely on insert + add child + remove child and compare orig to final so 
        // maxLastModified changes
        //testEqual(cn+".maxLastModified", expected.getMaxLastModified(), actual.getMaxLastModified());
    }
    
    private void testEqual(Observation expected, Observation actual)
    {
        testEntity(expected, actual);
        Assert.assertEquals(expected.getURI(), actual.getURI());
        Assert.assertEquals("algorithm.name", expected.getAlgorithm().getName(), actual.getAlgorithm().getName());

        Assert.assertEquals("type", expected.type, actual.type);
        Assert.assertEquals("intent", expected.intent, actual.intent);
        Assert.assertEquals("sequenceNumber", expected.sequenceNumber, actual.sequenceNumber);

        testEqualSeconds("observation.metaRelease", expected.metaRelease, actual.metaRelease);

        if (expected.proposal != null)
        {
            Assert.assertEquals("proposal.id", expected.proposal.getID(), actual.proposal.getID());
            Assert.assertEquals("proposal.pi", expected.proposal.pi, actual.proposal.pi);
            Assert.assertEquals("proposal.project", expected.proposal.project, actual.proposal.project);
            Assert.assertEquals("proposal.title", expected.proposal.title, actual.proposal.title);
            testEqual("proposal.keywords", expected.proposal.getKeywords(), actual.proposal.getKeywords());
        }
        else
            Assert.assertNull("proposal", actual.proposal);
        if (expected.target != null)
        {
            Assert.assertNotNull("target", actual.target);
            Assert.assertEquals("target.name", expected.target.getName(), actual.target.getName());
            Assert.assertEquals("target.type", expected.target.type, actual.target.type);
            Assert.assertEquals("target.standard", expected.target.standard, actual.target.standard);
            Assert.assertEquals("target.redshift", expected.target.redshift, actual.target.redshift);
            testEqual("target.keywords", expected.target.getKeywords(), actual.target.getKeywords());
        }
        else
            Assert.assertNull("target", actual.target);
        
        if (expected.telescope != null)
        {
            Assert.assertNotNull("telescope", actual.telescope);
            Assert.assertEquals("telescope.name", expected.telescope.getName(), actual.telescope.getName());
            Assert.assertEquals("telescope.geoLocationX", expected.telescope.geoLocationX, actual.telescope.geoLocationX);
            Assert.assertEquals("telescope.geoLocationY", expected.telescope.geoLocationY, actual.telescope.geoLocationY);
            Assert.assertEquals("telescope.geoLocationZ", expected.telescope.geoLocationZ, actual.telescope.geoLocationZ);
            testEqual("telescope.keywords", expected.telescope.getKeywords(), actual.telescope.getKeywords());
        }
        else
            Assert.assertNull("telescope", actual.telescope);

        if (expected.instrument != null)
        {
            Assert.assertNotNull("instrument", actual.instrument);
            Assert.assertEquals("instrument.name", expected.instrument.getName(), actual.instrument.getName());
            testEqual("instrument.keywords", expected.instrument.getKeywords(), actual.instrument.getKeywords());
        }
        else
            Assert.assertNull("instrument", actual.instrument);

        if (expected.environment != null)
        {
            Assert.assertNotNull("environment", actual.environment);
            Assert.assertEquals("environment.seeing", expected.environment.seeing, actual.environment.seeing);
            Assert.assertEquals("environment.photometric", expected.environment.photometric, actual.environment.photometric);
            Assert.assertEquals("environment.humidity", expected.environment.humidity, actual.environment.humidity);
            Assert.assertEquals("environment.tau", expected.environment.tau, actual.environment.tau);
            Assert.assertEquals("environment.wavelengthTau", expected.environment.wavelengthTau, actual.environment.wavelengthTau);
        }
        else
            Assert.assertNull("environment", actual.environment);
        
        log.debug("num planes: " + expected.getPlanes().size() + " == " + actual.getPlanes().size());
        Assert.assertEquals("number of planes", expected.getPlanes().size(), actual.getPlanes().size());
        Iterator<Plane> e = expected.getPlanes().iterator();
        Iterator<Plane> a = actual.getPlanes().iterator();
        while ( e.hasNext() || a.hasNext() )
            testEqual(e.next(), a.next());
        
        testEntityChecksums(expected, actual);
    }

    private void testEqual(String name, Collection<String> expected, Collection<String> actual)
    {
        Assert.assertEquals(name, expected, actual);
    }
    
    private void testEqual(Plane expected, Plane actual)
    {
        testEntity(expected, actual);
        Assert.assertEquals(expected.getProductID(), actual.getProductID());
        Assert.assertEquals(expected.creatorID, actual.creatorID);
        Assert.assertEquals(expected.calibrationLevel, actual.calibrationLevel);
        Assert.assertEquals(expected.dataProductType, actual.dataProductType);
        testEqualSeconds("plane.metaRelease", expected.metaRelease, actual.metaRelease);
        testEqualSeconds("plane.dataRelease", expected.dataRelease, actual.dataRelease);
        if (expected.provenance != null)
        {
            Assert.assertEquals(expected.provenance.getName(), actual.provenance.getName());
            Assert.assertEquals(expected.provenance.reference, actual.provenance.reference);
            Assert.assertEquals(expected.provenance.version, actual.provenance.version);
            Assert.assertEquals(expected.provenance.project, actual.provenance.project);
            Assert.assertEquals(expected.provenance.producer, actual.provenance.producer);
            Assert.assertEquals(expected.provenance.runID, actual.provenance.runID);
            testEqualSeconds("provenance.lastExecuted", expected.provenance.lastExecuted, actual.provenance.lastExecuted);
            Assert.assertEquals("provenance.inputs", expected.provenance.getInputs(), actual.provenance.getInputs());
            testEqual("provenance.keywords", expected.provenance.getKeywords(), actual.provenance.getKeywords());
        }
        else
            Assert.assertNull(actual.provenance);

        if (expected.metrics != null)
        {
            Assert.assertEquals(expected.metrics.sourceNumberDensity, actual.metrics.sourceNumberDensity);
            Assert.assertEquals(expected.metrics.background, actual.metrics.background);
            Assert.assertEquals(expected.metrics.backgroundStddev, actual.metrics.backgroundStddev);
            Assert.assertEquals(expected.metrics.fluxDensityLimit, actual.metrics.fluxDensityLimit);
            Assert.assertEquals(expected.metrics.magLimit, actual.metrics.magLimit);
        }
        else
            Assert.assertNull(actual.metrics);
        
        if (expected.position != null)
        {
            Assert.assertNotNull("plane.position", actual.position);
            if (expected.position.bounds != null && Polygon.class.equals(expected.position.bounds.getClass()))
            {
                Polygon ep = (Polygon) expected.position.bounds;
                Polygon ap = (Polygon) actual.position.bounds;
                Assert.assertEquals("num points", ep.getPoints().size(), ap.getPoints().size());
                for (int i=0; i<ep.getPoints().size(); i++)
                {
                    Point ept = ep.getPoints().get(i);
                    Point apt = ap.getPoints().get(i);
                    Assert.assertEquals("point.cval1", ept.cval1, apt.cval1, 0.0);
                    Assert.assertEquals("point.cval2", ept.cval2, apt.cval2, 0.0);
                }
                Assert.assertEquals("num vertices", ep.getSamples().getVertices().size(), ap.getSamples().getVertices().size());
                for (int i=0; i<ep.getSamples().getVertices().size(); i++)
                {
                    Vertex ev = ep.getSamples().getVertices().get(i);
                    Vertex av = ap.getSamples().getVertices().get(i);
                    Assert.assertEquals("vertex.cval2", ev.getType(), av.getType());
                    Assert.assertEquals("vertex.cval1", ev.cval1, av.cval1, 0.0);
                    Assert.assertEquals("vertex.cval2", ev.cval2, av.cval2, 0.0);
                }
            } else if (expected.position.bounds != null && Circle.class.equals(expected.position.bounds.getClass())) {
                Circle ep = (Circle) expected.position.bounds;
                Circle ap = (Circle) actual.position.bounds;
                Assert.assertEquals("center.cval1", ep.getCenter().cval1, ap.getCenter().cval1, 0.0);
                Assert.assertEquals("center.cval2", ep.getCenter().cval2, ap.getCenter().cval2, 0.0);
                Assert.assertEquals("radius", ep.getRadius(), ap.getRadius(), 0.0);
            }
            else
                Assert.assertNull(actual.position.bounds);
            if (expected.position.dimension != null)
            {
                Assert.assertEquals(expected.position.dimension.naxis1, actual.position.dimension.naxis1);
                Assert.assertEquals(expected.position.dimension.naxis2, actual.position.dimension.naxis2);
            }
            else
                Assert.assertNull(actual.position.dimension);
            Assert.assertEquals(expected.position.resolution, actual.position.resolution);
            Assert.assertEquals(expected.position.sampleSize, actual.position.sampleSize);
            Assert.assertEquals(expected.position.timeDependent, actual.position.timeDependent);
            
        }
        if (expected.energy != null)
        {
            Assert.assertNotNull("plane.energy", actual.energy);
            if (expected.energy.bounds != null)
            {
                Assert.assertNotNull(actual.energy.bounds);
                Assert.assertEquals(expected.energy.bounds.getLower(), actual.energy.bounds.getLower(), 0.0);
                Assert.assertEquals(expected.energy.bounds.getUpper(), actual.energy.bounds.getUpper(), 0.0);
                Assert.assertEquals(expected.energy.bounds.getSamples().size(), actual.energy.bounds.getSamples().size());
                for (int i=0; i<expected.energy.bounds.getSamples().size(); i++)
                {
                    SubInterval esi = expected.energy.bounds.getSamples().get(i);
                    SubInterval asi = actual.energy.bounds.getSamples().get(i);
                    Assert.assertEquals("SubInterval.lb", esi.getLower(), asi.getLower(), 0.0);
                    Assert.assertEquals("SubInterval.ub", esi.getUpper(), asi.getUpper(), 0.0);
                }
            }
            else
                Assert.assertNull(actual.energy.bounds);
            Assert.assertEquals(expected.energy.bandpassName, actual.energy.bandpassName);
            Assert.assertEquals(expected.energy.dimension, actual.energy.dimension);
            Assert.assertEquals(expected.energy.emBand, actual.energy.emBand);
            Assert.assertEquals(expected.energy.resolvingPower, actual.energy.resolvingPower);
            Assert.assertEquals(expected.energy.restwav, actual.energy.restwav);
            Assert.assertEquals(expected.energy.sampleSize, actual.energy.sampleSize);
            Assert.assertEquals(expected.energy.transition, actual.energy.transition);
        }
        if (expected.time != null)
        {
            Assert.assertNotNull("plane.time", actual.time);
            if (expected.time.bounds != null)
            {
                Assert.assertNotNull(actual.time.bounds);
                Assert.assertEquals(expected.time.bounds.getLower(), actual.time.bounds.getLower(), 0.0);
                Assert.assertEquals(expected.time.bounds.getUpper(), actual.time.bounds.getUpper(), 0.0);
                Assert.assertEquals(expected.time.bounds.getSamples().size(), actual.time.bounds.getSamples().size());
                for (int i=0; i<expected.time.bounds.getSamples().size(); i++)
                {
                    SubInterval esi = expected.time.bounds.getSamples().get(i);
                    SubInterval asi = actual.time.bounds.getSamples().get(i);
                    Assert.assertEquals("SubInterval.lb", esi.getLower(), asi.getLower(), 0.0);
                    Assert.assertEquals("SubInterval.ub", esi.getUpper(), asi.getUpper(), 0.0);
                }
            }
            else
                Assert.assertNull(actual.time.bounds);
            Assert.assertEquals(expected.time.dimension, actual.time.dimension);
            Assert.assertEquals(expected.time.exposure, actual.time.exposure);
            Assert.assertEquals(expected.time.resolution, actual.time.resolution);
            Assert.assertEquals(expected.time.sampleSize, actual.time.sampleSize);
        }
        if (expected.polarization != null)
        {
            Assert.assertNotNull("plane.polarization", actual.polarization);
            if (expected.polarization.states != null)
            {
                Assert.assertNotNull(actual.polarization.states);
                Assert.assertEquals(expected.polarization.states.size(), actual.polarization.states.size());
                for (int i=0; i<expected.polarization.states.size(); i++)
                {
                    Assert.assertEquals(expected.polarization.states.get(i), actual.polarization.states.get(i));
                }
                Assert.assertEquals(expected.polarization.dimension, actual.polarization.dimension);
            }
        }

        log.debug("num artifacts: " + expected.getArtifacts().size() + " == " + actual.getArtifacts().size());
        Assert.assertEquals("number of artifacts", expected.getArtifacts().size(), actual.getArtifacts().size());
        Iterator<Artifact> ea = expected.getArtifacts().iterator();
        Iterator<Artifact> aa = actual.getArtifacts().iterator();
        while ( ea.hasNext() )
        {
            Artifact ex = ea.next();
            Artifact ac = aa.next();
            testEqual(ex, ac);
        }
        
        testEntityChecksums(expected, actual);
    }
    
    private void testEqual(Artifact expected, Artifact actual)
    {
        testEntity(expected, actual);
        Assert.assertEquals(expected.getURI(), actual.getURI());
        Assert.assertEquals(expected.contentLength, actual.contentLength);
        Assert.assertEquals(expected.contentType, actual.contentType);
        Assert.assertEquals(expected.contentChecksum, actual.contentChecksum);
        Assert.assertEquals(expected.getProductType(), actual.getProductType());
        Assert.assertEquals(expected.getReleaseType(), actual.getReleaseType());

        log.debug("num Parts: " + expected.getParts().size() + " == " + actual.getParts().size());
        Assert.assertEquals("number of parts", expected.getParts().size(), actual.getParts().size());
        Iterator<Part> ea = expected.getParts().iterator();
        Iterator<Part> aa = actual.getParts().iterator();
        while ( ea.hasNext() )
        {
            Part ex = ea.next();
            Part ac = aa.next();
            testEqual(ex, ac);
        }
        
        testEntityChecksums(expected, actual);
    }
    private void testEqual(Part expected, Part actual)
    {
        testEntity(expected, actual);
        Assert.assertEquals("part.name", expected.getName(), actual.getName());
        Assert.assertEquals("number of chunks", expected.getChunks().size(), actual.getChunks().size());
        Iterator<Chunk> ea = expected.getChunks().iterator();
        Iterator<Chunk> aa = actual.getChunks().iterator();
        while ( ea.hasNext() )
        {
            Chunk ex = ea.next();
            Chunk ac = aa.next();
            testEqual(ex, ac);
        }
        
        testEntityChecksums(expected, actual);
    }
    private void testEqual(Chunk expected, Chunk actual)
    {
        testEntity(expected, actual);
        
        Assert.assertEquals("productType", expected.productType, actual.productType);
        Assert.assertEquals("naxis", expected.naxis, actual.naxis);
        Assert.assertEquals("positionAxis1", expected.positionAxis1, actual.positionAxis1);
        Assert.assertEquals("positionAxis2", expected.positionAxis2, actual.positionAxis2);
        Assert.assertEquals("energyAxis", expected.energyAxis, actual.energyAxis);
        Assert.assertEquals("timeAxis", expected.timeAxis, actual.timeAxis);
        Assert.assertEquals("polarizationAxis", expected.polarizationAxis, actual.polarizationAxis);
        Assert.assertEquals("observableAxis", expected.observableAxis, actual.observableAxis);

        if (expected.position == null)
            Assert.assertNull("position", actual.position);
        else
        {
            testEqual(expected.position.getAxis(), actual.position.getAxis());
            Assert.assertEquals("position.coordsys", expected.position.coordsys, actual.position.coordsys);
            Assert.assertEquals("position.equinox", expected.position.equinox, actual.position.equinox);
            Assert.assertEquals("position.resolution", expected.position.resolution, actual.position.resolution);
        }

        if (expected.energy == null)
            Assert.assertNull("energy", actual.energy);
        else
        {
            testEqual("energy", expected.energy.getAxis(), actual.energy.getAxis());
            Assert.assertEquals("energy.specsys", expected.energy.getSpecsys(), actual.energy.getSpecsys());
            Assert.assertEquals("energy.ssysobs", expected.energy.ssysobs, actual.energy.ssysobs);
            Assert.assertEquals("energy.ssyssrc", expected.energy.ssyssrc, actual.energy.ssyssrc);
            Assert.assertEquals("energy.restfrq", expected.energy.restfrq, actual.energy.restfrq);
            Assert.assertEquals("energy.restwav", expected.energy.restwav, actual.energy.restwav);
            Assert.assertEquals("energy.velosys", expected.energy.velosys, actual.energy.velosys);
            Assert.assertEquals("energy.zsource", expected.energy.zsource, actual.energy.zsource);
            Assert.assertEquals("energy.velang", expected.energy.velang, actual.energy.velang);
            Assert.assertEquals("energy.bandpassName", expected.energy.bandpassName, actual.energy.bandpassName);
            Assert.assertEquals("energy.resolvingPower", expected.energy.resolvingPower, actual.energy.resolvingPower);
        }

        if (expected.time == null)
            Assert.assertNull("time", actual.time);
        else
        {
            testEqual("time", expected.time.getAxis(), actual.time.getAxis());
            Assert.assertEquals("time.exposure", expected.time.exposure, actual.time.exposure);
            Assert.assertEquals("time.resolution", expected.time.resolution, actual.time.resolution);
        }

        if (expected.polarization == null)
            Assert.assertNull("polarization", actual.polarization);
        else
        {
            testEqual("polarization", expected.polarization.getAxis(), actual.polarization.getAxis());
        }

        if (expected.observable == null)
            Assert.assertNull("observable", actual.observable);
        else
        {
            Assert.assertEquals("observable.depndent.axis.ctype",
                    expected.observable.getDependent().getAxis().getCtype(),
                    actual.observable.getDependent().getAxis().getCtype());
            Assert.assertEquals("observable.depndent.axis.cunit",
                    expected.observable.getDependent().getAxis().getCunit(),
                    actual.observable.getDependent().getAxis().getCunit());
            Assert.assertEquals("observable.depndent.bin", expected.observable.getDependent().getBin(), actual.observable.getDependent().getBin());
            if (expected.observable.independent == null)
            {
                Assert.assertNull("observable.independent", actual.observable.independent);
            }
            else
            {
                Assert.assertEquals("observable.independent.axis.ctype",
                    expected.observable.independent.getAxis().getCtype(),
                    actual.observable.independent.getAxis().getCtype());
                Assert.assertEquals("observable.independent.axis.cunit",
                    expected.observable.independent.getAxis().getCunit(),
                    actual.observable.independent.getAxis().getCunit());
                Assert.assertEquals("observable.independent.bin",
                        expected.observable.independent.getBin(),
                        actual.observable.independent.getBin());
            }
        }
        
        testEntityChecksums(expected, actual);
    }

    private void testEqual(String s, CoordAxis1D expected, CoordAxis1D actual)
    {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);

        Assert.assertEquals(s, expected.getAxis().getCtype(), actual.getAxis().getCtype());
        Assert.assertEquals(s, expected.getAxis().getCunit(), actual.getAxis().getCunit());
        
        if (expected.error == null)
            Assert.assertNull("error", actual.error);
        else
        {
            Assert.assertNotNull(s, actual.error);
            Assert.assertEquals("syser", expected.error.syser, actual.error.syser);
            Assert.assertEquals("rnder", expected.error.rnder, actual.error.rnder);
        }
        if (expected.range == null)
            Assert.assertNull(s, actual.range);
        else
        {
            Assert.assertNotNull(s, actual.range);
            UtilTest.testEqual(s, expected.range, actual.range);
        }
        if (expected.bounds == null)
            Assert.assertNull(s, actual.bounds);
        else
        {
            Assert.assertNotNull(s, actual.bounds);
            UtilTest.testEqual(s, expected.bounds, actual.bounds);
        }
        if (expected.function == null)
            Assert.assertNull(s, actual.function);
        else
        {
            Assert.assertNotNull(s, actual.function);
            UtilTest.testEqual(s, expected.function, actual.function);
        }
    }

    private void testEqual(CoordAxis2D expected, CoordAxis2D actual)
    {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);
        
        Assert.assertEquals(expected.getAxis1().getCtype(), actual.getAxis1().getCtype());
        Assert.assertEquals(expected.getAxis1().getCunit(), actual.getAxis1().getCunit());
        Assert.assertEquals(expected.getAxis2().getCtype(), actual.getAxis2().getCtype());
        Assert.assertEquals(expected.getAxis2().getCunit(), actual.getAxis2().getCunit());
        if (expected.error1 == null)
            Assert.assertNull("error1", actual.error1);
        else
        {
            Assert.assertNotNull(actual.error1);
            Assert.assertEquals("syser1", expected.error1.syser, actual.error1.syser);
            Assert.assertEquals("rnder1", expected.error1.rnder, actual.error1.rnder);
        }
        if (expected.error2 == null)
            Assert.assertNull("error2", actual.error2);
        else
        {
            Assert.assertNotNull(actual.error2);
            Assert.assertEquals("syser2", expected.error2.syser, actual.error2.syser);
            Assert.assertEquals("rnder2", expected.error2.rnder, actual.error2.rnder);
        }
        if (expected.range == null)
            Assert.assertNull("range", actual.range);
        else
        {
            Assert.assertNotNull(actual.range);
            UtilTest.testEqual(expected.range, actual.range);
        }
        if (expected.bounds == null)
            Assert.assertNull("bounds", actual.bounds);
        else
        {
            Assert.assertNotNull(actual.bounds);
            UtilTest.testEqual(expected.bounds, actual.bounds);
        }
        if (expected.function == null)
            Assert.assertNull("function", actual.function);
        else
        {
            Assert.assertNotNull(actual.function);
            UtilTest.testEqual(expected.function, actual.function);
        }
    }

    private Observation getTestObservation(boolean full, int depth, boolean comp, boolean sci)
        throws Exception
    {
        Observation o;
        if (comp)
        {
            CompositeObservation co = new CompositeObservation("TEST", "SimpleBar", new Algorithm("doit"));
            if (full)
            {
                co.getMembers().add(new ObservationURI("TEST", "simple1"));
                co.getMembers().add(new ObservationURI("TEST", "simple2"));
                co.getMembers().add(new ObservationURI("TEST", "simple3"));
            }
            o = co;
        }
        else
            o = new SimpleObservation("TEST", "SimpleBar");

        if (full)
        {
            if (sci)
                o.intent = ObservationIntentType.SCIENCE;
            else
            {
                o.intent = ObservationIntentType.CALIBRATION;
                o.type = "flat";
            }
            o.sequenceNumber = new Integer(123);
            o.metaRelease = TEST_DATE;

            o.proposal = new Proposal("MyFirstProposal");
            o.proposal.getKeywords().addAll(TEST_KEYWORDS);
            o.proposal.pi = "little old me";
            o.proposal.title = "My Little Pony";
            o.proposal.project = "Project 51";

            o.target = new Target("Pony 51");
            o.target.type = TargetType.OBJECT;
            o.target.getKeywords().addAll(TEST_KEYWORDS);
            o.target.standard = Boolean.TRUE;
            o.target.redshift = new Double(0.0);
            o.target.moving = Boolean.FALSE;

            o.targetPosition = new TargetPosition("FK5", new Point(1.0, 2.0));
            if (sci)
                o.targetPosition.equinox = 2000.0;
            
            o.requirements = new Requirements(Status.FAIL);
            
            o.telescope = new Telescope("BothEyes");
            o.telescope.getKeywords().addAll(TEST_KEYWORDS);
            o.telescope.geoLocationX = 100.0;
            o.telescope.geoLocationY = 200.0;
            o.telescope.geoLocationZ = 300.0;

            o.instrument = new Instrument("test-instrument");
            o.instrument.getKeywords().addAll(TEST_KEYWORDS);

            o.environment = new Environment();
            o.environment.seeing = 0.08;
            o.environment.photometric = Boolean.TRUE;
        }
        
        if (depth == 1)
            return o;

        o.getPlanes().add(getTestPlane(full, "thing1", depth, true));
        o.getPlanes().add(getTestPlane(full, "thing2", depth, false));
        Assert.assertEquals(2, o.getPlanes().size());
        
        return o;
    }

    protected Plane getTestPlane(boolean full, String productID, int depth, boolean poly)
        throws Exception
    {
        Plane p = new Plane(productID);
        if (full)
        {
            p.creatorID = URI.create("ivo://example.com/TEST?"+productID);
            p.calibrationLevel = CalibrationLevel.CALIBRATED;
            p.dataProductType = DataProductType.IMAGE;
            p.metaRelease = TEST_DATE;
            p.dataRelease = TEST_DATE;

            p.provenance = new Provenance("doit");
            p.provenance.lastExecuted = TEST_DATE;
            p.provenance.producer = "MyProducer";
            p.provenance.project = "MyProject";
            p.provenance.reference = new URI("http://www.example.com/MyProject/doit");
            p.provenance.runID = "RUNID123";
            p.provenance.version = "0.1alpha4";
            p.provenance.getKeywords().addAll(TEST_KEYWORDS);
            p.provenance.getInputs().add(new PlaneURI(new ObservationURI("FOO", "bar"), "in1"));
            p.provenance.getInputs().add(new PlaneURI(new ObservationURI("FOO", "bar"), "in2"));
        

            p.metrics = new Metrics();
            p.metrics.sourceNumberDensity = 100.0;
            p.metrics.background = 2.7;
            p.metrics.backgroundStddev = 0.3;
            p.metrics.fluxDensityLimit = null;
            p.metrics.magLimit = null;
            
            p.quality = new DataQuality(Quality.JUNK);
            
            // previously was computed metadata
            p.energy = new Energy();
            p.energy.bandpassName = "V";
            p.energy.bounds = new Interval(400e-6, 900e-6);
            p.energy.bounds.getSamples().add(new SubInterval(400e-6, 500e-6));
            p.energy.bounds.getSamples().add(new SubInterval(800e-6, 900e-6));
            p.energy.dimension = 2l;
            p.energy.emBand = EnergyBand.OPTICAL;
            p.energy.resolvingPower = 2.0;
            p.energy.restwav = 600e-9;
            p.energy.sampleSize = 100e-6;
            p.energy.transition = new EnergyTransition("H", "alpha");

            p.polarization = new Polarization();
            p.polarization.dimension = 3l;
            p.polarization.states = new ArrayList<>();
            p.polarization.states.add(PolarizationState.I);
            p.polarization.states.add(PolarizationState.Q);
            p.polarization.states.add(PolarizationState.U);

            p.position = new Position();
            if (poly) {
                MultiPolygon mp = new MultiPolygon();
                mp.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
                mp.getVertices().add(new Vertex(1.0, 4.0, SegmentType.LINE));
                mp.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
                mp.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
                List<Point> points = new ArrayList<Point>();
                for (Vertex v : mp.getVertices())
                    if (!SegmentType.CLOSE.equals(v.getType()))
                        points.add(new Point(v.cval1, v.cval2));
                p.position.bounds = new Polygon(points, mp);
            } else {
                p.position.bounds = new Circle(new Point(0.0, 89.0), 2.0);
            }
            
            p.position.dimension = new Dimension2D(1024, 2048);
            p.position.resolution = 0.05;
            p.position.sampleSize = 0.025;
            p.position.timeDependent = false;

            p.time = new Time();
            p.time.bounds = new Interval(50000.25, 50000.75);
            p.time.bounds.getSamples().add(new SubInterval(50000.25, 50000.40));
            p.time.bounds.getSamples().add(new SubInterval(50000.50, 50000.75));
            p.time.dimension = 2l;
            p.time.exposure = 666.0;
            p.time.resolution = 0.5;
            p.time.sampleSize = 0.15;
        }
        if (depth <= 2)
            return p;

        p.getArtifacts().add(getTestArtifact(full, new URI("http://www.example.com/stuff/" + productID + "a"), depth));
        p.getArtifacts().add(getTestArtifact(full, new URI("http://www.example.com/stuff/" + productID + "b"), depth));
        Assert.assertEquals(2, p.getArtifacts().size());

        return p;
    }

    private Artifact getTestArtifact(boolean full, URI uri, int depth)
    {
        Artifact a = new Artifact(uri, ProductType.SCIENCE, ReleaseType.DATA);
        if (full)
        {
            a.contentType = "application/fits";
            a.contentLength = TEST_LONG;
            a.contentChecksum = URI.create("md5:fb696fe6e2fbb98dee340bd1e8811dcb");
        }
        
        if (depth <= 3)
            return a;

        a.getParts().add(getTestPart(full, new Integer(1), depth));
        a.getParts().add(getTestPart(full, "A", depth));
        Assert.assertEquals(2, a.getParts().size());

        return a;
    }

    private Part getTestPart(boolean full, Integer pnum, int depth)
    {
        Part p = new Part(pnum);
        if (full)
            p.productType = ProductType.SCIENCE;
        
        if (depth <= 4)
            return p;

        p.getChunks().add(getPosFunctionChunk());
        p.getChunks().add(getEmptyChunk());
        p.getChunks().add(getObservableChunk());

        return p;
    }
    private Part getTestPart(boolean full, String pname, int depth)
    {
        Part p = new Part(pname);
        if (full)
            p.productType = ProductType.SCIENCE;

        if (depth <= 4)
            return p;

        p.getChunks().add(getPosRangeChunk());
        p.getChunks().add(getSpecChunk());
        p.getChunks().add(getPolarChunk());

        return p;
    }

    private Chunk getPosFunctionChunk()
    {
        Chunk c = new Chunk();
        c.positionAxis1 = new Integer(1);
        c.positionAxis2 = new Integer(2);
        c.position = new SpatialWCS(new CoordAxis2D(new Axis("RA---TAN", "deg"), new Axis("DEC--TAN", "deg")));
        c.position.coordsys = "FK5";
        c.position.equinox = 2000.0;

        // rectangle centered at 10,20
        Coord2D ref = new Coord2D(new RefCoord(512.0, 10.0), new RefCoord(1024, 20.0));
        Dimension2D dim = new Dimension2D(1024, 2048);
        c.position.getAxis().function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 1.0e-3); // approx 1x2 deg

        c.time = getTemporalWCS(false);
        
        return c;
    }

    private TemporalWCS getTemporalWCS(boolean offset)
    {
        TemporalWCS t = new TemporalWCS(new CoordAxis1D(new Axis("TIME", "d")));
        t.timesys = "UTC";
        t.trefpos = "TOPOCENTER";
        if (offset)
            t.mjdref = 50000.0;
        t.exposure = new Double(0.05*86400.0);
        t.resolution = 0.1;

        double a = 1234.0;
        if (!offset)
            a += 50000.0; // absolute

        t.getAxis().range = new CoordRange1D(new RefCoord(0.5, a), new RefCoord(1.5, a+0.1));

        t.getAxis().bounds = new CoordBounds1D();
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a), new RefCoord(1.5, a+0.02)));
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a+0.04), new RefCoord(1.5, a+0.06)));
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a+0.09), new RefCoord(1.5, a+0.10)));
        
        return t;
    }

    private Chunk getPosRangeChunk()
    {
        Chunk c = new Chunk();
        c.positionAxis1 = new Integer(1);
        c.positionAxis2 = new Integer(2);
        c.position = new SpatialWCS(new CoordAxis2D(new Axis("RA", "deg"), new Axis("DEC", "deg")));

        // rectangle from 10,20 to 12,22
        Coord2D c1 = new Coord2D(new RefCoord(0.5, 10.0), new RefCoord(0.5, 20.0));
        Coord2D c2 = new Coord2D(new RefCoord(1024.5, 20.0), new RefCoord(2048.5, 22.0));
        c.position.getAxis().range = new CoordRange2D(c1, c2);

        c.time = getTemporalWCS(true);

        return c;
    }

    private Chunk getSpecChunk()
    {
        Chunk c = new Chunk();
        c.energyAxis = new Integer(1);
        c.energy = new SpectralWCS(new CoordAxis1D(new Axis("WAVE", "m")), "TOPOCENT");

        c.energy.ssysobs = "TOPOCENT";
        c.energy.ssyssrc = "TOPOCENT";
        c.energy.restwav = 1.0e-6;
        c.energy.resolvingPower = 50000.0;

        RefCoord c1 = new RefCoord(0.5, 300.0e-9);
        RefCoord c2 = new RefCoord(1024.0, 450.0e-9);
        RefCoord c3 = new RefCoord(2048.5, 600.0e-9);
        c.energy.getAxis().range = new CoordRange1D(c1, c3);

        c.energy.getAxis().bounds = new CoordBounds1D();
        c.energy.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        c.energy.getAxis().bounds.getSamples().add(new CoordRange1D(c2, c3));

        c.energy.getAxis().function = new CoordFunction1D(1024L, (c3.val - c1.val)/1024.0,  c1);
        
        return c;
    }
    private Chunk getPolarChunk()
    {
        Chunk c = new Chunk();
        c.polarizationAxis = new Integer(1);
        c.polarization = new PolarizationWCS(new CoordAxis1D(new Axis("STOKES", null)));
        c.polarization.getAxis().function = new CoordFunction1D(3L, 1.0, new RefCoord(1.0, 1.0)); // I Q U
        return c;
    }
    
    private Chunk getEmptyChunk()
    {
        Chunk c = new Chunk();

        return c;
    }
    private Chunk getObservableChunk()
    {
        Chunk c = new Chunk();
        c.observableAxis = new Integer(1);
        c.observable = new ObservableAxis(new Slice(new Axis("flux", "J"),new Long(3L)));
        c.observable.independent = new Slice(new Axis("WAV", "um"),new Long(4L));
        return c;
    }

    private void print(Observation o)
    {
        log.warn(o.toString());
        Iterator<Plane> pi = o.getPlanes().iterator();
        while ( pi.hasNext() )
        {
            Plane p = pi.next();
            log.warn("\t" + p);
            Iterator<Artifact> ai = p.getArtifacts().iterator();
            while (ai.hasNext())
            {
                Artifact a = ai.next();
                log.warn("\t\t" + a);
                Iterator<Part> pti = a.getParts().iterator();
                while ( pti.hasNext() )
                {
                    Part pt = pti.next();
                    log.warn("\t\t\t" + pt);
                    Iterator ci = pt.getChunks().iterator();
                    while ( ci.hasNext() )
                        log.warn("\t\t\t\t" + ci.next());
                }
            }
        }
    }
}
