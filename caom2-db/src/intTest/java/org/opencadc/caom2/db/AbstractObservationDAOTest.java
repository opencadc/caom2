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

package org.opencadc.caom2.db;

import org.opencadc.caom2.Algorithm;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.ArtifactDescription;
import org.opencadc.caom2.CalibrationLevel;
import org.opencadc.caom2.CaomEntity;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.DeletedArtifactDescriptionEvent;
import org.opencadc.caom2.DeletedObservationEvent;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.EnergyBand;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.PolarizationState;
import org.opencadc.caom2.SimpleObservation;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.util.ObservationState;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordAxis2D;
import org.opencadc.caom2.wcs.ObservableAxis;
import org.opencadc.caom2.wcs.Slice;
import org.opencadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.IntRowMapper;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base class for all DAO tests. Subclasses must call the protected ctor to setup
 * server, database, schema, and some options.
 *
 * @author pdowler
 */
public abstract class AbstractObservationDAOTest {

    protected static Logger log;

    // round-trip timestamp comparisons must not be more lossy than this
    public static final long TIME_TOLERANCE = 0L;

    public static final Long TEST_LONG = 123456789L;
    public static final List<String> TEST_KEYWORDS = new ArrayList<String>();
    public static Date TEST_DATE;

    static {
        TEST_KEYWORDS.add("abc");
        TEST_KEYWORDS.add("x=1");
        TEST_KEYWORDS.add("foo=bar");
        TEST_KEYWORDS.add("foo:42");
        try {
            TEST_DATE = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC).parse("1999-01-02 12:13:14.567");
        } catch (Exception oops) {
            log.error("BUG", oops);
        }
        Log4jInit.setLevel("org.opencadc.caom2.db", Level.INFO);
    }

    ObservationDAO dao;

    static Class[] ENTITY_CLASSES = new Class[] {
        // including join tables before FK targets
        ObservationMember.class, ProvenanceInput.class, 
        Chunk.class, Part.class, Artifact.class, Plane.class, Observation.class, ArtifactDescription.class,
        DeletedObservationEvent.class, DeletedArtifactDescriptionEvent.class
    };

    protected AbstractObservationDAOTest(Class genClass, String server, String database, String schema)
            throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
            DBUtil.createJNDIDataSource("jdbc/caom2-db-test", cc);
            
            Map<String, Object> config = new TreeMap<String, Object>();
            config.put("jndiDataSourceName", "jdbc/caom2-db-test");
            //config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            this.dao = new ObservationDAO(true);
            dao.setConfig(config);
        } catch (Exception ex) {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    @Before
    public void setup()
            throws Exception {
        log.info("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        for (Class c : ENTITY_CLASSES) {
            String cn = c.getSimpleName();
            String s = gen.getTable(c);

            String sql = "delete from " + s;
            log.info("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
        }
        log.info("clearing old tables... OK");
    }

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNestedTransaction() {
        // the ObservationDAO class uses a txn inside the put and delete methods
        // this verifies that it it fails and does a rollback that an outer txn
        // can still proceed -- eg it is really nested
        try {
            final Observation obs1 = new SimpleObservation("FOO", URI.create("caom:FOO/bar1"), SimpleObservation.EXPOSURE);
            final Observation dupe = new SimpleObservation("FOO", URI.create("caom:FOO/bar1"), SimpleObservation.EXPOSURE);
            final Observation obs2 = new SimpleObservation("FOO", URI.create("caom:FOO/bar2"), SimpleObservation.EXPOSURE);

            log.info("put: " + obs1);
            dao.put(obs1);
            Assert.assertNotNull(dao.get(obs1.getID()));
            log.info("put: " + obs1 + " [OK]");

            log.info("start outer");
            TransactionManager txn = dao.getTransactionManager();
            txn.startTransaction(); // outer txn
            try {
                log.info("put dupe: " + dupe);
                dao.put(dupe); // nested txn
                Assert.fail("expected exception, successfully put duplicate observation");
            } catch (DuplicateKeyException expected) {
                log.info("caught expected: " + expected);
            }

            log.info("put: " + obs2);
            dao.put(obs2); // another nested txn
            log.info("put: " + obs2 + " [OK]");
            Assert.assertNotNull(dao.get(obs2.getID()));

            log.info("commit outer");
            txn.commitTransaction(); // outer txn
            log.info("commit outer [OK]");

            Observation check1 = dao.get(obs1.getID());
            Assert.assertNotNull(check1);
            Assert.assertEquals(obs1.getURI(), check1.getURI());

            Observation check2 = dao.get(obs2.getID());
            Assert.assertNotNull(check2);
            Assert.assertEquals(obs2.getURI(), check2.getURI());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetState() {
        try {
            String collection = "FOO";

            Observation obs = new SimpleObservation("FOO", URI.create("caom:FOO/bar1"), SimpleObservation.EXPOSURE);
            dao.put(obs);
            ObservationState o = dao.getState(obs.getID());
            Assert.assertNotNull(o);
            log.info("state-by-id: " + o);

            ObservationState o2 = dao.getState(obs.getURI());
            Assert.assertNotNull(o);
            log.info("state-by-uri: " + o);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    /*
    @Test
    public void testGetObservationStateList() {
        try {
            String collection = "FOO";

            Thread.sleep(10);

            Observation obs = new SimpleObservation(collection, "bar1");
            dao.put(obs);
            ObservationState o = dao.getState(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date start = new Date(o.getMaxLastModified().getTime() - 2 * TIME_TOLERANCE); // before 1
            Thread.sleep(10);

            obs = new SimpleObservation(collection, "bar2");
            dao.put(obs);
            o = dao.getState(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date mid = new Date(o.getMaxLastModified().getTime() + 2 * TIME_TOLERANCE); // after 2
            Thread.sleep(10);

            obs = new SimpleObservation(collection, "bar3");
            dao.put(obs);
            Assert.assertNotNull(dao.getState(obs.getURI()));
            log.info("created: " + obs);
            Thread.sleep(10);

            obs = new SimpleObservation(collection, "bar4");
            dao.put(obs);
            o = dao.getState(obs.getID());
            Assert.assertNotNull(o);
            log.info("created: " + o);
            Date end = new Date(o.getMaxLastModified().getTime() + 2 * TIME_TOLERANCE); // after 4
            Thread.sleep(10);

            Integer batchSize = 100;
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

            List<ObservationState> result = dao.getObservationList(collection, start, end, batchSize);
            for (int i = 0; i < result.size(); i++) {
                ObservationState os = result.get(i);
                log.info("found: " + df.format(os.maxLastModified) + " " + os);
                ObservationURI exp = new ObservationURI(collection, "bar" + (i + 1)); // 1 2 3 4
                Assert.assertEquals(exp, os.getURI());
            }
            Assert.assertEquals("start-end", 4, result.size());

            result = dao.getObservationList(collection, start, end, batchSize, false); // descending order
            for (int i = 0; i < result.size(); i++) {
                ObservationState os = result.get(i);
                log.info("start-end found: " + df.format(os.maxLastModified) + " " + os);
                ObservationURI exp = new ObservationURI(collection, "bar" + (4 - i)); // 4 3 2 1
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

            try {
                result = dao.getObservationList(null, start, end, batchSize);
                Assert.fail("expected IllegalArgumentException for null collection, got results");
            } catch (IllegalArgumentException ex) {
                log.info("caught expected exception: " + ex);
            }

            result = dao.getObservationList(collection, null, end, batchSize);
            Assert.assertEquals("-end", 4, result.size());

            result = dao.getObservationList(collection, start, null, batchSize);
            Assert.assertEquals("start-", 4, result.size());

            result = dao.getObservationList(collection, null, null, batchSize);

            result = dao.getObservationList(collection, start, null, null);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */
    
    /*
    @Test
    public void testGetObservationListByCollection() {
        try {
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
            Assert.assertNotNull(dao.getState(obs.getURI()));
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
            for (ObservationResponse os : result) {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(4, result.size());

            result = dao.getList(collection, start, mid, batchSize);
            for (ObservationResponse os : result) {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(2, result.size());

            result = dao.getList(collection, mid, end, batchSize);
            for (ObservationResponse os : result) {
                log.info("found: " + os);
                Assert.assertNotNull(os.observationState);
                Assert.assertNotNull(os.observation);
            }
            Assert.assertEquals(2, result.size());

            try {
                String str = null;
                result = dao.getList(str, start, end, batchSize);
                Assert.fail("expected IllegalArgumentException for null collection, got results");
            } catch (IllegalArgumentException ex) {
                log.info("caught expected exception: " + ex);
            }

            result = dao.getList(collection, null, end, batchSize);
            Assert.assertEquals(4, result.size());

            result = dao.getList(collection, start, null, batchSize);
            Assert.assertEquals(4, result.size());

            result = dao.getList(collection, null, null, batchSize);

            result = dao.getList(collection, start, null, null);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */

    @Test
    public void testGetDeleteNonExistentObservation() {
        try {
            URI uri = new URI("caom:TEST/NonExistentObservation");
            
            Observation notFound;
            
            notFound = dao.get(uri);
            Assert.assertNull(uri.toString(), notFound);

            UUID uuid = UUID.randomUUID();
            notFound = dao.get(uuid);
            Assert.assertNull(uuid.toString(), notFound);

            // should return without failing
            dao.delete(uuid);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutGetDeleteSimple() {
        SimpleObservation o = new SimpleObservation("TEST", URI.create("caom:TEST/foo"), SimpleObservation.EXPOSURE);
        testPutGetDelete(o);
    }

    @Test
    public void testPutGetDeleteDerived() {
        DerivedObservation o = new DerivedObservation("TEST", URI.create("caom:TEST/foo"), new Algorithm("stacked"));
        o.getMembers().add(URI.create("caom:TEST/foo1"));
        o.getMembers().add(URI.create("caom:TEST/foo2"));
        testPutGetDelete(o);
    }

    private void testPutGetDelete(Observation orig) {
        try {
            // !EXISTS
            Assert.assertNull(dao.get(orig.getID()));
            Assert.assertNull(dao.get(orig.getURI()));
            log.info("not-exists");

            // PUT
            dao.put(orig);
            Assert.assertNotNull("side effect", orig.getMetaChecksum());
            Assert.assertNotNull("side effect", orig.getAccMetaChecksum());
            Assert.assertNotNull("side effect", orig.getLastModified());
            Assert.assertNotNull("side effect", orig.getMaxLastModified());
            
            // this is so we can detect incorrect timestamp round trips
            // caused by assigning something other than what was stored
            // and assigned to orig as a side effect
            Thread.sleep(2 * TIME_TOLERANCE);

            log.info("put + sleep");

            // GET by ID
            Observation oid = dao.get(orig.getID());
            Assert.assertNotNull("found by ID", oid);
            log.info("retrieved by UUID");
            testEqual(orig, oid);
            
            // GET by URI
            Observation ouri = dao.get(orig.getURI());
            log.info("found: " + ouri);
            Assert.assertNotNull("found by URI", ouri);
            testEqual(orig, ouri);

            // DELETE by ID
            dao.delete(orig.getID());

            // !EXISTS
            Assert.assertNull(dao.get(orig.getID()));
            log.info("delete & not-exists");

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNonOriginPut() {
        try {
            dao.setOrigin(false);
            Observation orig = new SimpleObservation("TEST", new URI("caom:TEST/foo"), SimpleObservation.EXPOSURE);
            final UUID externalID = orig.getID();

            // assign last modified timestamps
            final Date externalLastModified = new Date(System.currentTimeMillis() - 10000L); // past
            CaomUtil.assignLastModified(orig, externalLastModified, "lastModified");
            CaomUtil.assignLastModified(orig, externalLastModified, "maxLastModified");
                       
            // !EXISTS
            Assert.assertNull(dao.get(orig.getID()));
            Assert.assertNull(dao.get(orig.getURI()));

            // PUT
            dao.put(orig);

            // this is so we can detect incorrect timestamp round trips
            // caused by assigning something other than what was stored
            Thread.sleep(2 * TIME_TOLERANCE);

            // GET by ID
            Observation retrieved = dao.get(orig.getID());
            Assert.assertNotNull("found by ID", retrieved);
            testEqual(orig, retrieved);

            // non-origin: make sure UUID did not change
            Assert.assertEquals("non-origin UUID", externalID, retrieved.getID());
            // make sure timestamps did not change
            Assert.assertEquals("non-origin lastModified", externalLastModified, retrieved.getLastModified());
            Assert.assertEquals("non-origin lastModified", externalLastModified, retrieved.getMaxLastModified());
            
            // DELETE by ID
            dao.delete(orig.getID());

            // !EXISTS
            Assert.assertNull(dao.get(orig.getID()));
            log.info("delete & not-exists");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            dao.setOrigin(true);
        }
    }

    @Test
    public void testPutSimpleObservation() {
        try {
            int minDepth = 1;
            int maxDepth = 5;
            for (int i = minDepth; i <= maxDepth; i++) {
                log.info("testPutSimpleObservation: depth=" + i);
                Observation orig = getTestObservation(i, true);
                for (Plane p : orig.getPlanes()) {
                    p.publisherID = p.getURI();
                }
                dao.put(orig);

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                Observation retrieved = dao.get(orig.getURI());
                Assert.assertNotNull("found", retrieved);
                testEqual(orig, retrieved);

                dao.delete(orig.getID());

                Observation deleted = dao.get(orig.getURI());
                Assert.assertNull("deleted", deleted);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutDerivedObservation() {
        try {
            int minDepth = 1;
            int maxDepth = 5;
            for (int i = minDepth; i <= maxDepth; i++) {
                log.info("testPutCompositeObservation: depth=" + i);
                Observation orig = getTestObservation(i, false);
                for (Plane p : orig.getPlanes()) {
                    p.publisherID = p.getURI();
                }
                dao.put(orig);

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                Observation retrieved = dao.get(orig.getURI());
                Assert.assertNotNull("found", retrieved);
                testEqual(orig, retrieved);

                dao.delete(orig.getID());

                Observation deleted = dao.get(orig.getID());
                Assert.assertNull("deleted", deleted);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateSimpleObservation() {
        try {
            int minDepth = 1;
            int maxDepth = 5;
            boolean full = true;
            for (int i = minDepth; i <= maxDepth; i++) {
                log.info("testUpdateSimpleObservation: depth=" + i);
                Observation orig = getTestObservation(i, true);
                for (Plane p : orig.getPlanes()) {
                    p.publisherID = p.getURI();
                }
                //txnManager.startTransaction();
                dao.put(orig);
                //txnManager.commitTransaction();

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                Observation ret1 = dao.get(orig.getURI());

                Assert.assertNotNull("found", ret1);
                log.info("testUpdateSimpleObservation/created: orig vs ret1");
                testEqual(orig, ret1);

                // the lastModified timestamps are maintained by the DAO, so lets set them to null here
                CaomUtil.assignLastModified(ret1, null, "lastModified");
                for (Plane p : ret1.getPlanes()) {
                    CaomUtil.assignLastModified(p, null, "lastModified");
                    for (Artifact a : p.getArtifacts()) {
                        CaomUtil.assignLastModified(a, null, "lastModified");
                        for (Part pa : a.getParts()) {
                            CaomUtil.assignLastModified(pa, null, "lastModified");
                            for (Chunk c : pa.getChunks()) {
                                CaomUtil.assignLastModified(c, null, "lastModified");
                            }
                        }
                    }
                }

                // now modify the objects
                ret1.proposal.getKeywords().add("something=new");
                if (i > 1) {
                    Plane p = ret1.getPlanes().iterator().next();
                    p.calibrationLevel = CalibrationLevel.PRODUCT;
                    if (i > 2) {
                        Artifact a = p.getArtifacts().iterator().next();
                        a.contentType = "application/foo";
                        a.contentLength = 123456789L;
                        if (i > 3) {
                            Part part = a.getParts().iterator().next();
                            part.productType = DataLinkSemantics.PREVIEW.PREVIEW;
                            if (i > 4) {
                                Chunk c = part.getChunks().iterator().next();
                                c.observable = new ObservableAxis(new Slice(new Axis("flux", "J"), new Long(2)));
                                c.observable.independent = new Slice(new Axis("wavelength", "nm"), new Long(1));
                            }
                        }
                    }
                }
                dao.put(ret1);

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                Observation ret2 = dao.get(orig.getURI());
                Assert.assertNotNull("found", ret2);
                log.info("testUpdateSimpleObservation/updated: ret1 vs ret2");
                testEqual(ret1, ret2);

                if (i > 1) {
                    // test setting lastModified on observation when it is only force-updated for maxLastModified
                    // the lastModified timestamps are maintained by the DAO, so lets set them to null here
                    CaomUtil.assignLastModified(ret1, null, "lastModified");
                    for (Plane p : ret1.getPlanes()) {
                        CaomUtil.assignLastModified(p, null, "lastModified");
                        for (Artifact a : p.getArtifacts()) {
                            CaomUtil.assignLastModified(a, null, "lastModified");
                            for (Part pa : a.getParts()) {
                                CaomUtil.assignLastModified(pa, null, "lastModified");
                                for (Chunk c : pa.getChunks()) {
                                    CaomUtil.assignLastModified(c, null, "lastModified");
                                }
                            }
                        }
                    }
                    Plane pp = ret1.getPlanes().iterator().next();
                    pp.calibrationLevel = CalibrationLevel.RAW_INSTRUMENTAL;
                    dao.put(ret1);

                    // this is so we can detect incorrect timestamp round trips
                    // caused by assigning something other than what was stored
                    Thread.sleep(2 * TIME_TOLERANCE);

                    Observation ret3 = dao.get(orig.getURI());
                    Assert.assertNotNull("found", ret3);

                    // make sure the DAO assigned lastModified values everywhere
                    Assert.assertNotNull(ret1.getLastModified());
                    for (Plane p : ret1.getPlanes()) {
                        Assert.assertNotNull(p.getLastModified());
                        for (Artifact a : p.getArtifacts()) {
                            Assert.assertNotNull(a.getLastModified());
                            for (Part pa : a.getParts()) {
                                Assert.assertNotNull(pa.getLastModified());
                                for (Chunk c : pa.getChunks()) {
                                    Assert.assertNotNull(c.getLastModified());
                                }
                            }
                        }
                    }

                    log.info("testUpdateSimpleObservation/updated-timestamps: ret1 vs ret3");
                    testEqual(ret1, ret3); // this makes sure the lastModified values assigned in the put match the ones in the DB
                }

                dao.delete(orig.getID());

                Observation deleted = dao.get(orig.getID());
                Assert.assertNull("deleted", deleted);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen())
                try {
                txnManager.rollbackTransaction();
            } catch (Throwable t) {
                log.error("failed to rollback transaction", t);
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateSimpleObservationAddRemovePlane() {
        try {
            int minDepth = 1;
            int maxDepth = 5;
            boolean full = true;
            for (int i = minDepth; i <= maxDepth; i++) {
                log.info("testUpdateSimpleObservationAddRemovePlane: full=" + full + ", depth=" + i);
                Observation orig = getTestObservation(i, true);
                for (Plane p : orig.getPlanes()) {
                    p.publisherID = p.getURI();
                }
                final int numPlanes = orig.getPlanes().size();

                log.debug("put: orig");
                //txnManager.startTransaction();
                dao.put(orig);
                //txnManager.commitTransaction();
                log.debug("put: orig DONE");

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                log.debug("get: orig");
                Observation ret1 = dao.get(orig.getID());
                log.debug("get: orig DONE");
                Assert.assertNotNull("found", ret1);
                Assert.assertEquals(numPlanes, ret1.getPlanes().size());
                testEqual(orig, ret1);

                Plane newPlane = new Plane(new URI(orig.getURI().toASCIIString() + "/testPlane2"));
                newPlane.publisherID = newPlane.getURI();
                ret1.getPlanes().add(newPlane);

                log.debug("put: added");
                //txnManager.startTransaction();
                dao.put(ret1);
                //txnManager.commitTransaction();
                log.debug("put: added DONE");

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                log.debug("get: added");
                Observation ret2 = dao.get(orig.getID());
                log.debug("get: added DONE");
                Assert.assertNotNull("found", ret2);
                Assert.assertEquals(numPlanes + 1, ret1.getPlanes().size());
                testEqual(ret1, ret2);

                ret2.getPlanes().remove(newPlane);

                log.debug("put: removed");
                //txnManager.startTransaction();
                dao.put(ret2);
                //txnManager.commitTransaction();
                log.debug("put: removed DONE");

                // this is so we can detect incorrect timestamp round trips
                // caused by assigning something other than what was stored
                Thread.sleep(2 * TIME_TOLERANCE);

                log.debug("get: removed");
                Observation ret3 = dao.get(orig.getID());
                log.debug("get: removed DONE");
                Assert.assertNotNull("found", ret3);
                Assert.assertEquals(numPlanes, ret3.getPlanes().size());
                testEqual(orig, ret3);

                //txnManager.startTransaction();
                dao.delete(orig.getID());
                //txnManager.commitTransaction();

                Observation deleted = dao.get(orig.getID());
                Assert.assertNull("deleted", deleted);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen())
                try {
                txnManager.rollbackTransaction();
            } catch (Throwable t) {
                log.error("failed to rollback transaction", t);
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateDerivedToSimple() {
        try {
            DerivedObservation comp = new DerivedObservation("FOO", URI.create("caom:FOO/bar"), new Algorithm("baz"));
            comp.getMembers().add(URI.create("caom:FOO/part"));
            log.debug("put: comp");
            dao.put(comp);
            log.debug("put: comp DONE");

            String sql = "SELECT count(*) from " + dao.gen.getTable(ObservationMember.class)
                    + " WHERE parentID = " + dao.gen.literal(comp.getID());
            JdbcTemplate jdbc = new JdbcTemplate(dao.dataSource);

            if (dao.gen.persistOptimisations) {
                int compMembers = jdbc.queryForObject(sql, new IntRowMapper());
                log.info("composite members: " + compMembers);
                Assert.assertEquals("one compMember", 1, compMembers);
            }

            // this is so we can detect incorrect timestamp round trips
            // caused by assigning something other than what was stored
            Thread.sleep(2 * TIME_TOLERANCE);

            log.debug("get: comp");
            Observation c = dao.get(comp.getID());
            log.debug("get: comp DONE");
            Assert.assertNotNull("found", c);
            Assert.assertEquals("composite", DerivedObservation.class, c.getClass());
            testEqual(comp, c);
            log.info("verified: " + c);

            Observation simp = new SimpleObservation(comp.getCollection(), comp.getURI(), SimpleObservation.EXPOSURE);
            CaomUtil.assignID(simp, comp.getID());

            log.debug("put: simp");
            dao.put(simp);
            log.debug("put: simp DONE");

            log.debug("get: simp");
            Observation s = dao.get(simp.getID());
            log.debug("get: comp DONE");
            Assert.assertNotNull("found", s);
            Assert.assertEquals("simple", simp.getClass(), s.getClass());
            testEqual(simp, s);
            log.info("verified: " + s);

            log.info("update: " + comp.getID() + " == " + simp.getID());
            Assert.assertEquals("single UUID -- put was an update", comp.getID(), simp.getID());

            if (dao.gen.persistOptimisations) {
                int simpMembers = jdbc.queryForObject(sql, new IntRowMapper());
                log.info("simple members: " + simpMembers);
                Assert.assertEquals("no simpMembers", 0, simpMembers);
            }

            dao.delete(simp.getID());

            Observation deleted = dao.get(simp.getID());
            Assert.assertNull("deleted", deleted);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testConditionalUpdate() {
        try {
            SimpleObservation orig = new SimpleObservation("FOO", "bar");
            dao.put(orig);

            Observation c = dao.get(orig.getURI());
            Assert.assertNotNull("found", c);
            testEqual(orig, c);

            URI cs1 = c.getAccMetaChecksum();
            c.sequenceNumber = 1;
            dao.put(c, cs1);
            Observation c2 = dao.get(c.getURI());
            Assert.assertNotNull("found", c2);
            URI cs2 = c.getAccMetaChecksum();

            // conditional update fails when expected checksum does not match
            c.type = "OBJECT";
            try {
                dao.put(c, cs1);
            } catch (PreconditionFailedException expected) {
                log.info("caught expected exception: " + expected);
            }

            // conditional update succeeds when expected checksum does match
            dao.put(c, cs2);
            Observation c3 = dao.get(c.getURI());
            Assert.assertNotNull("found", c3);
            URI cs3 = c.getAccMetaChecksum();

            dao.delete(orig.getID());

            // copnditional update fails when not found
            try {
                dao.put(c, cs3);
            } catch (PreconditionFailedException expected) {
                log.info("caught expected exception: " + expected);
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen())
                try {
                txnManager.rollbackTransaction();
            } catch (Throwable t) {
                log.error("failed to rollback transaction", t);
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    /*
    @Test
    public void testGetObservationList() {
        try {
            log.info("testGetObservationList");
            Integer batchSize = new Integer(3);

            String collection = "FOO";
            Observation o1 = new SimpleObservation(collection, "obs1");
            Observation o2 = new SimpleObservation(collection, "obsA");
            Observation o3 = new SimpleObservation(collection, "obs2");
            Observation o4 = new SimpleObservation(collection, "obsB");
            Observation o5 = new SimpleObservation(collection, "obs3");

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

            List<ObservationResponse> obs;

            // get first batch
            obs = dao.getList(collection, null, null, batchSize);
            Assert.assertNotNull(obs);
            Assert.assertEquals(3, obs.size());
            Assert.assertEquals(o1.getURI(), obs.get(0).observation.getURI());
            Assert.assertEquals(o2.getURI(), obs.get(1).observation.getURI());
            Assert.assertEquals(o3.getURI(), obs.get(2).observation.getURI());

            // get next batch
            obs = dao.getList(collection, o3.getMaxLastModified(), null, batchSize);
            Assert.assertNotNull(obs);
            Assert.assertEquals(3, obs.size()); // o3 gets picked up by the >=
            Assert.assertEquals(o3.getURI(), obs.get(0).observation.getURI());
            Assert.assertEquals(o4.getURI(), obs.get(1).observation.getURI());
            Assert.assertEquals(o5.getURI(), obs.get(2).observation.getURI());

            //txnManager.startTransaction();
            dao.delete(o1.getURI());
            dao.delete(o2.getURI());
            dao.delete(o3.getURI());
            dao.delete(o4.getURI());
            dao.delete(o5.getURI());
            //txnManager.commitTransaction();

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            if (txnManager.isOpen())
                try {
                txnManager.rollbackTransaction();
            } catch (Throwable t) {
                log.error("failed to rollback transaction", t);
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */
    
    @Test
    public void testPutObservationDeleteChildren() {
        try {
            log.info("testPutObservationDeleteChildren");
            Observation orig = getTestObservation(3, true);
            for (Plane p : orig.getPlanes()) {
                p.publisherID = p.getURI();
            }
            
            dao.put(orig);

            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found", retrieved);
            log.info("retrieved: " + retrieved.getPlanes().size());
            testEqual(orig, retrieved);

            Plane rem = orig.getPlanes().iterator().next();
            orig.getPlanes().remove(rem);

            dao.put(orig);

            Observation smaller = dao.get(orig.getURI());
            Assert.assertNotNull("found", smaller);
            log.info("smaller: " + smaller.getPlanes().size());
            testEqual(orig, smaller);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateMaxLastModified() {
        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        try {
            log.info("testUpdateMaxLastModified: origin=" + dao.origin);
            Observation orig = getTestObservation(2, true);
            for (Plane p : orig.getPlanes()) {
                p.publisherID = p.getURI();
            }

            dao.put(orig);

            Thread.sleep(10L);

            Observation obs0 = dao.get(orig.getID());
            testEqual(orig, obs0);
            //log.info("obs0: mcs " + obs0.getMetaChecksum() + " " + df.format(obs0.getLastModified()));
            //log.info("obs0: acc " + obs0.getAccMetaChecksum() + " " + df.format(obs0.getMaxLastModified()));
            final Date d0 = obs0.getMaxLastModified();

            for (Plane p : orig.getPlanes()) {
                p.calibrationLevel = CalibrationLevel.RAW_INSTRUMENTAL;
            }
            
            Thread.sleep(10L);

            dao.put(orig); // maxLastModified increases here due to changes in the planes
            
            final Observation obs1 = dao.get(orig.getID());
            //log.info("obs1: mcs " + obs1.getMetaChecksum() + " " + df.format(obs1.getLastModified()));
            //log.info("obs1: acc " + obs1.getAccMetaChecksum() + " " + df.format(obs1.getMaxLastModified()));
            final Date d1 = obs1.getMaxLastModified();

            Assert.assertNotEquals(df.format(d0), df.format(d1));
            Assert.assertTrue("maxLastModified increased from update", d1.after(d0));

            Thread.sleep(10L);

            Plane firstPlane = obs1.getPlanes().iterator().next();
            boolean del = obs1.getPlanes().remove(firstPlane);
            Assert.assertTrue("deleted plane", del);

            dao.put(obs1); // maxLastModified increases here due to delete of plane

            Observation obs2 = dao.get(orig.getID());
            //log.info("obs2: mcs " + obs2.getMetaChecksum() + " " + df.format(obs2.getLastModified()));
            //log.info("obs2: acc " + obs2.getAccMetaChecksum() + " " + df.format(obs2.getMaxLastModified()));
            final Date d2 = obs2.getMaxLastModified();
            
            Assert.assertEquals("num planes after delete", obs1.getPlanes().size(), obs2.getPlanes().size());
            Assert.assertNotEquals(df.format(d1), df.format(d2));
            Assert.assertTrue("maxLastModified increased from delete child", d2.after(d1));
            
            dao.delete(orig.getID());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            TransactionManager txnManager = dao.getTransactionManager();
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    /*
    @Test
    public void testUpdateOptimisationFields() {
        try {
            log.info("testUpdateOptimisationFields");
            Observation orig = getTestObservation(5, true);
            for (Plane p : orig.getPlanes()) {
                Assert.assertNull("original metaRelease is null", p.metaRelease); // verify starting state
            }
            dao.put(orig);

            Observation retrieved = dao.get(orig.getURI());
            Assert.assertNotNull("found", retrieved);
            testEqual(orig, retrieved);

            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 3600 * 1000L); // +1 hour

            for (Plane p : orig.getPlanes()) {
                p.metaRelease = d1; // assign value; change metaChecksum
            }
            dao.put(orig); // maxLastModified increases here due to changes in the planes

            Observation changed = dao.get(orig.getURI());
            Assert.assertNotNull("found", changed);
            testEqual(orig, changed);

            checkOptimizations(orig, d1);

            // update Plane.metaRelease to d2
            for (Plane p : orig.getPlanes()) {
                p.metaRelease = d2; // update value; change metaChecksum
            }
            dao.put(orig); // maxLastModified increases here due to changes in the planes

            // check for metaRelease in artifact, part, chunk = d2
            checkOptimizations(orig, d2);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            if (txnManager.isOpen()) {
                try {
                    txnManager.rollbackTransaction();
                } catch (Throwable t) {
                    log.error("failed to rollback transaction", t);
                }
            }
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */

    protected void checkOptimizations(Observation o, Date expectedMetaRelease) {
        log.info("checkOptimizations: no checks implemented");
    }

    // for comparing release dates: compare second
    protected void testEqualSeconds(String s, Date expected, Date actual) {
        if (expected == null) {
            Assert.assertNull(s, actual);
        } else {
            Assert.assertNotNull(s, actual);
            long esec = expected.getTime() / 1000L;
            long asec = actual.getTime() / 1000L;
            Assert.assertEquals(s, esec, asec);
        }
    }

    private void testEntity(CaomEntity expected, CaomEntity actual) {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);
        String cn = expected.getClass().getSimpleName();

        Assert.assertEquals(cn + ".ID", expected.getID(), actual.getID());
        if (expected.metaProducer == null) {
            Assert.assertNull(cn + ".metaProducer", actual.metaProducer);
        } else {
            Assert.assertEquals(cn + ".metaProducer", expected.metaProducer, actual.metaProducer);
        }
    }

    private void testEntityChecksums(CaomEntity expected, CaomEntity actual) {
        log.debug("testEqual: " + expected + " == " + actual);
        String cn = expected.getClass().getSimpleName();

        // read from database should always have checksums
        Assert.assertNotNull(cn + ".metaChecksum", actual.getMetaChecksum());
        Assert.assertNotNull(cn + ".accMetaChecksum", actual.getAccMetaChecksum());

        // all checksums are computed and assigned on expected as a side effect of calling put
        Assert.assertEquals(cn + ".metaChecksum", expected.getMetaChecksum(), actual.getMetaChecksum());
        Assert.assertEquals(cn + ".accMetaChecksum", expected.getAccMetaChecksum(), actual.getAccMetaChecksum());

        try {
            // above verifies that checksum was written to and read from database
            // below checks that all values included in the checksum were faithfully written/read
            // in case some other assert is not catching it - fail  means there is a bug in the
            // comparisons this should catch it
            URI mcs = actual.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(cn + " recomputed metaChecksum", actual.getMetaChecksum(), mcs);
        } catch (Exception ex) {
            log.error(cn + " failed to compute metaChecksum", ex);
            Assert.fail(cn + " failed to compute metaChecksum: " + ex);
        }

        //Assert.assertEquals(cn+".getStateCode", expected.getStateCode(), actual.getStateCode());
        testEqual(cn + ".lastModified", expected.getLastModified(), actual.getLastModified());

        // some tests rely on insert + add child + remove child and compare orig to final so 
        // maxLastModified changes
        //testEqual(cn+".maxLastModified", expected.getMaxLastModified(), actual.getMaxLastModified());
    }

    // for comparing lastModified: Sybase isn't reliable to ms accuracy when using UTC
    protected void testEqual(String s, Date expected, Date actual) {
        if (expected == null) {
            Assert.assertNull(s, actual);
        } else {
            Assert.assertNotNull(s, actual);
            long dt = Math.abs(expected.getTime() - actual.getTime());
            Assert.assertTrue(s + ": " + expected.getTime() + " vs " + actual.getTime(), (dt <= TIME_TOLERANCE));
        }
    }

    private void testEqual(Observation expected, Observation actual) {
        testEntity(expected, actual);
        Assert.assertEquals(expected.getURI(), actual.getURI());
        Assert.assertEquals(expected.getUriBucket(), actual.getUriBucket());
        Assert.assertEquals("algorithm.name", expected.getAlgorithm().getName(), actual.getAlgorithm().getName());

        testEqual("observation.metaRelease", expected.metaRelease, actual.metaRelease);
        testEqual("observation.metaReadGroups", expected.getMetaReadGroups(), actual.getMetaReadGroups());
        
        Assert.assertEquals("type", expected.type, actual.type);
        Assert.assertEquals("intent", expected.intent, actual.intent);
        Assert.assertEquals("sequenceNumber", expected.sequenceNumber, actual.sequenceNumber);

        

        if (expected.proposal != null) {
            Assert.assertEquals("proposal.id", expected.proposal.getID(), actual.proposal.getID());
            Assert.assertEquals("proposal.pi", expected.proposal.pi, actual.proposal.pi);
            Assert.assertEquals("proposal.project", expected.proposal.project, actual.proposal.project);
            Assert.assertEquals("proposal.title", expected.proposal.title, actual.proposal.title);
            testEqual("proposal.keywords", expected.proposal.getKeywords(), actual.proposal.getKeywords());
        } else {
            Assert.assertNull("proposal", actual.proposal);
        }
        if (expected.target != null) {
            Assert.assertNotNull("target", actual.target);
            Assert.assertEquals("target.name", expected.target.getName(), actual.target.getName());
            Assert.assertEquals("target.type", expected.target.type, actual.target.type);
            Assert.assertEquals("target.standard", expected.target.standard, actual.target.standard);
            Assert.assertEquals("target.redshift", expected.target.redshift, actual.target.redshift);
            testEqual("target.keywords", expected.target.getKeywords(), actual.target.getKeywords());
        } else {
            Assert.assertNull("target", actual.target);
        }

        if (expected.telescope != null) {
            Assert.assertNotNull("telescope", actual.telescope);
            Assert.assertEquals("telescope.name", expected.telescope.getName(), actual.telescope.getName());
            Assert.assertEquals("telescope.geoLocationX", expected.telescope.geoLocationX, actual.telescope.geoLocationX);
            Assert.assertEquals("telescope.geoLocationY", expected.telescope.geoLocationY, actual.telescope.geoLocationY);
            Assert.assertEquals("telescope.geoLocationZ", expected.telescope.geoLocationZ, actual.telescope.geoLocationZ);
            testEqual("telescope.keywords", expected.telescope.getKeywords(), actual.telescope.getKeywords());
        } else {
            Assert.assertNull("telescope", actual.telescope);
        }

        if (expected.instrument != null) {
            Assert.assertNotNull("instrument", actual.instrument);
            Assert.assertEquals("instrument.name", expected.instrument.getName(), actual.instrument.getName());
            testEqual("instrument.keywords", expected.instrument.getKeywords(), actual.instrument.getKeywords());
        } else {
            Assert.assertNull("instrument", actual.instrument);
        }

        if (expected.environment != null) {
            Assert.assertNotNull("environment", actual.environment);
            Assert.assertEquals("environment.seeing", expected.environment.seeing, actual.environment.seeing);
            Assert.assertEquals("environment.photometric", expected.environment.photometric, actual.environment.photometric);
            Assert.assertEquals("environment.humidity", expected.environment.humidity, actual.environment.humidity);
            Assert.assertEquals("environment.tau", expected.environment.tau, actual.environment.tau);
            Assert.assertEquals("environment.wavelengthTau", expected.environment.wavelengthTau, actual.environment.wavelengthTau);
        } else {
            Assert.assertNull("environment", actual.environment);
        }

        Assert.assertEquals("metaReadGroups.size", expected.getMetaReadGroups().size(), actual.getMetaReadGroups().size());
        Iterator<URI> eu = expected.getMetaReadGroups().iterator();
        Iterator<URI> au = actual.getMetaReadGroups().iterator();
        while (eu.hasNext() || au.hasNext()) {
            Assert.assertEquals(eu.next(), au.next());
        }

        log.debug("num planes: " + expected.getPlanes().size() + " == " + actual.getPlanes().size());
        Assert.assertEquals("number of planes", expected.getPlanes().size(), actual.getPlanes().size());
        Iterator<Plane> e = expected.getPlanes().iterator();
        Iterator<Plane> a = actual.getPlanes().iterator();
        while (e.hasNext() || a.hasNext()) {
            testEqual(e.next(), a.next());
        }

        testEntityChecksums(expected, actual);
    }

    private void testEqual(String name, Collection<String> expected, Collection<String> actual) {
        Assert.assertEquals(name, expected, actual);
    }

    private void testEqual(String name, Set<URI> expected, Set<URI> actual) {
        Assert.assertEquals(name, expected.size(), actual.size());
        Iterator<URI> ei = expected.iterator();
        Iterator<URI> ai = actual.iterator();
        while (ei.hasNext()) {
            URI e = ei.next();
            URI a = ai.next();
            Assert.assertEquals(name, e, a);
        }
    }
    
    private void testEqual(Plane expected, Plane actual) {
        testEntity(expected, actual);
        Assert.assertEquals("uri", expected.getURI(), actual.getURI());
        Assert.assertEquals("calibrationLevel", expected.calibrationLevel, actual.calibrationLevel);
        Assert.assertEquals("dataProductType", expected.dataProductType, actual.dataProductType);
        
        testEqual("plane.metaRelease", expected.metaRelease, actual.metaRelease);
        testEqual("plane.metaReadGroups", expected.getMetaReadGroups(), actual.getMetaReadGroups());
        
        testEqualSeconds("plane.dataRelease", expected.dataRelease, actual.dataRelease);
        testEqual("plane.dataReadGroups", expected.getDataReadGroups(), actual.getDataReadGroups());
        
        if (expected.provenance != null) {
            Assert.assertEquals("provenance.name", expected.provenance.getName(), actual.provenance.getName());
            Assert.assertEquals("provenance.reference", expected.provenance.reference, actual.provenance.reference);
            Assert.assertEquals("provenance.version", expected.provenance.version, actual.provenance.version);
            Assert.assertEquals("provenance.project", expected.provenance.project, actual.provenance.project);
            Assert.assertEquals("provenance.producer", expected.provenance.producer, actual.provenance.producer);
            Assert.assertEquals("provenance.runID", expected.provenance.runID, actual.provenance.runID);
            testEqualSeconds("provenance.lastExecuted", expected.provenance.lastExecuted, actual.provenance.lastExecuted);
            Assert.assertEquals("provenance.inputs", expected.provenance.getInputs(), actual.provenance.getInputs());
            testEqual("provenance.keywords", expected.provenance.getKeywords(), actual.provenance.getKeywords());
        } else {
            Assert.assertNull(actual.provenance);
        }

        if (expected.metrics != null) {
            Assert.assertEquals("metrics.sourceNumberDensity", expected.metrics.sourceNumberDensity, actual.metrics.sourceNumberDensity);
            Assert.assertEquals("metrics.background", expected.metrics.background, actual.metrics.background);
            Assert.assertEquals("metrics.backgroundStdde", expected.metrics.backgroundStddev, actual.metrics.backgroundStddev);
            Assert.assertEquals("metrics.fluxDensityLimit", expected.metrics.fluxDensityLimit, actual.metrics.fluxDensityLimit);
            Assert.assertEquals("metrics.magLimit", expected.metrics.magLimit, actual.metrics.magLimit);
        } else {
            Assert.assertNull(actual.metrics);
        }

        if (expected.position != null) {
            Assert.assertNotNull("plane.position", actual.position);
            if (expected.position.getBounds() != null && Polygon.class.equals(expected.position.getBounds().getClass())) {
                Polygon ep = (Polygon) expected.position.getBounds();
                Polygon ap = (Polygon) actual.position.getBounds();
                Assert.assertEquals("vertices.size", ep.getVertices().size(), ap.getVertices().size());
                for (int i = 0; i < ep.getVertices().size(); i++) {
                    Point ept = ep.getVertices().get(i);
                    Point apt = ap.getVertices().get(i);
                    Assert.assertEquals("point.long", ept.getLongitude(), apt.getLongitude(), 0.0);
                    Assert.assertEquals("point.lat", ept.getLatitude(), apt.getLatitude(), 0.0);
                }
            } else if (expected.position.getBounds() != null && Circle.class.equals(expected.position.getBounds().getClass())) {
                Circle ep = (Circle) expected.position.getBounds();
                Circle ap = (Circle) actual.position.getBounds();
                Assert.assertEquals("center.long", ep.getCenter().getLongitude(), ap.getCenter().getLongitude(), 0.0);
                Assert.assertEquals("center.lat", ep.getCenter().getLatitude(), ap.getCenter().getLatitude(), 0.0);
                Assert.assertEquals("radius", ep.getRadius(), ap.getRadius(), 0.0);
            } else {
                Assert.assertNull(actual.position.getBounds());
            }
            // TODO: position.samples
            if (expected.position.dimension != null) {
                Assert.assertEquals("position.dimension.naxis1", expected.position.dimension.naxis1, actual.position.dimension.naxis1);
                Assert.assertEquals("position.dimension.naxis2", expected.position.dimension.naxis2, actual.position.dimension.naxis2);
            } else {
                Assert.assertNull(actual.position.dimension);
            }
            Assert.assertEquals("position.resolution", expected.position.resolution, actual.position.resolution);
            Assert.assertEquals("position.sampleSize", expected.position.sampleSize, actual.position.sampleSize);
            // TODO: new fields
        }
        if (expected.energy != null) {
            Assert.assertNotNull("plane.energy", actual.energy);
            Assert.assertEquals("energy.bounds.lower", expected.energy.getBounds().getLower(), actual.energy.getBounds().getLower(), 0.0);
            Assert.assertEquals("energy.bounds.upper", expected.energy.getBounds().getUpper(), actual.energy.getBounds().getUpper(), 0.0);
            
            Assert.assertEquals("energy.samples.size", expected.energy.getSamples().size(), actual.energy.getSamples().size());
            for (int i = 0; i < expected.energy.getSamples().size(); i++) {
                DoubleInterval esi = expected.energy.getSamples().get(i);
                DoubleInterval asi = actual.energy.getSamples().get(i);
                Assert.assertEquals("sample.lb", esi.getLower(), asi.getLower(), 0.0);
                Assert.assertEquals("sample.ub", esi.getUpper(), asi.getUpper(), 0.0);
            }
            Assert.assertEquals("energy.bandpassName", expected.energy.bandpassName, actual.energy.bandpassName);
            Assert.assertEquals("energy.dimension", expected.energy.dimension, actual.energy.dimension);
            Iterator<EnergyBand> ee = expected.energy.getEnergyBands().iterator();
            Iterator<EnergyBand> ae = actual.energy.getEnergyBands().iterator();
            while (ee.hasNext()) {
                EnergyBand ex = ee.next();
                EnergyBand ac = ae.next();
                Assert.assertEquals("energy.energyBand", ex, ac);
            }
            Assert.assertEquals("energy.resolvingPower", expected.energy.resolvingPower, actual.energy.resolvingPower);
            Assert.assertEquals("energy.rest", expected.energy.rest, actual.energy.rest);
            Assert.assertEquals("energy.sampleSize", expected.energy.sampleSize, actual.energy.sampleSize);
            Assert.assertEquals("energy.transition", expected.energy.transition, actual.energy.transition);
            // TODO: new fields
        } else {
            Assert.assertNull(actual.energy);
        }
            
        if (expected.time != null) {
            Assert.assertNotNull("plane.time", actual.time);
            Assert.assertEquals("time.bounds.lower", expected.time.getBounds().getLower(), actual.time.getBounds().getLower(), 0.0);
            Assert.assertEquals("time.bounds.upper", expected.time.getBounds().getUpper(), actual.time.getBounds().getUpper(), 0.0);
            Assert.assertEquals("time.samples.size", expected.time.getSamples().size(), actual.time.getSamples().size());
            for (int i = 0; i < expected.time.getSamples().size(); i++) {
                DoubleInterval esi = expected.time.getSamples().get(i);
                DoubleInterval asi = actual.time.getSamples().get(i);
                Assert.assertEquals("SubInterval.lb", esi.getLower(), asi.getLower(), 0.0);
                Assert.assertEquals("SubInterval.ub", esi.getUpper(), asi.getUpper(), 0.0);
            }
            
            Assert.assertEquals("time.dimension", expected.time.dimension, actual.time.dimension);
            Assert.assertEquals("time.exposure", expected.time.exposure, actual.time.exposure);
            Assert.assertEquals("time.resolution", expected.time.resolution, actual.time.resolution);
            Assert.assertEquals("time.sampleSize", expected.time.sampleSize, actual.time.sampleSize);
        } else {
            Assert.assertNull(actual.time);
        }
        
        if (expected.polarization != null) {
            Assert.assertNotNull("plane.polarization", actual.polarization);
            Assert.assertNotNull("polarization.states", actual.polarization.getStates());
            Assert.assertEquals("polarization.states.size", expected.polarization.getStates().size(), actual.polarization.getStates().size());
            Iterator<PolarizationState> ei = expected.polarization.getStates().iterator();
            Iterator<PolarizationState> ai = actual.polarization.getStates().iterator();
            while (ei.hasNext()) {
                Assert.assertEquals("polarization.state", ei.next(), ai.next());
            }
            Assert.assertEquals("polarization.dimension", expected.polarization.dimension, actual.polarization.dimension);
        } else {
            Assert.assertNull(actual.polarization);
        }

        // TODO: custom axis
        
        // TODO: visibility
        
        Assert.assertEquals("metaReadGroups.size", expected.getMetaReadGroups().size(), actual.getMetaReadGroups().size());
        Iterator<URI> emra = expected.getMetaReadGroups().iterator();
        Iterator<URI> amra = actual.getMetaReadGroups().iterator();
        while (emra.hasNext() || amra.hasNext()) {
            Assert.assertEquals(emra.next(), amra.next());
        }

        Assert.assertEquals("dataReadGroups.size", expected.getMetaReadGroups().size(), actual.getMetaReadGroups().size());
        Iterator<URI> edra = expected.getDataReadGroups().iterator();
        Iterator<URI> adra = actual.getDataReadGroups().iterator();
        while (edra.hasNext() || adra.hasNext()) {
            Assert.assertEquals(edra.next(), adra.next());
        }

        log.debug("num artifacts: " + expected.getArtifacts().size() + " == " + actual.getArtifacts().size());
        Assert.assertEquals("number of artifacts", expected.getArtifacts().size(), actual.getArtifacts().size());
        Iterator<Artifact> ea = expected.getArtifacts().iterator();
        Iterator<Artifact> aa = actual.getArtifacts().iterator();
        while (ea.hasNext()) {
            Artifact ex = ea.next();
            Artifact ac = aa.next();
            testEqual(ex, ac);
        }

        testEntityChecksums(expected, actual);
    }

    private void testEqual(Artifact expected, Artifact actual) {
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
        while (ea.hasNext()) {
            Part ex = ea.next();
            Part ac = aa.next();
            testEqual(ex, ac);
        }

        testEntityChecksums(expected, actual);
    }

    private void testEqual(Part expected, Part actual) {
        testEntity(expected, actual);
        Assert.assertEquals("part.name", expected.getName(), actual.getName());
        Assert.assertEquals("number of chunks", expected.getChunks().size(), actual.getChunks().size());
        Iterator<Chunk> ea = expected.getChunks().iterator();
        Iterator<Chunk> aa = actual.getChunks().iterator();
        while (ea.hasNext()) {
            Chunk ex = ea.next();
            Chunk ac = aa.next();
            testEqual(ex, ac);
        }

        testEntityChecksums(expected, actual);
    }

    private void testEqual(Chunk expected, Chunk actual) {
        testEntity(expected, actual);

        Assert.assertEquals("productType", expected.productType, actual.productType);
        Assert.assertEquals("naxis", expected.naxis, actual.naxis);
        Assert.assertEquals("positionAxis1", expected.positionAxis1, actual.positionAxis1);
        Assert.assertEquals("positionAxis2", expected.positionAxis2, actual.positionAxis2);
        Assert.assertEquals("energyAxis", expected.energyAxis, actual.energyAxis);
        Assert.assertEquals("timeAxis", expected.timeAxis, actual.timeAxis);
        Assert.assertEquals("polarizationAxis", expected.polarizationAxis, actual.polarizationAxis);
        Assert.assertEquals("observableAxis", expected.observableAxis, actual.observableAxis);

        if (expected.position == null) {
            Assert.assertNull("position", actual.position);
        } else {
            testEqual(expected.position.getAxis(), actual.position.getAxis());
            Assert.assertEquals("position.coordsys", expected.position.coordsys, actual.position.coordsys);
            Assert.assertEquals("position.equinox", expected.position.equinox, actual.position.equinox);
            Assert.assertEquals("position.resolution", expected.position.resolution, actual.position.resolution);
        }

        if (expected.energy == null) {
            Assert.assertNull("energy", actual.energy);
        } else {
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

        if (expected.time == null) {
            Assert.assertNull("time", actual.time);
        } else {
            testEqual("time", expected.time.getAxis(), actual.time.getAxis());
            Assert.assertEquals("time.exposure", expected.time.exposure, actual.time.exposure);
            Assert.assertEquals("time.resolution", expected.time.resolution, actual.time.resolution);
        }

        if (expected.polarization == null) {
            Assert.assertNull("polarization", actual.polarization);
        } else {
            testEqual("polarization", expected.polarization.getAxis(), actual.polarization.getAxis());
        }

        if (expected.observable == null) {
            Assert.assertNull("observable", actual.observable);
        } else {
            Assert.assertEquals("observable.depndent.axis.ctype",
                    expected.observable.getDependent().getAxis().getCtype(),
                    actual.observable.getDependent().getAxis().getCtype());
            Assert.assertEquals("observable.depndent.axis.cunit",
                    expected.observable.getDependent().getAxis().getCunit(),
                    actual.observable.getDependent().getAxis().getCunit());
            Assert.assertEquals("observable.depndent.bin", expected.observable.getDependent().getBin(), actual.observable.getDependent().getBin());
            if (expected.observable.independent == null) {
                Assert.assertNull("observable.independent", actual.observable.independent);
            } else {
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

    private void testEqual(String s, CoordAxis1D expected, CoordAxis1D actual) {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);

        Assert.assertEquals(s, expected.getAxis().getCtype(), actual.getAxis().getCtype());
        Assert.assertEquals(s, expected.getAxis().getCunit(), actual.getAxis().getCunit());

        if (expected.error == null) {
            Assert.assertNull("error", actual.error);
        } else {
            Assert.assertNotNull(s, actual.error);
            Assert.assertEquals("syser", expected.error.syser, actual.error.syser);
            Assert.assertEquals("rnder", expected.error.rnder, actual.error.rnder);
        }
        if (expected.range == null) {
            Assert.assertNull(s, actual.range);
        } else {
            Assert.assertNotNull(s, actual.range);
            UtilTest.testEqual(s, expected.range, actual.range);
        }
        if (expected.bounds == null) {
            Assert.assertNull(s, actual.bounds);
        } else {
            Assert.assertNotNull(s, actual.bounds);
            UtilTest.testEqual(s, expected.bounds, actual.bounds);
        }
        if (expected.function == null) {
            Assert.assertNull(s, actual.function);
        } else {
            Assert.assertNotNull(s, actual.function);
            UtilTest.testEqual(s, expected.function, actual.function);
        }
    }

    private void testEqual(CoordAxis2D expected, CoordAxis2D actual) {
        log.debug("testEqual: " + expected + " == " + actual);
        Assert.assertFalse("same objects", expected == actual);

        Assert.assertEquals(expected.getAxis1().getCtype(), actual.getAxis1().getCtype());
        Assert.assertEquals(expected.getAxis1().getCunit(), actual.getAxis1().getCunit());
        Assert.assertEquals(expected.getAxis2().getCtype(), actual.getAxis2().getCtype());
        Assert.assertEquals(expected.getAxis2().getCunit(), actual.getAxis2().getCunit());
        if (expected.error1 == null) {
            Assert.assertNull("error1", actual.error1);
        } else {
            Assert.assertNotNull(actual.error1);
            Assert.assertEquals("syser1", expected.error1.syser, actual.error1.syser);
            Assert.assertEquals("rnder1", expected.error1.rnder, actual.error1.rnder);
        }
        if (expected.error2 == null) {
            Assert.assertNull("error2", actual.error2);
        } else {
            Assert.assertNotNull(actual.error2);
            Assert.assertEquals("syser2", expected.error2.syser, actual.error2.syser);
            Assert.assertEquals("rnder2", expected.error2.rnder, actual.error2.rnder);
        }
        if (expected.range == null) {
            Assert.assertNull("range", actual.range);
        } else {
            Assert.assertNotNull(actual.range);
            UtilTest.testEqual(expected.range, actual.range);
        }
        if (expected.bounds == null) {
            Assert.assertNull("bounds", actual.bounds);
        } else {
            Assert.assertNotNull(actual.bounds);
            UtilTest.testEqual(expected.bounds, actual.bounds);
        }
        if (expected.function == null) {
            Assert.assertNull("function", actual.function);
        } else {
            Assert.assertNotNull(actual.function);
            UtilTest.testEqual(expected.function, actual.function);
        }
    }

    private Observation getTestObservation(int depth, boolean simple) throws Exception {
        // read stored file copied from caom2
        String fname = "sample-derived-caom25.xml";
        File f = FileUtil.getFileFromResource(fname, AbstractObservationDAOTest.class);
        if (!f.exists()) {
            throw new RuntimeException("SETUP: failed to find " + fname);
        }
        
        ObservationReader r = new ObservationReader();
        DerivedObservation orig = (DerivedObservation) r.read(new FileReader(f));
        
        Observation ret;
        if (simple) {
            ret = new SimpleObservation(orig.getCollection(), orig.getURI(), SimpleObservation.EXPOSURE);
        } else {
            DerivedObservation tmp = new DerivedObservation(orig.getCollection(), orig.getURI(), orig.getAlgorithm());
            tmp.getMembers().addAll(orig.getMembers());
            ret = tmp;
        }
        // copy common
        ret.environment = orig.environment;
        ret.instrument = orig.instrument;
        ret.intent = orig.intent;
        ret.metaProducer = orig.metaProducer;
        ret.metaRelease = orig.metaRelease;
        ret.proposal = orig.proposal;
        ret.requirements = orig.requirements;
        ret.sequenceNumber = orig.sequenceNumber;
        ret.target = orig.target;
        ret.targetPosition = orig.targetPosition;
        ret.telescope = orig.telescope;
        ret.type = orig.type;
        ret.getMetaReadGroups().addAll(orig.getMetaReadGroups());
        
        ret.getPlanes().addAll(orig.getPlanes());
        for (Plane p : ret.getPlanes()) {
            CaomUtil.assignLastModified(p, null, "lastModified");
            CaomUtil.assignLastModified(p, null, "maxLastModified");
            CaomUtil.assignMetaChecksum(p, null, "metaChecksum");
            CaomUtil.assignMetaChecksum(p, null, "accMetaChecksum");
            for (Artifact a : p.getArtifacts()) {
                CaomUtil.assignLastModified(a, null, "lastModified");
                CaomUtil.assignLastModified(a, null, "maxLastModified");
                CaomUtil.assignMetaChecksum(a, null, "metaChecksum");
                CaomUtil.assignMetaChecksum(a, null, "accMetaChecksum");
                for (Part part : a.getParts()) {
                    CaomUtil.assignLastModified(part, null, "lastModified");
                    CaomUtil.assignLastModified(part, null, "maxLastModified");
                    CaomUtil.assignMetaChecksum(part, null, "metaChecksum");
                    CaomUtil.assignMetaChecksum(part, null, "accMetaChecksum");
                    for (Chunk c : part.getChunks()) {
                        CaomUtil.assignLastModified(c, null, "lastModified");
                        CaomUtil.assignLastModified(c, null, "maxLastModified");
                        CaomUtil.assignMetaChecksum(c, null, "metaChecksum");
                        CaomUtil.assignMetaChecksum(c, null, "accMetaChecksum");
                    }
                    if (depth < 5) {
                        part.getChunks().clear();
                    }
                }
                if (depth < 4) {
                    a.getParts().clear();
                }
            }
            if (depth < 3) {
                p.getArtifacts().clear();
            }
        }
        if (depth < 2) {
            ret.getPlanes().clear();
        }
        return ret;
        
    }

    private void junk() {
    /**
    private Observation getTestObservation(boolean full, int depth, boolean comp, boolean sci)
            throws Exception {
        Observation o;
        if (comp) {
            DerivedObservation obs = new DerivedObservation("TEST", new URI("caom:TEST/derived-bar"), new Algorithm("doit"));
            if (full) {
                obs.getMembers().add(new URI("caom:TEST/simple1"));
                obs.getMembers().add(new URI("caom:TEST/simple2"));
                obs.getMembers().add(new URI("caom:TEST/simple3"));
            }
            o = obs;
        } else {
            o = new SimpleObservation("TEST", new URI("caom:TEST/simple-bar"), SimpleObservation.EXPOSURE);
        }

        if (full) {

            o.metaProducer = URI.create("test:observation/roundrip-1.0");

            if (sci) {
                o.intent = ObservationIntentType.SCIENCE;
            } else {
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
            o.target.targetID = URI.create("naif:1701");
            o.target.type = TargetType.OBJECT;
            o.target.getKeywords().addAll(TEST_KEYWORDS);
            o.target.standard = Boolean.TRUE;
            o.target.redshift = new Double(0.0);
            o.target.moving = Boolean.FALSE;

            o.targetPosition = new TargetPosition("FK5", new Point(1.0, 2.0));
            if (sci) {
                o.targetPosition.equinox = 2000.0;
            }

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

            o.getMetaReadGroups().add(URI.create("ivo://example.net/gms?GroupA"));
            o.getMetaReadGroups().add(URI.create("ivo://example.net/gms?GroupB"));
        }

        if (depth == 1) {
            return o;
        }

        o.getPlanes().add(getTestPlane(full, new URI(o.getURI().toASCIIString() + "/thing1"), depth, true));
        o.getPlanes().add(getTestPlane(full, new URI(o.getURI().toASCIIString() + "/thing2"), depth, false));
        Assert.assertEquals(2, o.getPlanes().size());

        return o;
    }

    protected Plane getTestPlane(boolean full, URI productID, int depth, boolean poly)
            throws Exception {
        Plane p = new Plane(productID);
        if (full) {
            p.metaProducer = URI.create("test:plane/roundrip-1.0");

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
            p.provenance.getInputs().add(new URI("caom:FOO/bar/in1"));
            p.provenance.getInputs().add(new URI("caom:FOO/bar/in2"));

            p.metrics = new Metrics();
            p.metrics.sourceNumberDensity = 100.0;
            p.metrics.background = 2.7;
            p.metrics.backgroundStddev = 0.3;
            p.metrics.fluxDensityLimit = 1.0e-5;
            p.metrics.magLimit = 28.5;
            p.metrics.sampleSNR = 11.0;

            p.quality = new DataQuality(Quality.JUNK);

            p.energy = new Energy();
            p.energy.bandpassName = "V";
            p.energy.bounds = new SampledInterval(400e-6, 900e-6);
            p.energy.bounds.getSamples().add(new Interval(400e-6, 500e-6));
            p.energy.bounds.getSamples().add(new Interval(800e-6, 900e-6));
            p.energy.dimension = 2L;
            p.energy.getEnergyBands().add(EnergyBand.OPTICAL);
            p.energy.resolvingPower = 2.0;
            p.energy.resolvingPowerBounds = new Interval(1.8, 2.2);
            p.energy.restwav = 600e-9;
            p.energy.sampleSize = 100e-6;
            p.energy.transition = new EnergyTransition("H", "alpha");

            p.polarization = new Polarization();
            p.polarization.dimension = 3L;
            p.polarization.states = new TreeSet<>();
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
                for (Vertex v : mp.getVertices()) {
                    if (!SegmentType.CLOSE.equals(v.getType())) {
                        points.add(new Point(v.cval1, v.cval2));
                    }
                }
                p.position.bounds = new Polygon(points, mp);
            } else {
                p.position.bounds = new Circle(new Point(0.0, 89.0), 2.0);
            }

            p.position.dimension = new Dimension2D(1024, 2048);
            p.position.resolution = 0.05;
            p.position.resolutionBounds = new Interval(0.04, 0.06);
            p.position.sampleSize = 0.025;
            p.position.timeDependent = false;

            p.time = new Time();
            p.time.bounds = new SampledInterval(50000.25, 50000.75);
            p.time.bounds.getSamples().add(new Interval(50000.25, 50000.40));
            p.time.bounds.getSamples().add(new Interval(50000.50, 50000.75));
            p.time.dimension = 2L;
            p.time.exposure = 666.0;
            p.time.resolution = 0.5;
            p.time.resolutionBounds = new Interval(0.22, 0.88);
            p.time.sampleSize = 0.15;

            p.custom = new CustomAxis("FDEP");
            p.custom.bounds = new SampledInterval(100.0, 200.0);
            p.custom.bounds.getSamples().add(new Interval(100.0, 140.0));
            p.custom.bounds.getSamples().add(new Interval(160.0, 200.0));
            p.custom.bounds.validate();
            p.custom.dimension = 1024L;

            p.observable = new Observable("phot.flux");

            p.getMetaReadGroups().add(URI.create("ivo://example.net/gms?GroupA"));
            p.getMetaReadGroups().add(URI.create("ivo://example.net/gms?GroupB"));

            p.getDataReadGroups().add(URI.create("ivo://example.net/gms?GroupC"));
            p.getDataReadGroups().add(URI.create("ivo://example.net/gms?GroupD"));
        }
        if (depth <= 2) {
            return p;
        }

        p.getArtifacts().add(getTestArtifact(full, new URI("http://www.example.com/stuff/" + productID + "a"), depth));
        p.getArtifacts().add(getTestArtifact(full, new URI("http://www.example.com/stuff/" + productID + "b"), depth));
        Assert.assertEquals(2, p.getArtifacts().size());

        return p;
    }

    private Artifact getTestArtifact(boolean full, URI uri, int depth) {
        Artifact a = new Artifact(uri, ProductType.SCIENCE, ReleaseType.DATA);
        if (full) {
            a.metaProducer = URI.create("test:artifact/roundrip-1.0");

            a.contentType = "application/fits";
            a.contentLength = TEST_LONG;
            a.contentChecksum = URI.create("md5:fb696fe6e2fbb98dee340bd1e8811dcb");
            a.contentRelease = TEST_DATE;
            a.getContentReadGroups().add(URI.create("ivo://example.net/gms?GroupQ"));
            a.getContentReadGroups().add(URI.create("ivo://example.net/gms?GroupW"));
        }

        if (depth <= 3) {
            return a;
        }

        a.getParts().add(getTestPart(full, new Integer(1), depth));
        a.getParts().add(getTestPart(full, "A", depth));
        Assert.assertEquals(2, a.getParts().size());

        return a;
    }

    private Part getTestPart(boolean full, Integer pnum, int depth) {
        Part p = new Part(pnum);
        if (full) {
            p.metaProducer = URI.create("test:part/roundrip-1.0");
            p.productType = ProductType.SCIENCE;
        }

        if (depth <= 4) {
            return p;
        }

        p.getChunks().add(getPosFunctionChunk(full));
        p.getChunks().add(getEmptyChunk());
        p.getChunks().add(getObservableChunk(full));

        return p;
    }

    private Part getTestPart(boolean full, String pname, int depth) {
        Part p = new Part(pname);
        if (full) {
            p.metaProducer = URI.create("test:part/roundrip-1.0");
            p.productType = ProductType.SCIENCE;
        }

        if (depth <= 4) {
            return p;
        }

        p.getChunks().add(getPosRangeChunk(full));
        p.getChunks().add(getSpecChunk(full));
        p.getChunks().add(getPolarChunk(full));

        return p;
    }

    private Chunk getPosFunctionChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
        c.positionAxis1 = new Integer(1);
        c.positionAxis2 = new Integer(2);
        c.position = new SpatialWCS(new CoordAxis2D(new Axis("RA---TAN", "deg"), new Axis("DEC--TAN", "deg")));
        c.position.coordsys = "FK5";
        c.position.equinox = 2000.0;
        c.position.getAxis().error1 = new CoordError(1.0, 2.0);
        c.position.getAxis().error2 = new CoordError(3.0, 4.0);

        // rectangle centered at 10,20
        Coord2D ref = new Coord2D(new RefCoord(512.0, 10.0), new RefCoord(1024, 20.0));
        Dimension2D dim = new Dimension2D(1024, 2048);
        c.position.getAxis().function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 1.0e-3); // approx 1x2 deg

        c.time = getTemporalWCS(false);

        return c;
    }

    private TemporalWCS getTemporalWCS(boolean offset) {
        TemporalWCS t = new TemporalWCS(new CoordAxis1D(new Axis("TIME", "d")));
        t.timesys = "UTC";
        t.trefpos = "TOPOCENTER";
        if (offset) {
            t.mjdref = 50000.0;
        }
        t.exposure = new Double(0.05 * 86400.0);
        t.resolution = 0.1;

        double a = 1234.0;
        if (!offset) {
            a += 50000.0; // absolute
        }
        t.getAxis().range = new CoordRange1D(new RefCoord(0.5, a), new RefCoord(1.5, a + 0.1));

        t.getAxis().bounds = new CoordBounds1D();
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a), new RefCoord(1.5, a + 0.02)));
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a + 0.04), new RefCoord(1.5, a + 0.06)));
        t.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, a + 0.09), new RefCoord(1.5, a + 0.10)));

        return t;
    }

    private Chunk getPosRangeChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
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

    private Chunk getSpecChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
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

        c.energy.getAxis().function = new CoordFunction1D(1024L, (c3.val - c1.val) / 1024.0, c1);

        return c;
    }

    private Chunk getPolarChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
        c.polarizationAxis = new Integer(1);
        c.polarization = new PolarizationWCS(new CoordAxis1D(new Axis("STOKES", null)));
        c.polarization.getAxis().function = new CoordFunction1D(3L, 1.0, new RefCoord(1.0, 1.0)); // I Q U
        return c;
    }

    private Chunk getCustomChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
        c.customAxis = new Integer(1);
        c.custom = new CustomWCS(new CoordAxis1D(new Axis("FDEP", "flibbles")));
        c.custom.getAxis().range = new CoordRange1D(new RefCoord(100.0, 1.0), new RefCoord(900.0, 200.0));
        c.custom.getAxis().bounds = new CoordBounds1D();
        c.custom.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(100.0, 100.0), new RefCoord(400.0, 140.0)));
        c.custom.getAxis().bounds.getSamples().add(new CoordRange1D(new RefCoord(600.0, 160.0), new RefCoord(900.0, 200.0)));
        c.custom.getAxis().function = new CoordFunction1D(1024L, 1.0, new RefCoord(1.0, 1.0));
        return c;
    }

    private Chunk getEmptyChunk() {
        Chunk c = new Chunk();

        return c;
    }

    private Chunk getObservableChunk(boolean full) {
        Chunk c = new Chunk();
        c.metaProducer = URI.create("test:chunk/roundrip-1.0");

        if (full) {
            c.productType = ProductType.SCIENCE;
        }
        c.observableAxis = new Integer(1);
        c.observable = new ObservableAxis(new Slice(new Axis("flux", "J"), new Long(3L)));
        c.observable.independent = new Slice(new Axis("WAV", "um"), new Long(4L));
        return c;
    }

    private void print(Observation o) {
        log.warn(o.toString());
        Iterator<Plane> pi = o.getPlanes().iterator();
        while (pi.hasNext()) {
            Plane p = pi.next();
            log.warn("\t" + p);
            Iterator<Artifact> ai = p.getArtifacts().iterator();
            while (ai.hasNext()) {
                Artifact a = ai.next();
                log.warn("\t\t" + a);
                Iterator<Part> pti = a.getParts().iterator();
                while (pti.hasNext()) {
                    Part pt = pti.next();
                    log.warn("\t\t\t" + pt);
                    Iterator ci = pt.getChunks().iterator();
                    while (ci.hasNext()) {
                        log.warn("\t\t\t\t" + ci.next());
                    }
                }
            }
        }
    }
    */
    }
}
