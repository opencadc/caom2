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

package ca.nrc.cadc.caom2.util;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CaomUtilTest 
{
    private static final Logger log = Logger.getLogger(CaomUtilTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.INFO);
    }

    Random rnd = new Random();
    
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
    public void testAssignID()
    {
        try
        {
            UUID id = new UUID(0L, 666L);
            
            CaomEntity ce = new SimpleObservation("FOO", "bar");
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
            
            ce = new CompositeObservation("FOO", "bar", new Algorithm("doit"));
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
            
            ce = new Plane("baz");
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
            
            ce = new Artifact(new URI("ad:FOO/bar"), ProductType.SCIENCE, ReleaseType.DATA);
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
            
            ce = new Part(0);
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
            
            ce = new Chunk();
            CaomUtil.assignID(ce, id);
            Assert.assertEquals(id, ce.getID());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testAssignLastModified()
    {
        try
        {
            Date expected = new Date();
            
            CaomEntity ce = new SimpleObservation("FOO", "bar");
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            ce = new CompositeObservation("FOO", "bar", new Algorithm("doit"));
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            ce = new Plane("baz");
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            ce = new Artifact(new URI("ad:FOO/bar"), ProductType.SCIENCE, ReleaseType.DATA);
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            ce = new Part(0);
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            ce = new Chunk();
            CaomUtil.assignLastModified(ce, expected, "lastModified");
            Assert.assertEquals(expected, ce.getLastModified());
            CaomUtil.assignLastModified(ce, expected, "maxLastModified");
            Assert.assertEquals(expected, ce.getMaxLastModified());
            
            Date d = CaomUtil.getLastModified(ce);
            Assert.assertEquals(expected, d);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testObservationURI()
    {
        try
        {
            Set<ObservationURI> uris = new TreeSet<ObservationURI>();
            String actual = CaomUtil.encodeObservationURIs(uris);
            Assert.assertNull(actual);
            Set<ObservationURI> uris2 = new TreeSet<ObservationURI>();
            CaomUtil.decodeObservationURIs(actual, uris2);
            Assert.assertTrue(uris2.isEmpty());
            
            ObservationURI u1 = new ObservationURI("FOO", "bar1");
            ObservationURI u2 = new ObservationURI("FOO", "bar2");
            ObservationURI u3 = new ObservationURI("FOO", "bar3");
            
            uris2.clear();
            uris.add(u1);
            uris.add(u2);
            uris.add(u3);
            actual = CaomUtil.encodeObservationURIs(uris);
            Assert.assertNotNull(actual);
            CaomUtil.decodeObservationURIs(actual, uris2);
            Assert.assertEquals(3, uris2.size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPlaneURI()
    {
        try
        {
            try
        {
            Set<PlaneURI> uris = new TreeSet<PlaneURI>();
            String actual = CaomUtil.encodePlaneURIs(uris);
            Assert.assertNull(actual);
            Set<PlaneURI> uris2 = new TreeSet<PlaneURI>();
            CaomUtil.decodePlaneURIs(actual, uris2);
            Assert.assertTrue(uris2.isEmpty());
            
            ObservationURI ouri = new ObservationURI("FOO", "bar");
            uris.add(new PlaneURI(ouri, "foo1"));
            uris.add(new PlaneURI(ouri, "foo2"));
            uris.add(new PlaneURI(ouri, "foo3"));
            actual = CaomUtil.encodePlaneURIs(uris);
            Assert.assertNotNull(actual);
            CaomUtil.decodePlaneURIs(actual, uris2);
            Assert.assertEquals(3, uris2.size());
            Assert.assertTrue( uris.containsAll(uris2));
            Assert.assertTrue( uris2.containsAll(uris));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPolarizationStates()
    {
        try
        {
            List<PolarizationState> pol = new ArrayList<PolarizationState>();
            List<PolarizationState> apol = new ArrayList<PolarizationState>();
            String actual = CaomUtil.encodeStates(pol);
            Assert.assertNull(actual);
            
            pol.add(PolarizationState.I);
            actual = CaomUtil.encodeStates(pol);
            Assert.assertEquals("/I/", actual);
            
            CaomUtil.decodeStates(actual, apol);
            Assert.assertEquals(1, apol.size());
            Assert.assertEquals(PolarizationState.I, apol.get(0));
            
            pol.add(PolarizationState.Q);
            pol.add(PolarizationState.U);
            actual = CaomUtil.encodeStates(pol);
            Assert.assertEquals("/I/Q/U/", actual);
            
            pol.clear();
            pol.add(PolarizationState.U);
            pol.add(PolarizationState.Q);
            pol.add(PolarizationState.I);
            actual = CaomUtil.encodeStates(pol);
            Assert.assertEquals("/I/Q/U/", actual); // canonical order
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testKeywordList()
    {
        try
        {
            Set<String> keywords = new TreeSet<String>();
            String actual = CaomUtil.encodeListString(keywords);
            Assert.assertNull(actual);
            
            Set<String> kw2 = new TreeSet<String>();
            CaomUtil.decodeListString(actual, kw2);
            Assert.assertTrue(kw2.isEmpty());
            
            keywords.add("foo");
            keywords.add("bar");
            keywords.add("num=2");
            actual = CaomUtil.encodeListString(keywords);
            Assert.assertNotNull(actual);
            CaomUtil.decodeListString(actual, kw2);
            Assert.assertEquals(3, kw2.size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordRange1D()
    {
        log.debug("testCoordRange1D - START");
        try
        {
            String s = CaomUtil.encodeCoordRange1D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordRange1D expected = new CoordRange1D(new RefCoord(2.0+dx, 3.0+dx), new RefCoord(4.0+dx, 5.0+dx));
            s = CaomUtil.encodeCoordRange1D(expected);
            log.debug("testCoordRange1D - encoded: " + s.length() + " " + s);
            CoordRange1D actual = CaomUtil.decodeCoordRange1D(s);
            testEqual("range", expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testCoordBounds1D()
    {
        log.debug("testCoordBounds1D - START");
        try
        {
            String s = CaomUtil.encodeCoordBounds1D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordBounds1D expected = new CoordBounds1D();
            expected.getSamples().add(new CoordRange1D(new RefCoord(2.0+dx, 0.0+dx), new RefCoord(4.0+dx, 20.0+dx)));
            expected.getSamples().add(new CoordRange1D(new RefCoord(5.0+dx, 30.0+dx), new RefCoord(8.0+dx, 50.0+dx)));
            expected.getSamples().add(new CoordRange1D(new RefCoord(9.0+dx, 60.0+dx), new RefCoord(10.0+dx, 80.0+dx)));
            s = CaomUtil.encodeCoordBounds1D(expected);
            log.debug("testCoordBounds1D - encoded: " + s.length() + " " + s);
            CoordBounds1D actual = CaomUtil.decodeCoordBounds1D(s);
            testEqual("bounds", expected, actual);

            // empty samples list
            expected = new CoordBounds1D();
            s = CaomUtil.encodeCoordBounds1D(expected);
            actual = CaomUtil.decodeCoordBounds1D(s);
            testEqual("bounds", expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testCoordFunction1D()
    {
        log.debug("testCoordFunction1D - START");
        try
        {
            String s = CaomUtil.encodeCoordFunction1D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordFunction1D expected = new CoordFunction1D(1024L, 0.1+dx, new RefCoord(2.0+dx, 0.0+dx));
            s = CaomUtil.encodeCoordFunction1D(expected);
            log.debug("testCoordFunction1D - encoded: " + s.length() + " " + s);
            CoordFunction1D actual = CaomUtil.decodeCoordFunction1D(s);
            testEqual("function", expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordRange2D()
    {
        log.debug("testCoordRange2D - START");
        try
        {
            String s = CaomUtil.encodeCoordRange2D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordRange2D expected = new CoordRange2D(
                    new Coord2D(new RefCoord(2.0+dx, 3.0+dx), new RefCoord(4.0+dx, 5.0+dx)),
                    new Coord2D(new RefCoord(20.0+dx, 30.0+dx), new RefCoord(40.0+dx, 50.0+dx)));
            s = CaomUtil.encodeCoordRange2D(expected);
            log.debug("testCoordRange2D - encoded: " + s.length() + " " + s);
            CoordRange2D actual = CaomUtil.decodeCoordRange2D(s);
            testEqual(expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testCoordBounds2D()
    {
        log.debug("testCoordBounds2D - START");
        try
        {
            String s = CaomUtil.encodeCoordBounds2D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordPolygon2D poly = new CoordPolygon2D();
            poly.getVertices().add(new ValueCoord2D(2.0+dx, 3.0+dx));
            poly.getVertices().add(new ValueCoord2D(2.0+dx, 4.0+dx));
            poly.getVertices().add(new ValueCoord2D(3.0+dx, 3.0+dx));

            CoordBounds2D expected = poly;
            s = CaomUtil.encodeCoordBounds2D(expected);
            log.debug("testCoordBounds2D - encoded: " + s.length() + " " + s);
            CoordBounds2D actual = CaomUtil.decodeCoordBounds2D(s);
            testEqual(expected, actual);

            expected = new CoordCircle2D(new ValueCoord2D(2.0+dx, 3.0+dx), 1.0+dx);
            s = CaomUtil.encodeCoordBounds2D(expected);
            log.debug("testCoordBounds2D - encoded: " + s.length() + " " + s);
            actual = CaomUtil.decodeCoordBounds2D(s);
            testEqual(expected, actual);

            // empty vertex list
            expected = new CoordPolygon2D();
            s = CaomUtil.encodeCoordBounds2D(expected);
            log.debug("testCoordBounds2D - encoded: " + s.length() + " " + s);
            actual = CaomUtil.decodeCoordBounds2D(s);
            testEqual(expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testCoordFunction2D()
    {
        log.debug("testCoordFunction2D - START");
        try
        {
            String s = CaomUtil.encodeCoordFunction2D(null);
            Assert.assertNull(s);
            
            double dx = rnd.nextDouble(); // add random factor so we get lots of digits
            CoordFunction2D expected = new CoordFunction2D(new Dimension2D(123456,123456),
                    new Coord2D(new RefCoord(2.0+dx, 3.0+dx), new RefCoord(4.0+dx, 5.0+dx)),
                    dx*1.0e-3, dx*1.0e-6, dx*1.0e-6, dx*1.0e-3);
            s = CaomUtil.encodeCoordFunction2D(expected);
            log.debug("testCoordFunction2D - encoded: " + s.length() + " " + s);
            CoordFunction2D actual = CaomUtil.decodeCoordFunction2D(s);
            testEqual(expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    static void testEqual(String s, CoordRange1D expected, CoordRange1D actual)
    {
        Assert.assertTrue(s+".start.pix", expected.getStart().pix == actual.getStart().pix);
        Assert.assertTrue(s+".start.val", expected.getStart().val == actual.getStart().val);
        Assert.assertTrue(s+".end.pix", expected.getEnd().pix == actual.getEnd().pix);
        Assert.assertTrue(s+".end.val", expected.getEnd().val == actual.getEnd().val);
    }
    static void testEqual(String s, CoordBounds1D expected, CoordBounds1D actual)
    {
        Assert.assertEquals("bounds.samples.size", expected.getSamples().size(), actual.getSamples().size());
        Iterator<CoordRange1D> ex = expected.getSamples().iterator();
        Iterator<CoordRange1D> ac = actual.getSamples().iterator();
        while ( ex.hasNext() )
            testEqual(s+".sample", ex.next(), ac.next());
    }
    static void testEqual(String s, CoordFunction1D expected, CoordFunction1D actual)
    {
        Assert.assertEquals(s+".function.naxis", expected.getNaxis(), actual.getNaxis());
        Assert.assertEquals(s+".function.delta", expected.getDelta(), actual.getDelta());
        Assert.assertTrue(s+".function.refCoord.pix", expected.getRefCoord().pix == actual.getRefCoord().pix);
        Assert.assertTrue(s+".function.refCoord.val", expected.getRefCoord().val == actual.getRefCoord().val);
    }
    static void testEqual(CoordRange2D expected, CoordRange2D actual)
    {
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord1().pix == actual.getStart().getCoord1().pix);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord1().val == actual.getStart().getCoord1().val);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord2().pix == actual.getStart().getCoord2().pix);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord2().val == actual.getStart().getCoord2().val);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord1().pix == actual.getEnd().getCoord1().pix);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord1().val == actual.getEnd().getCoord1().val);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord2().pix == actual.getEnd().getCoord2().pix);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord2().val == actual.getEnd().getCoord2().val);
    }
    static void testEqual(CoordBounds2D expected, CoordBounds2D actual)
    {
        Assert.assertEquals("bounds class", expected.getClass(), actual.getClass());
        if (expected instanceof CoordCircle2D)
        {
            CoordCircle2D ex = (CoordCircle2D) expected;
            CoordCircle2D ac = (CoordCircle2D) actual;
            Assert.assertTrue("bounds.center.coord1", ex.getCenter().coord1 == ac.getCenter().coord1);
            Assert.assertTrue("bounds.center.coord2",  ex.getCenter().coord2 == ac.getCenter().coord2);
            Assert.assertTrue("bounds.center.radius", ex.getRadius().equals(ac.getRadius()));
        }
        else
        {
            CoordPolygon2D ex = (CoordPolygon2D) expected;
            CoordPolygon2D ac = (CoordPolygon2D) actual;
            Assert.assertEquals("bounds.vertices.size", ex.getVertices().size(), ac.getVertices().size());
            Iterator<ValueCoord2D> exi = ex.getVertices().iterator();
            Iterator<ValueCoord2D> aci = ac.getVertices().iterator();
            while ( exi.hasNext() )
            {
                ValueCoord2D e = exi.next();
                ValueCoord2D a = aci.next();
                Assert.assertTrue(e.coord1 == a.coord1);
                Assert.assertTrue(e.coord2 == a.coord2);
            }

        }
    }
    static void testEqual(CoordFunction2D expected, CoordFunction2D actual)
    {
        Assert.assertEquals("function.dimension.naxis1", expected.getDimension().naxis1, actual.getDimension().naxis1);
        Assert.assertEquals("function.dimension.naxis2", expected.getDimension().naxis2, actual.getDimension().naxis2);
        Assert.assertTrue("function.refCoord1.pix", expected.getRefCoord().getCoord1().pix
                == actual.getRefCoord().getCoord1().pix);
        Assert.assertTrue("function.refCoord1.val", expected.getRefCoord().getCoord1().val
                == actual.getRefCoord().getCoord1().val);
        Assert.assertTrue("function.refCoord2.pix", expected.getRefCoord().getCoord2().pix
                == actual.getRefCoord().getCoord2().pix);
        Assert.assertTrue("function.refCoord2.val", expected.getRefCoord().getCoord2().val
                == actual.getRefCoord().getCoord2().val);
        Assert.assertTrue("cd11", expected.getCd11() == actual.getCd11());
        Assert.assertTrue("cd12", expected.getCd12() == actual.getCd12());
        Assert.assertTrue("cd21", expected.getCd21() == actual.getCd21());
        Assert.assertTrue("cd22", expected.getCd22() == actual.getCd22());
    }
}
