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

package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author pdowler
 */
public class PositionUtilTest {
    private static final Logger log = Logger.getLogger(PositionUtilTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }


    //@Test
    public void testTemplate() {
        try {
            // TODO
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInferCoordSys() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("ICRS", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());

            // FK5
            wcs.equinox = 1999.1;
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("FK5", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());

            // FK5
            wcs.equinox = 2000.9;
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("FK5", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());


            // FK4
            wcs.equinox = 1949.1;
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("FK4", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());

            wcs.equinox = 1950.9;
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("FK4", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());

            // swapped
            axis = new CoordAxis2D(axis2, axis1);
            wcs = new SpatialWCS(axis);
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("ICRS", cs.getName());
            Assert.assertEquals(true, cs.isSwappedAxes());

            // GAL
            axis1 = new Axis("GLON-TAN", "deg");
            axis2 = new Axis("GLAT-TAN", "deg");
            axis = new CoordAxis2D(axis1, axis2);
            wcs = new SpatialWCS(axis);
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("GAL", cs.getName());
            Assert.assertEquals(false, cs.isSwappedAxes());

            // swapped
            axis1 = new Axis("GLON-TAN", "deg");
            axis2 = new Axis("GLAT-TAN", "deg");
            axis = new CoordAxis2D(axis2, axis1);
            wcs = new SpatialWCS(axis);
            cs = PositionUtil.inferCoordSys(wcs);
            Assert.assertNotNull(cs);
            Assert.assertNotNull(cs.getName());
            Assert.assertEquals("GAL", cs.getName());
            Assert.assertEquals(true, cs.isSwappedAxes());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordRangeToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;

            Coord2D start = new Coord2D(new RefCoord(0.5, 10), new RefCoord(0.5, 20));
            Coord2D end = new Coord2D(new RefCoord(512.5, 11), new RefCoord(512.5, 21));
            axis.range = new CoordRange2D(start, end);
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            for (Vertex v : poly.getVertices()) {
                log.debug("testCoordRangeToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordCircleToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            ValueCoord2D cen = new ValueCoord2D(2.0, 3.0);
            axis.bounds = new CoordCircle2D(cen, 0.5);
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);

            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            for (Vertex v : poly.getVertices()) {
                log.debug("testCoordCircleToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());
            //double expected = Math.PI*0.25; // triangulated
            double area = 1.0; // bounding box 
            Assert.assertEquals("area", area, poly.getArea(), 0.01);

            // circle overlapping meridian
            cen = new ValueCoord2D(0.1, 3.0);
            axis.bounds = new CoordCircle2D(cen, 0.5);

            poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            for (Vertex v : poly.getVertices()) {
                log.debug("testCoordCircleToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());
            //double expected = Math.PI*0.25; // triangulated
            area = 1.0; // bounding box 
            Assert.assertEquals("area at meridian", area, poly.getArea(), 0.01);
            Point pcen = poly.getCenter();
            Assert.assertEquals("center at pole", 0.1, pcen.cval1, 0.01);
            Assert.assertEquals("center at pole", 3.0, pcen.cval2, 0.01);


            // circle at the pole
            cen = new ValueCoord2D(30.0, 89.0);
            axis.bounds = new CoordCircle2D(cen, 0.5);
            poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            Assert.assertEquals("area at pole", area, poly.getArea(), 0.01);
            pcen = poly.getCenter();
            Assert.assertEquals("center at pole", 30.0, pcen.cval1, 0.01);
            Assert.assertEquals("center at pole", 89.0, pcen.cval2, 0.01);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordPolygonToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            CoordPolygon2D cp = new CoordPolygon2D();
            cp.getVertices().add(new ValueCoord2D(10, 20));
            cp.getVertices().add(new ValueCoord2D(11, 20));
            cp.getVertices().add(new ValueCoord2D(11, 21));
            cp.getVertices().add(new ValueCoord2D(10, 21));
            axis.bounds = cp;
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            for (Vertex v : poly.getVertices()) {
                log.debug("testCoordPolygonToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidCoordPolygonToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            CoordPolygon2D cp = new CoordPolygon2D();
            // polygon with zero area
            cp.getVertices().add(new ValueCoord2D(10, 20));
            cp.getVertices().add(new ValueCoord2D(11, 21));
            cp.getVertices().add(new ValueCoord2D(11, 21));
            cp.getVertices().add(new ValueCoord2D(10, 20));
            axis.bounds = cp;
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            Assert.fail("expected IllegalPolygonException, got: " + poly);
        } catch (IllegalPolygonException expected) {
            log.debug("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

        // bow-tie
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            CoordPolygon2D cp = new CoordPolygon2D();
            // polygon with zero area
            cp.getVertices().add(new ValueCoord2D(10, 20));
            cp.getVertices().add(new ValueCoord2D(11, 21));
            cp.getVertices().add(new ValueCoord2D(11, 20));
            cp.getVertices().add(new ValueCoord2D(10, 21));
            axis.bounds = cp;
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            Assert.fail("expected IllegalPolygonException, got: " + poly);
        } catch (IllegalPolygonException expected) {
            log.debug("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCoordFunctionToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            Dimension2D dim = new Dimension2D(1024, 1024);
            Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
            axis.function = new CoordFunction2D(dim, ref, 1.e-3, 0.0, 0.0, 1.0e-3);
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);
            for (Vertex v : poly.getVertices()) {
                log.debug("testFunctionToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testCoordFunctionToICRSPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            Dimension2D dim = new Dimension2D(1024, 1024);
            Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
            axis.function = new CoordFunction2D(dim, ref, 1.e-3, 0.0, 0.0, 1.0e-3);
            MultiPolygon poly = PositionUtil.toShapeICRS(wcs);
            for (Vertex v : poly.getVertices()) {
                log.debug("testFunctionToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            Dimension2D dim = new Dimension2D(1024, 1024);
            Coord2D ref = new Coord2D(new RefCoord(512, 370), new RefCoord(512, 20)); // requires range reduction
            axis.function = new CoordFunction2D(dim, ref, 1.e-3, 0.0, 0.0, 1.0e-3);
            MultiPolygon poly = PositionUtil.toShapeICRS(wcs);
            for (Vertex v : poly.getVertices()) {
                log.debug("testFunctionToPolygon: " + v);
            }
            Assert.assertNotNull(poly);
            Assert.assertEquals(5, poly.getVertices().size());
            for (Vertex v : poly.getVertices()) {
                if (!SegmentType.CLOSE.equals(v.getType())) {
                    Assert.assertTrue("range reduction: " + v.cval1, 9.0 < v.cval1 && v.cval1 < 11.0);
                }
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testInvalidCoordFunctionToPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            Dimension2D dim = new Dimension2D(1024, 1024);
            Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
            axis.function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 0.0); // singular CD matrix
            PositionUtil.CoordSys cs = PositionUtil.inferCoordSys(wcs);
            
            MultiPolygon poly = PositionUtil.toShape(wcs, cs.swappedAxes);

            Assert.fail("expected WCSLibRuntimeException");
        } catch (WCSLibRuntimeException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidCoordFunctionToICRSPolygon() {
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.equinox = null;
            Dimension2D dim = new Dimension2D(1024, 1024);
            Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
            axis.function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 0.0); // singular CD matrix
            MultiPolygon poly = PositionUtil.toShapeICRS(wcs);

            Assert.fail("expected WCSLibRuntimeException");
        } catch (WCSLibRuntimeException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSkippableToICRSPolygon() {
        // some SpatialWCS but not enough to compute should not fail
        try {
            Axis axis1 = new Axis("RA---TAN", "deg");
            Axis axis2 = new Axis("DEC--TAN", "deg");
            CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(axis);
            
            MultiPolygon poly = PositionUtil.toShapeICRS(wcs);
            
            Assert.assertNull("no polygon", poly);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeBounds() {
        try {
            Plane plane;
            Polygon poly;

            // coordinate range
            plane = getTestSetRange(1, 2);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(2.0, poly.getArea(), 0.05);
            Assert.assertEquals(21.0, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            plane = getTestSetRange(1, 2);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(2.0, poly.getArea(), 0.05);
            Assert.assertEquals(21.0, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            plane = getTestSetRange(1, 3);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(3.0, poly.getArea(), 0.08);
            Assert.assertEquals(21.5, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            plane = getTestSetRange(4, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(4.0, poly.getArea(), 0.1);
            Assert.assertEquals(22.0, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            // coordinate function
            plane = getTestSetFunction(1, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(1.0, poly.getArea(), 0.05);
            Assert.assertEquals(20.5, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);
            
            // mask with CoordCircle2D; array is 1000x1000 at 1e-3 aka 1x1 deg
            Chunk chunk = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            chunk.position.getAxis().bounds = new CoordCircle2D(new ValueCoord2D(20.5, 10.5), 0.25);
            Circle circ = (Circle) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(circ);
            Assert.assertEquals(20.5, circ.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, circ.getCenter().cval2, 0.02);
            Assert.assertEquals(0.25, circ.getRadius(), 0.02);

            plane = getTestSetFunction(1, 2);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            log.debug("[testComputeBounds] getTestSetFunction union: " + poly);
            //Assert.assertEquals(5, poly.getVertices().size());
            Assert.assertEquals(2.0, poly.getArea(), 0.05);
            Assert.assertEquals(21.0, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            plane = getTestSetFunction(1, 3);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            log.debug("[testComputeBounds] getTestSetFunction union: " + poly);
            //Assert.assertEquals(5, poly.getVertices().size());
            Assert.assertEquals(3.0, poly.getArea(), 0.08);
            Assert.assertEquals(21.5, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);

            plane = getTestSetFunction(4, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            log.debug("[testComputeBounds] getTestSetFunction union: " + poly);
            //Assert.assertEquals(5, poly.getVertices().size());
            Assert.assertEquals(4.0, poly.getArea(), 0.1);
            Assert.assertEquals(22.0, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeDimension() {
        try {
            Plane plane;
            Polygon poly;
            Dimension2D dim;

            plane = getTestSetFunction(1, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(1000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);
            
            // mask with CoordCircle2D; array is 1000x1000 at 1e-3 aka 1x1 deg
            Chunk chunk = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            chunk.position.getAxis().bounds = new CoordCircle2D(new ValueCoord2D(20.5, 10.5), 0.25);
            Shape bounds = PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(bounds);
            dim = PositionUtil.computeDimensionsFromWCS(bounds, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            // TODO: need to use pixel-aligned "cutout" of circle vs array to get the right dimensions
            //Assert.assertEquals(500L, dim.naxis1, 1L);
            //Assert.assertEquals(500L, dim.naxis2, 1L);

            // with GAL <-> ICRS transformations
            plane = getTestSetFunction(1, 1, true);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(1000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);

            plane = getTestSetFunction(1, 2);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(2000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);

            plane = getTestSetRange(1, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNull(dim);
            dim = PositionUtil.computeDimensionsFromRange(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(1000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);

            plane = getTestSetRange(1, 3);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNull(dim);
            dim = PositionUtil.computeDimensionsFromRange(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("ComputeUtil" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(3000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);

            plane = getTestSetRange(4, 1);
            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNotNull(poly);
            dim = PositionUtil.computeDimensionsFromWCS(poly, plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            Assert.assertNull(dim);
            dim = PositionUtil.computeDimensionsFromRange(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeDimension] dim=" + dim);
            Assert.assertNotNull(dim);
            Assert.assertEquals(4000L, dim.naxis1, 1L);
            Assert.assertEquals(1000L, dim.naxis2, 1L);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeResolution() {
        try {
            Plane plane;
            Polygon poly;
            Double sz;
            double expectedSize = 0.03; // weighted average of the two chunks

            plane = getTestSetFunction(1, 2);
            Iterator<Part> pi = plane.getArtifacts().iterator().next().getParts().iterator();
            Chunk c1 = pi.next().getChunks().iterator().next();
            Chunk c2 = pi.next().getChunks().iterator().next();
            c1.position.resolution = 0.02; // arcsec
            c2.position.resolution = 0.04; // arcsec
            sz = PositionUtil.computeResolution(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeResolution] sz=" + sz);
            Assert.assertNotNull(sz);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeSampleSize() {
        try {
            Plane plane;
            Polygon poly;
            Double sz;
            double expectedSize = 3600.0 * 1.0e-3; // arcsec

            plane = getTestSetFunction(1, 1);
            sz = PositionUtil.computeSampleSize(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeSampleSize] sz=" + sz);
            Assert.assertNotNull(sz);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);

            plane = getTestSetRange(1, 1);
            sz = PositionUtil.computeSampleSize(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeSampleSize] sz=" + sz);
            Assert.assertNotNull(sz);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);
            Assert.assertEquals(expectedSize, sz, 1.0e-6);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeTimeDependent() {
        try {
            Plane plane = getTestSetFunction(1, 2);
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            Boolean td;

            td = PositionUtil.computeTimeDependent(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeResolution] timeDep=" + td);
            Assert.assertNull(td);

            c.position.coordsys = "GAPPT";
            td = PositionUtil.computeTimeDependent(plane.getArtifacts(), DataLinkSemantics.SCIENCE);
            log.debug("[testComputeResolution] timeDep=" + td);
            Assert.assertNotNull(td);
            Assert.assertTrue(td);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromCalibration() {
        try {
            Plane plane;
            Polygon poly;

            // coordinate range
            plane = getTestSetRange(1, 1, DataLinkSemantics.CALIBRATION);
            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), Util.choseProductType(plane.getArtifacts()));
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(1.0, poly.getArea(), 0.02);
            Assert.assertEquals(20.5, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromMixed() {
        try {
            Plane plane;
            Polygon poly;

            // coordinate range
            plane = getTestSetRange(1, 1, DataLinkSemantics.SCIENCE);
            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.CALIBRATION, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            poly = (Polygon) PositionUtil.computeBounds(plane.getArtifacts(), Util.choseProductType(plane.getArtifacts()));
            Assert.assertNotNull(poly);
            Assert.assertEquals(4, poly.getPoints().size());
            Assert.assertEquals(1.0, poly.getArea(), 0.02);
            Assert.assertEquals(20.5, poly.getCenter().cval1, 0.02);
            Assert.assertEquals(10.5, poly.getCenter().cval2, 0.02);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    static Plane getTestSetRange(int numA, int numP)
        throws URISyntaxException {
        return getTestSetRange(numA, numP, DataLinkSemantics.SCIENCE);

    }

    static Plane getTestSetRange(int numA, int numP, DataLinkSemantics ptype)
        throws URISyntaxException {
        double px = 0.5;
        double py = 0.5;
        double sx = 20.0;
        double sy = 10.0;
        double dp = 1000.0;
        double ds = 1.0;

        Plane plane = new Plane("foo");
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), ptype, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                Chunk nc = new Chunk();
                np.getChunks().add(nc);
                // just shift to higher values of coordX for each subsequent chunk
                nc.position = getTestRange(px + n * dp, py, sx + n * ds, sy, dp, ds);
                n++;
            }
        }
        log.debug("getTestSetRange: " + n + " chunks");
        return plane;
    }

    static Plane getTestSetBounds(int numA, int numP)
        throws URISyntaxException {
        Plane plane = new Plane("foo");
        // TODO
        return plane;
    }

    static Plane getTestSetFunction(int numA, int numP)
        throws URISyntaxException {
        return getTestSetFunction(numA, numP, false);
    }

    static Plane getTestSetFunction(int numA, int numP, boolean gal)
        throws URISyntaxException {
        double px = 0.5;
        double py = 0.5;
        double sx = 20.0;
        double sy = 10.0;
        double ds = 1.0;
        Plane plane = new Plane("foo");
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), DataLinkSemantics.SCIENCE, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                Chunk nc = new Chunk();
                np.getChunks().add(nc);
                // just shift to higher values of sx for each subsequent chunk
                nc.position = getTestFunction(px, py, sx + n * ds, sy, gal);
                n++;
            }
        }
        log.debug("getTestSetFunction: " + n + " chunks");
        return plane;
    }

    static SpatialWCS getTestRange(double px, double py, double sx, double sy, double dp, double ds) {
        Axis axis1 = new Axis("RA", "deg");
        Axis axis2 = new Axis("DEC", "deg");
        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
        log.debug("test axis: " + axis);
        SpatialWCS wcs = new SpatialWCS(axis);
        wcs.equinox = null;

        Coord2D start = new Coord2D(new RefCoord(px, sx), new RefCoord(py, sy));
        Coord2D end = new Coord2D(new RefCoord(px + dp, sx + ds), new RefCoord(py + dp, sy + ds));
        axis.range = new CoordRange2D(start, end);
        log.debug("test range: " + axis.range);
        return wcs;
    }

    static SpatialWCS getTestFunction(double px, double py, double sx, double sy, boolean gal) {
        Axis axis1 = new Axis("RA", "deg");
        Axis axis2 = new Axis("DEC", "deg");
        if (gal) {
            axis1 = new Axis("GLON", "deg");
            axis2 = new Axis("GLAT", "deg");
        }
        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
        log.debug("test axis: " + axis);
        SpatialWCS wcs = new SpatialWCS(axis);
        wcs.coordsys = "ICRS";
        if (gal) {
            wcs.coordsys = null;
        }
        wcs.equinox = null;

        Dimension2D dim = new Dimension2D(1000, 1000); // 1000 coordX 1.0e-3 = 1 deg
        Coord2D ref = new Coord2D(new RefCoord(px, sx), new RefCoord(py, sy));
        axis.function = new CoordFunction2D(dim, ref, 1.e-3, 0.0, 0.0, 1.0e-3);
        log.debug("test function: " + axis.function);
        return wcs;
    }
}
