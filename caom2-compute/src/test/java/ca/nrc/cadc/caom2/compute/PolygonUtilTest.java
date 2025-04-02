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

import ca.nrc.cadc.caom2.compute.types.MultiPolygon;
import ca.nrc.cadc.caom2.compute.types.SegmentType;
import ca.nrc.cadc.caom2.compute.types.Vertex;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author pdowler
 */
public class PolygonUtilTest {
    private static final Logger log = Logger.getLogger(PolygonUtilTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }


    ////@Test
    public void testTemplate() {
        try {
            // TODO
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUnionOverlap() {
        try {
            MultiPolygon p1 = new MultiPolygon(); // 2x2 at 2,2
            p1.getVertices().add(new Vertex(1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(3.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(1.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            MultiPolygon p2 = new MultiPolygon(); // 2x2 at 3,3
            p2.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(4.0, 2.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(4.0, 4.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(2.0, 4.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            // area = 7 center=2.5,2.5
            MultiPolygon actual = PolygonUtil.doUnionCAG(p1, p2);

            log.debug("testUnionDisjoint: " + p1);
            log.debug("testUnionDisjoint: " + p2);
            log.debug("testUnionDisjoint: " + actual);


            Assert.assertNotNull(actual);
            Assert.assertTrue(actual.isSimple());
            Assert.assertEquals(9, actual.getVertices().size());

            Point center = actual.getCenter();
            Double area = actual.getArea();

            Assert.assertNotNull(center);
            Assert.assertEquals(2.5, center.getLongitude(), 0.01);
            Assert.assertEquals(2.5, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(7.0, area, 0.01);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUnionDisjoint() {
        try {
            double x = 30.0;
            MultiPolygon p1 = new MultiPolygon(); // 2x2 at 2,2
            p1.getVertices().add(new Vertex(x + 1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(x + 3.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(x + 3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(x + 1.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            MultiPolygon p2 = new MultiPolygon(); // 2x2 at 6,6
            p2.getVertices().add(new Vertex(x + 5.0, 5.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(x + 7.0, 5.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(x + 7.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(x + 5.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            // area = 8 center=4,4
            MultiPolygon actual = PolygonUtil.doUnionCAG(p1, p2);

            log.debug("testUnionDisjoint: " + p1);
            log.debug("testUnionDisjoint: " + p2);
            log.debug("testUnionDisjoint: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertFalse(actual.isSimple());
            Assert.assertEquals(10, actual.getVertices().size());

            Point center = actual.getCenter();
            Double area = actual.getArea();

            Assert.assertNotNull(center);
            Assert.assertEquals(x + 4.0, center.getLongitude(), 0.02);
            Assert.assertEquals(4.0, center.getLatitude(), 0.02);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.02);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetConcaveHullFromHole() {
        try {
            // TODO: test polygon with a hole in it
            MultiPolygon p1 = new MultiPolygon(); // 3x3 at 2.5,2.5 with 1x1 hole in middle
            // outside: ccw
            p1.getVertices().add(new Vertex(1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(4.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(4.0, 4.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(1.0, 4.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            // hole: cw
            p1.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(2.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 2.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            log.debug("testGetConcaveHullFromHole: " + p1);

            Assert.assertFalse(p1.isSimple());
            Point center = p1.getCenter();
            Double area = p1.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(2.5, center.getLongitude(), 0.01);
            Assert.assertEquals(2.5, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.01);

            Polygon actual = PolygonUtil.getConcaveHull(p1);
            log.debug("testGetConcaveHullFromHole: " + actual);

            center = actual.getCenter();
            area = actual.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(2.5, center.getLongitude(), 0.01);
            Assert.assertEquals(2.5, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(9.0, area, 0.02);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetConcaveHullFromClose() {
        try {
            MultiPolygon p1 = new MultiPolygon(); // 2x2
            p1.getVertices().add(new Vertex(1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(3.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(1.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            MultiPolygon p2 = new MultiPolygon(); // 2x2 slightly to right
            p2.getVertices().add(new Vertex(3.1, 1.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(5.1, 1.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(5.1, 3.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(3.1, 3.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            log.debug("testGetConcaveHullFromClose: " + p1);
            log.debug("testGetConcaveHullFromClose: " + p2);

            MultiPolygon union = PolygonUtil.doUnionCAG(p1, p2);
            Assert.assertNotNull(union);
            log.debug("testGetConcaveHullFromClose: " + union);
            Assert.assertFalse(union.isSimple());
            Point center = union.getCenter();
            Double area = union.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(3.05, center.getLongitude(), 0.01); // mid between 2 and 4.1
            Assert.assertEquals(2.0, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.01); // 4 + 4

            log.debug("computing concave hull from " + union);

            Polygon actual = PolygonUtil.getConcaveHull(union);
            Assert.assertNotNull(actual);
            log.info("testGetConcaveHullFromClose: " + actual);
            Assert.assertEquals(4, actual.getVertices().size());

            center = actual.getCenter();
            area = actual.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(3.05, center.getLongitude(), 0.01); // mid between 2 and 4.1
            Assert.assertEquals(2.0, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0 + 2 * 0.1, area, 0.1); // 4 + 4 + gap

            // more complex scenario: p2 slightly to the right and up by 1
            p2 = new MultiPolygon(); // 2x2 slightly to right
            p2.getVertices().add(new Vertex(3.02, 2.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(5.02, 2.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(5.02, 4.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(3.02, 4.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            log.debug("testGetConcaveHullFromClose: " + p1);
            log.debug("testGetConcaveHullFromClose: " + p2);

            union = PolygonUtil.doUnionCAG(p1, p2);
            Assert.assertNotNull(union);
            log.debug("testGetConcaveHullFromClose: " + union);
            Assert.assertFalse(union.isSimple());
            center = union.getCenter();
            area = union.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(3.01, center.getLongitude(), 0.01);
            Assert.assertEquals(2.5, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.01); // 4 + 4

            log.debug("computing concave hull from " + union);

            actual = PolygonUtil.getConcaveHull(union);
            Assert.assertNotNull(actual);
            log.debug("testGetConcaveHullFromClose: " + actual);
            Assert.assertEquals(8, actual.getVertices().size());

            center = actual.getCenter();
            area = actual.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(3.01, center.getLongitude(), 0.01); // mid between 2 and 4.1
            Assert.assertEquals(2.5, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0 + 2 * 0.02, area, 0.1); // 4 + 4 + gap
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetConcaveHullFromFar() {
        try {
            MultiPolygon p1 = new MultiPolygon(); // 2x2
            p1.getVertices().add(new Vertex(1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(3.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(1.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            MultiPolygon p2 = new MultiPolygon(); // 2x2 above and to the right
            p2.getVertices().add(new Vertex(5.0, 5.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(5.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(7.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(7.0, 5.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            log.debug("testGetConcaveHullFromFar: " + p1);
            log.debug("testGetConcaveHullFromFar: " + p2);

            MultiPolygon union = PolygonUtil.doUnionCAG(p1, p2);
            Assert.assertNotNull(union);
            log.debug("testGetConcaveHullFromFar: " + union);
            Assert.assertFalse(union.isSimple());
            Point center = union.getCenter();
            Double area = union.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(4.0, center.getLongitude(), 0.01);
            Assert.assertEquals(4.0, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.02); // 4 + 4

            log.debug("computing concave hull from " + union);

            Polygon actual = PolygonUtil.getConcaveHull(union);
            Assert.assertNull(actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetConvexHull() {
        try {
            MultiPolygon p1 = new MultiPolygon(); // 2x2
            p1.getVertices().add(new Vertex(1.0, 1.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(3.0, 1.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(1.0, 3.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            MultiPolygon p2 = new MultiPolygon(); // 2x2 above and to the right
            p2.getVertices().add(new Vertex(5.0, 5.0, SegmentType.MOVE));
            p2.getVertices().add(new Vertex(5.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(7.0, 7.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(7.0, 5.0, SegmentType.LINE));
            p2.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));

            log.debug("testGetConvexHull: " + p1);
            log.debug("testGetConvexHull: " + p2);

            MultiPolygon union = PolygonUtil.doUnionCAG(p1, p2);
            Assert.assertNotNull(union);
            log.debug("testGetConvexHull: " + union);
            Assert.assertFalse(union.isSimple());
            Point center = union.getCenter();
            Double area = union.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(4.0, center.getLongitude(), 0.01);
            Assert.assertEquals(4.0, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(8.0, area, 0.02); // 4 + 4

            log.debug("computing convex hull from " + union);

            Polygon actual = PolygonUtil.getConvexHull(union);
            Assert.assertNotNull(actual);
            Assert.assertEquals("convex hull num verts", 6, actual.getVertices().size());
            center = actual.getCenter();
            area = actual.getArea();
            Assert.assertNotNull(center);
            Assert.assertEquals(4.0, center.getLongitude(), 0.01);
            Assert.assertEquals(4.0, center.getLatitude(), 0.01);

            Assert.assertNotNull(area);
            Assert.assertEquals(20.0, area, 0.1); // 4 + 4 + 12

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
