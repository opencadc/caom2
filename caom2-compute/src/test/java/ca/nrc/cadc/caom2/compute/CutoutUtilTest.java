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

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.PolarizationState;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.Coord2D;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordAxis2D;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordCircle2D;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordFunction2D;
import org.opencadc.caom2.wcs.CoordPolygon2D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.CoordRange2D;
import org.opencadc.caom2.wcs.Dimension2D;
import org.opencadc.caom2.wcs.ObservableAxis;
import org.opencadc.caom2.wcs.PolarizationWCS;
import org.opencadc.caom2.wcs.RefCoord;
import org.opencadc.caom2.wcs.Slice;
import org.opencadc.caom2.wcs.SpatialWCS;
import org.opencadc.caom2.wcs.SpectralWCS;
import org.opencadc.caom2.wcs.TemporalWCS;
import org.opencadc.caom2.wcs.ValueCoord2D;

/**
 * @author pdowler
 */
public class CutoutUtilTest {
    private static final Logger log = Logger.getLogger(CutoutUtilTest.class);

    private static final String TEST_CTYPE = "FARADAY";

    private ComputeDataGenerator dataGenerator = new ComputeDataGenerator();

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
    public void testInitCutout() {
        try {
            Part p = new Part("name");
            Chunk c = new Chunk();
            p.getChunks().add(c);
            StringBuilder sb;
            
            // no axes
            sb = CutoutUtil.initCutout(p.getName(), p);
            String nox = sb.toString();
            log.info("no axes: " + nox);
            Assert.assertEquals("[name][]", nox);
            
            // all axes in typical order
            c.naxis = 6;
            c.positionAxis1 = 1;
            c.positionAxis2 = 2;
            c.energyAxis = 3;
            c.timeAxis = 4;
            c.polarizationAxis = 5;
            c.observableAxis = 6;
            sb = CutoutUtil.initCutout(p.getName(), p);
            String tox = sb.toString();
            log.info("typical axes: " + tox);
            Assert.assertEquals("[name][px,py,ee,tt,pp,oo]", tox);
            
            // all axes in reverse order
            c.naxis = 6;
            c.positionAxis1 = 6;
            c.positionAxis2 = 5;
            c.energyAxis = 4;
            c.timeAxis = 3;
            c.polarizationAxis = 2;
            c.observableAxis = 1;
            sb = CutoutUtil.initCutout(p.getName(), p);
            String rox = sb.toString();
            log.info("reverse axes: " + rox);
            Assert.assertEquals("[name][oo,pp,tt,ee,py,px]", rox);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInitCutoutMultipleChunks() {
        try {
            Part p = new Part("name");
            Chunk c1 = new Chunk();
            p.getChunks().add(c1);
            Chunk c2 = new Chunk();
            StringBuilder sb;
            
            // no axes
            sb = CutoutUtil.initCutout(p.getName(), p);
            String nox = sb.toString();
            log.info("no axes: " + nox);
            Assert.assertEquals("[name][]", nox);
            
            // all axes in typical order
            c1.naxis = 2;
            c1.energyAxis = 1;
            c1.observableAxis = 2;
            // these have no effect with naxis=2
            c1.positionAxis1 = 3;
            c1.positionAxis2 = 4;
            c1.timeAxis = 5;
            c1.polarizationAxis = 6;
            
            sb = CutoutUtil.initCutout(p.getName(), p);
            String tox = sb.toString();
            log.info("one chunk of spectrum in an image array (energy+observable): " + tox);
            Assert.assertEquals("[name][ee,oo]", tox);
            
            p.getChunks().add(c2);
            c2.naxis = 2;
            c2.energyAxis = 1;
            c2.observableAxis = 2;
            
            sb = CutoutUtil.initCutout(p.getName(), p);
            tox = sb.toString();
            log.info("two chunks of spectrum in an image array (energy+observable): " + tox);
            Assert.assertEquals("[name][ee,oo]", tox);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testIllegalArgs() {
        try {
            try {
                CutoutUtil.computeCutout(null, new Circle(new Point(12, 34), 1), null, null, null, null, null);
                Assert.fail("expected IllegalArgumentException for null artifact");
            } catch (IllegalArgumentException expected) {
            }

            // this is not testable without part and chunk metadata because the Shape->convex support is in a wcs-specific
            // piece of code
            //try 
            //{ 
            //    CutoutUtil.computeCutout(new Artifact(new URI("ad", "FOO/bar", null)), new Location(new Point(1.0, 2.0)), null, null, null); 
            //    Assert.fail("expected IllegalArgumentException for Location cutout");
            //}
            //catch(IllegalArgumentException expected) { }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

        log.info("done testIllegalArgs");
    }

    @Test
    public void testComputeCutoutAll() {
        try {
            Chunk c = new Chunk();
            c.position = new SpatialWCS(new CoordAxis2D(new Axis("RA", "deg"), new Axis("DEC", "deg")));
            c.position.coordsys = "ICRS";
            c.position.getAxis().function = new CoordFunction2D(
                new Dimension2D(256, 256),
                new Coord2D(new RefCoord(128.5, 10.0), new RefCoord(128.5, 10.0)),
                1.0e-3, 0.0, 0.0, 1.0e-3);

            c.energy = new SpectralWCS(new CoordAxis1D(new Axis("WAVE", "nm")), "TOPOCENT");
            c.energy.getAxis().function = new CoordFunction1D(2500L, 0.1, new RefCoord(0.5, 300.0));  // 300-550 nm

            c.time = new TemporalWCS(new CoordAxis1D(new Axis("TIME", "d")));
            c.time.timesys = "UTC";
            c.time.getAxis().function = new CoordFunction1D(100L, 1.0, new RefCoord(0.5, 55000.0));   // 55000 to 55100 days

            c.polarization = new PolarizationWCS(new CoordAxis1D(new Axis("STOKES", null)));
            c.polarization.getAxis().function = new CoordFunction1D(3L, 1.0, new RefCoord(0.5, 1.0)); // IQU

            c.observable = new ObservableAxis(new Slice(new Axis("stuff", "quatloobs"), 1L));

            c.custom = dataGenerator.mkGoodCustomWCS();

            // axes are not bound to specific dimensions 1-5
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.positionAxis1 = 1;
            c.positionAxis2 = 2;
            c.energyAxis = 3;
            c.timeAxis = 4;
            c.polarizationAxis = 5;
            c.observableAxis = 6;
            c.customAxis = 7;

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);
            List<String> cus;

            String tmpl = "[0][STAR]";
            String cur = "";
            for (int i = 1; i <= 6; i++) {
                c.naxis = i;
                if (i > 1) {
                    cur += ",";
                }
                if (i == 6) {
                    cur += "1:1"; // internal observable cut
                } else {
                    cur += "*";
                }
                String expected = tmpl.replace("STAR", cur);
                cus = CutoutUtil.computeCutout(a, null, null, null, null, null,null);
                Assert.assertNotNull(cus);
                Assert.assertTrue(cus.size() == 1);
                String cutout = cus.get(0);
                log.debug("cutout " + i + ":" + cutout);
                Assert.assertEquals("cutout " + i, expected, cutout); // [part name][*,*] for all pixels
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

        log.info("Done testComputeCutoutAll");
    }

    @Test
    public void testCutoutPosition() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.position = new SpatialWCS(new CoordAxis2D(new Axis("RA", "deg"), new Axis("DEC", "deg")));
            c.position.coordsys = "ICRS";
            c.naxis = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            Coord2D start = new Coord2D(new RefCoord(0.5, 10.0), new RefCoord(0.5, 10.0));
            Coord2D end = new Coord2D(new RefCoord(256.0, 11.0), new RefCoord(256.0, 11.0));
            c.position.getAxis().range = new CoordRange2D(start, end);
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.position.getAxis().bounds = new CoordCircle2D(new ValueCoord2D(10.0, 11.0), 0.5);
            Assert.assertFalse(CutoutUtil.canCutout(c));

            CoordPolygon2D poly = new CoordPolygon2D();
            poly.getVertices().add(new ValueCoord2D(10.0, 10.0));
            poly.getVertices().add(new ValueCoord2D(11.0, 10.0));
            poly.getVertices().add(new ValueCoord2D(10.0, 11.0));
            c.position.getAxis().bounds = poly;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.positionAxis1 = 1;
            c.positionAxis2 = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c)); // no function

            CoordFunction2D wcsf = new CoordFunction2D(
                new Dimension2D(256, 256),
                new Coord2D(new RefCoord(128.0, 10.0), new RefCoord(128.0, 10.0)),
                1.0e-3, 0.0, 0.0, 1.0e-3);
            c.position.getAxis().function = wcsf;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // axes are meta only
            c.positionAxis1 = 3;
            c.positionAxis2 = 4;
            Assert.assertFalse(CutoutUtil.canCutout(c)); // no function

            // not enough axes in data
            c.naxis = 1;
            c.positionAxis1 = 1;
            c.positionAxis2 = 2;

            Assert.assertFalse(CutoutUtil.canCutout(c)); // no function

            c.naxis = 2;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // cleanup so these don't effect cutouts
            c.position.getAxis().range = null;
            c.position.getAxis().bounds = null;

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            Circle miss = new Circle(new Point(2.0, 2.0), 0.1);
            Circle inside = new Circle(new Point(10.0, 10.0), 1.0e-3); // 3x3 cutout
            Circle outside = new Circle(new Point(10.0, 10.0), 1.0);

            List<String> cus;

            cus = CutoutUtil.computeCutout(a, miss, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.isEmpty());

            cus = CutoutUtil.computeCutout(a, inside, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertEquals(1, cus.size());
            String cutout = cus.get(0);
            log.info("position cut: " + cutout);
            Assert.assertEquals("[0][127:129,127:129]", cutout); // 3x3 pixel in the middle of part [0]

            cus = CutoutUtil.computeCutout(a, outside, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.info("position cut: " + cutout);
            Assert.assertEquals("[0][*,*]", cutout); // [part name][*,*] for all pixels
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutPositionGAL() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.position = new SpatialWCS(new CoordAxis2D(new Axis("GLON", "deg"), new Axis("GLAT", "deg")));
            c.naxis = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c));


            c.positionAxis1 = 1;
            c.positionAxis2 = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c)); // no function

            CoordFunction2D wcsf = new CoordFunction2D(
                new Dimension2D(256, 256),
                new Coord2D(new RefCoord(128.0, 10.0), new RefCoord(128.0, 10.0)),
                1.0e-3, 0.0, 0.0, 1.0e-3);
            c.position.getAxis().function = wcsf;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            Circle miss = new Circle(new Point(2.0, 2.0), 0.1);
            Circle inside = new Circle(new Point(262.89, -15.21), 1.0e-4); // 10,10 in gal ~~ 262,-15 in ICRS
            Circle outside = new Circle(new Point(262.89, -15.21), 1.0);

            log.info("cutout: miss");
            List<String> cus = CutoutUtil.computeCutout(a, miss, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.isEmpty());

            log.info("cutout: inside -> subset");
            cus = CutoutUtil.computeCutout(a, inside, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertEquals(1, cus.size());
            String cutout = cus.get(0);
            Assert.assertEquals("[0][125:125,129:129]", cutout); // one pixel approximately in the middle of part [0]

            log.info("coutout: outside -> *,*");
            cus = CutoutUtil.computeCutout(a, outside, null, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            Assert.assertEquals("[0][*,*]", cutout); // [part name][*,*] for all pixels
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutEnergy() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.energy = new SpectralWCS(new CoordAxis1D(new Axis("WAVE", "nm")), "TOPOCENT");
            c.naxis = 1;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.energy.getAxis().range = new CoordRange1D(new RefCoord(0.5, 300.0), new RefCoord(256.5, 550.0));
            Assert.assertFalse(CutoutUtil.canCutout(c));

            CoordBounds1D bounds = new CoordBounds1D();
            bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, 300.0), new RefCoord(40.0, 350.0)));
            bounds.getSamples().add(new CoordRange1D(new RefCoord(80.0, 400.0), new RefCoord(120.0, 450.0)));
            bounds.getSamples().add(new CoordRange1D(new RefCoord(220.0, 500.0), new RefCoord(256.5, 550.0)));
            Assert.assertFalse(CutoutUtil.canCutout(c));

            CoordFunction1D function = new CoordFunction1D(2000L, 0.1, new RefCoord(0.5, 300.0)); // 300-500 nm
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.energyAxis = 1;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            // bounds cutout
            c.energy.getAxis().function = null;
            c.energy.getAxis().bounds = bounds;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // function cutout
            c.energy.getAxis().function = function;
            c.energy.getAxis().bounds = null;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // metadata only
            c.energyAxis = null;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.energyAxis = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            // restore working structure
            c.energyAxis = 1;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // cleanup so these don't effect cutouts
            c.energy.getAxis().range = null;
            c.energy.getAxis().bounds = null;

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            // cutout requests: must be wavelength in meters
            Interval<Double> miss = new Interval<Double>(600.0e-9, 800.0e-9);
            Interval<Double> inside = new Interval<Double>(399.9e-9, 400.1e-9); // 400 +- 0.1 aka 3 pixels
            Interval<Double> outside = new Interval<Double>(200.0e-9, 900.0e-9);

            List<String> cus = CutoutUtil.computeCutout(a, null, miss, null, null, null,null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.isEmpty());

            cus = CutoutUtil.computeCutout(a, null, inside, null, null, null,null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            String cutout = cus.get(0);
            log.info("energy cutout: " + cutout);
            Assert.assertEquals("[0][999:1001]", cutout); // 3 pixels

            cus = CutoutUtil.computeCutout(a, null, outside, null, null, null,null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.info("energy cutout: " + cutout);
            Assert.assertEquals("[0][*]", cutout); // all pixels
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutTime() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.time = dataGenerator.mkGoodTemporalWCS();
            c.naxis = 1;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.timeAxis = 1;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // metadata only
            c.timeAxis = null;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.timeAxis = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            // restore working structure
            c.timeAxis = 1;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            // test a range of cutout requests to make sure templating is working correctly
            // WCS has range of 200 to 400
            Interval<Double> inside = new Interval<Double>(210.0, 220.0);
            List<String> cus = CutoutUtil.computeCutout(a, null, null, inside, null, null, null);
            Assert.assertTrue(cus.size() == 1);
            String cutout = cus.get(0);
            log.debug("inside cutout: " + cutout);
            Assert.assertEquals("[0][11:20]", cutout);

            Interval<Double> outside_include = new Interval<Double>(100.0, 500.0);
            cus = CutoutUtil.computeCutout(a, null, null, outside_include, null, null, null);
            Assert.assertNotNull(cus);

            Interval<Double> overlap_lower = new Interval<Double>(100.0, 250.0);
            cus = CutoutUtil.computeCutout(a, null, null,  overlap_lower, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.debug("overlap_lower cutout: " + cutout);
            Assert.assertEquals("[0][1:50]", cutout);

            Interval<Double> overlap_upper = new Interval<Double>(300.0, 550.0);
            cus = CutoutUtil.computeCutout(a, null, null, overlap_upper, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.info("overlap_upper cutout: " + cutout);
            Assert.assertEquals("[0][101:200]", cutout);

            Interval<Double> outside_below = new Interval<Double>(100.0, 150.0);
            cus = CutoutUtil.computeCutout(a, null, null,  outside_below, null, null, null);
            Assert.assertNotNull(cus);
            log.info("overlap_upper cutout: " + cus);
            Assert.assertTrue(cus.isEmpty());

            Interval<Double> outside_above = new Interval<Double>(500.0, 650.0);
            cus = CutoutUtil.computeCutout(a, null, null, outside_above, null, null, null);
            Assert.assertNotNull(cus);
            log.debug("overlap_upper cutout: " + cus);
            Assert.assertTrue(cus.isEmpty());

            // long [0] - matches boundary exactly
            Interval<Double> includes = new Interval<Double>(200.0, 400.0);
            cus = CutoutUtil.computeCutout(a, null, null, includes, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.debug("overlap_upper cutout: " + cutout);
            Assert.assertEquals("[0][*]", cutout);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutPolarization() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));
            
            c.naxis = 1;
            c.polarization = new PolarizationWCS(new CoordAxis1D(new Axis("STOKES", null)));
            Assert.assertFalse(CutoutUtil.canCutout(c));
            
            c.polarization.getAxis().range = new CoordRange1D(new RefCoord(0.5, 1.0), new RefCoord(4.5, 4.0)); // IQUV
            Assert.assertFalse("cutout in range", CutoutUtil.canCutout(c));
            
            c.polarization.getAxis().function = new CoordFunction1D(4L, 1.0, new RefCoord(1.0, 1.0)); // IQUV
            Assert.assertFalse("cutout in metadata", CutoutUtil.canCutout(c)); // still just metadata
            
            c.polarizationAxis = 1;
            Assert.assertTrue("cutout in data", CutoutUtil.canCutout(c)); 
            
            List<PolarizationState> states = new ArrayList<PolarizationState>();
            states.add(PolarizationState.I);
            
            long[] cut = CutoutUtil.getPolarizationBounds(c.polarization, states);
            Assert.assertNotNull("cutout I", cut);
            Assert.assertEquals("cutout I pix", 2, cut.length);
            Assert.assertEquals("cutout I min pix", 1L, cut[0]);
            Assert.assertEquals("cutout I max pix", 1L, cut[1]);
            
            states.add(PolarizationState.Q);
            states.add(PolarizationState.U);
            states.add(PolarizationState.V);
            
            cut = CutoutUtil.getPolarizationBounds(c.polarization, states);
            Assert.assertNotNull("cutout IQUV", cut);
            Assert.assertEquals("cutout IQUV", 0, cut.length); // all
            
            states.clear();
            states.add(PolarizationState.RR);
            cut = CutoutUtil.getPolarizationBounds(c.polarization, states);
            Assert.assertNull("cutout RR", cut);
            
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutObservable() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canObservableCutout(c));
            
            c.energyAxis = 1;
            c.energy = new SpectralWCS(new CoordAxis1D(new Axis("WAVE", "nm")), "TOPOCENT");
            c.energy.getAxis().function = new CoordFunction1D(2000L, 0.1, new RefCoord(0.5, 300.0)); // 300-500 nm

            Slice s1 = new Slice(new Axis("foo", "m"), 1L);
            Slice s2 = new Slice(new Axis("bar", "s"), 2L);
            c.observable = new ObservableAxis(s1);
            c.observableAxis = 2;
            c.naxis = 2;
            
            Assert.assertTrue(CutoutUtil.canObservableCutout(c));

            c.observable.independent = s2;
            Assert.assertTrue(CutoutUtil.canObservableCutout(c));
            
            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);
            
            // cutout requests: must be wavelength in meters
            Interval<Double> miss = new Interval<Double>(600.0e-9, 800.0e-9);
            Interval<Double> inside = new Interval<Double>(399.9e-9, 400.1e-9); // 400 +- 0.1 aka 3 pixels
            Interval<Double> outside = new Interval<Double>(200.0e-9, 900.0e-9);

            List<String> cus = CutoutUtil.computeCutout(a, null, miss, null, null, null, null);
            Assert.assertNotNull(cus);
            for (String s : cus) {
                log.info("empty energy+observable cutout: " + s);
            }
            Assert.assertTrue(cus.isEmpty());

            cus = CutoutUtil.computeCutout(a, null, inside, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            String cutout = cus.get(0);
            log.info("energy+observable cutout: " + cutout);
            Assert.assertEquals("[0][999:1001,1:2]", cutout); // 3 pixels

            cus = CutoutUtil.computeCutout(a, null, outside, null, null, null, null);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.info("energy+observable cutout: " + cutout);
            Assert.assertEquals("[0][*,1:2]", cutout); // all pixels
            

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetCutoutFromCircle() {
        try {
            CoordAxis2D axis = new CoordAxis2D(new Axis("RA", "deg"), new Axis("DEC", "deg"));
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.getAxis().function = new CoordFunction2D(
                new Dimension2D(1000L, 1000L),
                new Coord2D(new RefCoord(500.0, 10.0), new RefCoord(500.0, 20.0)),
                1.0e-3, 0.0, 0.0, 1.0e-3); // 1x1 deg square @ 10,20

            Circle c = new Circle(new Point(9.75, 20.0), 0.05); // @250,500 radius~50 pix

            long[] cutout = CutoutUtil.getPositionBounds(wcs, c);
            Assert.assertNotNull("cutout", cutout);
            Assert.assertEquals("length 4", 4, cutout.length);
            Assert.assertEquals(200, cutout[0], 5);
            Assert.assertEquals(300, cutout[1], 5);
            Assert.assertEquals(450, cutout[2], 5);
            Assert.assertEquals(550, cutout[3], 5);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetCutoutFromPolygon() {
        try {
            CoordAxis2D axis = new CoordAxis2D(new Axis("RA", "deg"), new Axis("DEC", "deg"));
            SpatialWCS wcs = new SpatialWCS(axis);
            wcs.getAxis().function = new CoordFunction2D(
                new Dimension2D(1000L, 1000L),
                new Coord2D(new RefCoord(500.0, 10.0), new RefCoord(500.0, 20.0)),
                1.0e-3, 0.0, 0.0, 1.0e-3); // 1x1 deg square @ 10,20

            Polygon p = new Polygon();
            p.getVertices().add(new Point(9.9, 19.9));
            p.getVertices().add(new Point(10.1, 20.1));
            p.getVertices().add(new Point(9.8, 20.2));

            long[] cutout = CutoutUtil.getPositionBounds(wcs, p);
            Assert.assertNotNull("cutout", cutout);
            Assert.assertEquals("length 4", 4, cutout.length);
            Assert.assertEquals(300, cutout[0], 5);
            Assert.assertEquals(600, cutout[1], 5);
            Assert.assertEquals(400, cutout[2], 5);
            Assert.assertEquals(700, cutout[3], 5);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testCutoutCustomSimple() {
        try {
            Chunk c = new Chunk();
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.custom = dataGenerator.mkGoodCustomWCS();
            c.naxis = 1;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.customAxis = 1;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            // metadata only
            c.customAxis = null;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            c.customAxis = 2;
            Assert.assertFalse(CutoutUtil.canCutout(c));

            // restore working structure
            c.customAxis = 1;
            Assert.assertTrue("can cutout", CutoutUtil.canCutout(c));

            c.custom.getAxis().range = null;

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            // test a range of cutout requests to make sure templating is working correctly
            // WCS has range of 200 to 400
            Interval<Double> inside = new Interval<Double>(210.0, 220.0);
            List<String> cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, inside);
            Assert.assertTrue(cus.size() == 1);
            String cutout = cus.get(0);
            log.debug("inside cutout: " + cutout);
            Assert.assertEquals("[0][11:20]", cutout);

            Interval<Double> outside_include = new Interval<Double>(100.0, 500.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, outside_include);
            Assert.assertNotNull(cus);

            Interval<Double> overlap_lower = new Interval<Double>(100.0, 250.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, overlap_lower);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.debug("overlap_lower cutout: " + cutout);
            Assert.assertEquals("[0][1:50]", cutout);

            Interval<Double> overlap_upper = new Interval<Double>(300.0, 550.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, overlap_upper);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.debug("overlap_upper cutout: " + cutout);
            Assert.assertEquals("[0][101:200]", cutout);

            Interval<Double> outside_below = new Interval<Double>(100.0, 150.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, outside_below);
            Assert.assertNotNull(cus);
            log.debug("overlap_upper cutout: " + cus);
            Assert.assertTrue(cus.isEmpty());

            Interval<Double> outside_above = new Interval<Double>(500.0, 650.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, outside_above);
            Assert.assertNotNull(cus);
            log.debug("overlap_upper cutout: " + cus);
            Assert.assertTrue(cus.isEmpty());

            // long [0] - matches boundary exactly
            Interval<Double> includes = new Interval<Double>(200.0, 400.0);
            cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, includes);
            Assert.assertNotNull(cus);
            Assert.assertTrue(cus.size() == 1);
            cutout = cus.get(0);
            log.debug("overlap_upper cutout: " + cutout);
            Assert.assertEquals("[0][*]", cutout);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutCustom() {
        try {
            Chunk c = new Chunk();
            c.naxis = 3;

            c.energyAxis = 1;
            c.energy = dataGenerator.mkGoodSpectralWCS();

            c.positionAxis1 = 2;
            c.position = dataGenerator.mkGoodSpatialWCS();

            c.customAxis = 3;
            c.custom = dataGenerator.mkGoodCustomWCS();

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            // test a single cutout request to make sure templating is working correctly
            // full suite of tests is in another function
            Interval<Double> inside = new Interval<Double>(210.0, 220.0);
            List<String> cus = CutoutUtil.computeCutout(a, null, null, null, null, TEST_CTYPE, inside);
            Assert.assertTrue(cus.size() == 1);
            String cutout = cus.get(0);
            Assert.assertEquals("[0][*,*,11:20]", cutout);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCutoutCustomCtypeNotFound() {
        try {
            Chunk c = new Chunk();
            c.naxis = 3;

            c.energyAxis = 1;
            c.energy = dataGenerator.mkGoodSpectralWCS();

            c.positionAxis1 = 2;
            c.position = dataGenerator.mkGoodSpatialWCS();

            c.customAxis = 3;
            c.custom = dataGenerator.mkGoodCustomWCS();

            Artifact a = new Artifact(new URI("ad", "FOO/bar", null), DataLinkSemantics.THIS, ReleaseType.DATA);
            Part p = new Part(0);
            a.getParts().add(p);
            p.getChunks().add(c);

            // test a single cutout request to make sure templating is working correctly
            // full suite of tests is in another function
            Interval<Double> inside = new Interval<Double>(210.0, 220.0);
            List<String> cus = CutoutUtil.computeCutout(a, null, null, null, null, "RM", inside);
            Assert.assertTrue(cus.size() == 0);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
