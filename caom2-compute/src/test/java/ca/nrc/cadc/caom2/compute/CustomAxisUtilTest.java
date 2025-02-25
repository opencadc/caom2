/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.CustomAxis;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.CustomWCS;
import org.opencadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hjeeves
 */
public class CustomAxisUtilTest {

    private static final Logger log = Logger.getLogger(CustomAxisUtilTest.class);
    public static final String TEST_CTYPE = "FARADAY";
    public static final String TEST_RM_CTYPE = "RM";
    public static final String TEST_CUNIT = "rad/m**2";
    public static final String TEST_ALT_CUNIT = "rad/m^2";
    public static final String TEST_INVALID_CUNIT = "Hz";

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.types", Level.INFO);
    }

    @Test
    public void testMixedCtype() {
        try {
            // All chunks within artifact must have same ctype. Get one that has
            // mixed ctypes that are all valid
            Plane plane = getMixedTestSetRange(1, 2, 6, DataLinkSemantics.THIS);

            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.fail("zeroErr -- expected IllegalArgumentException. Validator passed when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("zeroErr -- caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidCtype() {
        try {
            Plane plane = getTestSetRange(1, 2, 3, DataLinkSemantics.THIS, "foo", "foo_unit");
            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.fail("zeroErr -- expected IllegalArgumentException. Validator passed when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("zeroErr -- caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidCunit() {
        try {
            Plane plane = getTestSetRange(1, 2, 3, DataLinkSemantics.THIS, TEST_RM_CTYPE, TEST_INVALID_CUNIT);
            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.fail("zeroErr -- expected IllegalArgumentException. Validator passed when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("zeroErr -- caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAltCunit() {
        try {
            // Normalization should happen with the alternate cunits,
            // compute should still pass
            Plane plane = getTestSetRange(1, 2, 3, DataLinkSemantics.THIS, TEST_CTYPE, TEST_ALT_CUNIT);
            log.info("plane: " + plane);
            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNoComputableChunks() {
        // The product type used in the set range here will not be detected by the
        // Util.choseProductType90 function in the compute function, so it
        // should return a null
        try {
            Plane plane = getTestSetRange(1, 2, 3, DataLinkSemantics.DARK);
            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.assertNull(ca);
        } catch (IllegalArgumentException expected) {
            log.info("zeroErr -- caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testEmptyList() {
        try {
            Plane plane = new Plane(URI.create("caom:FOO/foo"));
            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.assertNull(ca);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSkippableCompute() {
        try {
            Plane plane = getTestSetFunction(1, 1, 1, false);
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            CoordAxis1D axis = new CoordAxis1D(new Axis(TEST_CTYPE, TEST_CUNIT));
            c.custom = new CustomWCS(axis);

            CustomAxis ca = CustomAxisUtil.compute(plane.getArtifacts());

            Assert.assertNull("no custom bounds", ca);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromRange() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetRange(1, 1, 1);
            actual = CustomAxisUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromRange: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.0001);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromBounds() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L * 2 / 3;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetBounds(1, 1, 1);
            actual = CustomAxisUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromBounds: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.0001);
            Assert.assertEquals(2, actual.getSamples().size());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue(), 1L);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromFunction() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetFunction(1, 1, 1, false);
            actual = CustomAxisUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromFunction: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.0001);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFunctionDeltaZero() {
        try {
            CoordAxis1D axis = new CoordAxis1D(new Axis(TEST_CTYPE, TEST_CUNIT));
            CustomWCS wcs = new CustomWCS(axis);
            wcs.getAxis().function = new CoordFunction1D(10L, 0.0, new RefCoord(0.5, 54321.0));
            DoubleInterval i = CustomAxisUtil.toInterval(wcs, wcs.getAxis().function);
            Assert.fail("expected IllegalArgumentException, got: " + i);
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

        try {
            CoordAxis1D axis = new CoordAxis1D(new Axis(TEST_CTYPE, TEST_CUNIT));
            CustomWCS wcs = new CustomWCS(axis);
            // delata==0 allowed for single bin
            wcs.getAxis().function = new CoordFunction1D(1L, 0.0, new RefCoord(0.5, 54321.0));
            Interval i = CustomAxisUtil.toInterval(wcs, wcs.getAxis().function);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

    }

    @Test
    public void testComputeFromMultipleFunction() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 2 * 200 * 0.01;
            long expectedDimension = 2 * 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetFunction(1, 2, 1, true);
            actual = CustomAxisUtil.compute(plane.getArtifacts());
            log.info("testComputeFromFunction: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.0001);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromMultipleFunctionOverlap() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetFunction(1, 2, 1, false);
            actual = CustomAxisUtil.compute(plane.getArtifacts());
            log.info("testComputeFromMultipleFunctionOverlap: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromScience() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetRange(1, 1, 1, DataLinkSemantics.THIS);

            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            actual = CustomAxisUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromScience: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromCalibration() {
        try {
            Plane plane = getTestSetRange(1, 1, 1, DataLinkSemantics.CALIBRATION);

            // add some aux artifacts, should not affect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            CustomAxis actual = CustomAxisUtil.compute(plane.getArtifacts());
            Assert.assertNull(actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromMixed() {
        try {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200 * 0.01;
            long expectedDimension = 200L;

            Plane plane;
            CustomAxis actual;

            plane = getTestSetRange(1, 1, 1, DataLinkSemantics.THIS);

            // add some CAL artifacts, should not effect result since SCIENCE above
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.CALIBRATION, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            actual = CustomAxisUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromScience: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    Plane getTestSetRange(int numA, int numP, int numC)
            throws URISyntaxException {
        return getTestSetRange(numA, numP, numC, DataLinkSemantics.THIS);
    }

    Plane getTestSetRange(int numA, int numP, int numC, DataLinkSemantics ptype)
            throws URISyntaxException {
        return getTestSetRange(numA, numP, numC, ptype, TEST_CTYPE, TEST_CUNIT);
    }

    Plane getTestSetRange(int numA, int numP, int numC, DataLinkSemantics ptype, String ctype, String cunit)
            throws URISyntaxException {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), ptype, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // just shift to higher values of coordX for each subsequent chunk
                    // Invalid ctype & unit used
                    nc.custom = getTestRange(px, sx + n * nx * ds, nx, ds, ctype, cunit);
                    n++;
                }
            }
        }
        log.debug("getInvalidTestSetRange: " + n + " chunks");
        return plane;
    }

    Plane getMixedTestSetRange(int numA, int numP, int numC, DataLinkSemantics ptype)
            throws URISyntaxException {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), ptype, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(p);
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    // just shift to higher values of coordX for each subsequent chunk
                    // Both ctypes are valid, just alternating - a case that shouldn't happen
                    if ((c % 2) == 0) {
                        nc.custom = getTestRange(px, sx + n * nx * ds, nx, ds, TEST_CTYPE, TEST_CUNIT);
                    } else {
                        nc.custom = getTestRange(px, sx + n * nx * ds, nx, ds, TEST_RM_CTYPE, TEST_CUNIT);
                    }
                    np.getChunks().add(nc);
                    n++;
                }
            }
        }
        log.debug("getMixedTestSetRange: " + n + " chunks");
        return plane;
    }

    Plane getMixedProductTypeRange(int numA, int numP, int numC)
            throws URISyntaxException {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        DataLinkSemantics ptype;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            if ((a % 2) == 0) {
                ptype = DataLinkSemantics.CALIBRATION;
            } else {
                ptype = DataLinkSemantics.THIS;
            }
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), ptype, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(p);
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    // just shift to higher values of coordX for each subsequent chunk
                    // Both ctypes are valid, just alternating - a case that shouldn't happen
                    nc.custom = getTestRange(px, sx + n * nx * ds, nx, ds, TEST_RM_CTYPE, TEST_ALT_CUNIT);
                    np.getChunks().add(nc);
                    n++;
                }
            }
        }
        log.debug("getMixedProductTypeRange: " + n + " chunks");
        return plane;
    }

    Plane getTestSetBounds(int numA, int numP, int numC)
            throws URISyntaxException {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), DataLinkSemantics.THIS, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(p);
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // just shift to higher values of coordX for each subsequent chunk
                    nc.custom = getTestBounds(px, sx + n * nx * ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetBounds: " + n + " chunks");
        return plane;
    }

    Plane getTestSetFunction(int numA, int numP, int numC, boolean shift)
            throws URISyntaxException {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), DataLinkSemantics.THIS, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(p);
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    nc.custom = getTestFunction(px, sx + n * nx * ds, nx, ds);
                    if (shift) {
                        n++;
                    }
                }
            }
        }
        log.debug("getTestSetFunction: " + n + " chunks");
        return plane;
    }

    CustomWCS getTestRange(double px, double sx, double nx, double ds, String ctype, String cunit) {
        CoordAxis1D axis = new CoordAxis1D(new Axis(ctype, cunit));
        CustomWCS wcs = new CustomWCS(axis);

        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx, sx + nx * ds);
        wcs.getAxis().range = new CoordRange1D(c1, c2);

        return wcs;
    }

    CustomWCS getTestBounds(double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis(TEST_CTYPE, TEST_CUNIT));
        CustomWCS wcs = new CustomWCS(axis);

        // divide into 2 samples with a gap between
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx * 0.33, sx + nx * ds * 0.33);
        RefCoord c3 = new RefCoord(px + nx * 0.66, sx + nx * ds * 0.66);
        RefCoord c4 = new RefCoord(px + nx, sx + nx * ds);
        wcs.getAxis().bounds = new CoordBounds1D();
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));

        return wcs;
    }

    CustomWCS getTestFunction(double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis(TEST_CTYPE, TEST_CUNIT));
        CustomWCS wcs = new CustomWCS(axis);

        RefCoord c1 = new RefCoord(px, sx);
        wcs.getAxis().function = new CoordFunction1D((long) nx, ds, c1);

        return wcs;
    }

    // TODO: add getBounds test as in EnergyUtilTest. testGetBoundsWaveBounds()
}
