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

import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyBand;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.util.EnergyConverter;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.RefCoord;
import org.opencadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author pdowler
 */
public class EnergyUtilTest {
    private static final Logger log = Logger.getLogger(EnergyUtilTest.class);
    private static ComputeDataGenerator dataGenerator = new ComputeDataGenerator();

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    String BANDPASS_NAME = "H-Alpha-narrow";
    EnergyTransition TRANSITION = new EnergyTransition("H", "alpha");
    EnergyConverter econv = new EnergyConverter();

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
    public void testEmptyList() {
        try {
            Plane plane = new Plane(URI.create("caom:FOO/foo"));
            Energy nrg = EnergyUtil.compute(plane.getArtifacts());
            Assert.assertNull(nrg);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSkippableCompute() {
        log.debug("testSkippableCompute: START");
        try {
            Plane plane = getTestSetFunction(1, 1, 1);
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            CoordAxis1D axis = new CoordAxis1D(new Axis("WAV", "m"));
            c.energy = new SpectralWCS(axis, "TOPOCENT");
            
            Energy e = EnergyUtil.compute(plane.getArtifacts());

            Assert.assertNull("no energy bounds", e);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromRange() {
        log.debug("testComputeFromRange: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200L;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetRange(1, 1, 1);
            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromRange: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.resolvingPower);
            Assert.assertEquals(expectedRP, actual.resolvingPower.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertEquals(BANDPASS_NAME, actual.bandpassName);
            Assert.assertEquals(TRANSITION, actual.transition);
            Assert.assertTrue(actual.getEnergyBands().contains(EnergyBand.OPTICAL));
            Assert.assertNotNull(actual.rest);
            Assert.assertEquals(6563.0e-10, actual.rest, 1.0e-11);

            plane = getTestSetRange(1, 3, 1);
            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromRange: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.getBounds());
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB + 400.0e-9, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension * 3, actual.dimension.longValue());
            Assert.assertNotNull(actual.resolvingPower);
            Assert.assertEquals(expectedRP, actual.resolvingPower.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertEquals(BANDPASS_NAME, actual.bandpassName);
            Assert.assertEquals(TRANSITION, actual.transition);
            Assert.assertTrue(actual.getEnergyBands().contains(EnergyBand.OPTICAL));
            Assert.assertNotNull(actual.rest);
            Assert.assertEquals(6563.0e-10, actual.rest, 1.0e-11);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromBounds() {
        log.debug("testComputeFromBounds: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200 * 2 / 3;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetBounds(1, 1, 1);
            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromBounds: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.getBounds());
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertEquals(2, actual.getSamples().size());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue(), 1L);
            Assert.assertNotNull(actual.resolvingPower);
            Assert.assertEquals(expectedRP, actual.resolvingPower.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertEquals(BANDPASS_NAME, actual.bandpassName);
            Assert.assertEquals(TRANSITION, actual.transition);
            Assert.assertTrue(actual.getEnergyBands().contains(EnergyBand.OPTICAL));
            Assert.assertNotNull(actual.rest);
            Assert.assertEquals(6563.0e-10, actual.rest, 1.0e-11);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromFunction() {
        log.debug("testComputeFromFunction: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200L;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetFunction(1, 1, 1);
            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromFunction: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.getBounds());
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.resolvingPower);
            Assert.assertEquals(expectedRP, actual.resolvingPower.doubleValue(), 1.0);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertEquals(BANDPASS_NAME, actual.bandpassName);
            Assert.assertEquals(TRANSITION, actual.transition);
            Assert.assertTrue(actual.getEnergyBands().contains(EnergyBand.OPTICAL));
            Assert.assertNotNull(actual.rest);
            Assert.assertEquals(6563.0e-10, actual.rest, 1.0e-11);

            Assert.assertNotNull(actual.getFreqWidth());
            Assert.assertNotNull(actual.getFreqSampleSize());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromFreqRange() {
        try {
            EnergyConverter ec = new EnergyConverter();
            double expectedLB = ec.convert(20.0, "FREQ", "MHz");
            double expectedUB = ec.convert(2.0, "FREQ", "MHz");
            long expectedDimension = 1024L;
            double expectedSS = (expectedUB - expectedLB) / expectedDimension;

            CoordAxis1D axis = new CoordAxis1D(new Axis("FREQ", "MHz"));
            SpectralWCS spec = new SpectralWCS(axis, "TOPOCENT");
            spec.restfrq = econv.toHz(6563.0e-10, "m");

            RefCoord c1 = new RefCoord(0.5, 2.0);
            RefCoord c2 = new RefCoord(1024.5, 20.0);
            spec.getAxis().range = new CoordRange1D(c1, c2); //[2,20] MHz
            log.debug("testComputeFromFreqRange: " + spec.getAxis().range + " " + spec.getAxis().getAxis().getCunit());

            // use this to create the objects, then replace the single SpectralWCS with above
            Plane plane = getTestSetRange(1, 1, 1);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            c.energy = spec;

            Energy actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromFreqRange: " + actual);

            Assert.assertNotNull(actual);

            Assert.assertNotNull(actual.getBounds());
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertNotNull(actual.rest);
            Assert.assertEquals(6563.0e-10, actual.rest, 1.0e-11);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidComputeFromFunction() {
        log.debug("testComputeFromFunction: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200L;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetFunction(1, 1, 1);
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            c.energy = dataGenerator.mkBadSpectralWCSFn(); // replace the func
            actual = EnergyUtil.compute(plane.getArtifacts());

            Assert.fail("expected WCSlibRuntimeException");

        } catch (WCSLibRuntimeException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetBoundsWaveBounds() {
        try {
            SpectralWCS wcs = getTestBounds(false, 0.5, 400, 2000.0, 0.1); // [400,600] in two samples [400,466][534,600]
            DoubleInterval all = new DoubleInterval(300e-9, 800e-9);
            DoubleInterval none = new DoubleInterval(700e-9, 900e-9);
            DoubleInterval gap = new DoubleInterval(480e-9, 520e-9);
            DoubleInterval cut = new DoubleInterval(420e-9, 440e-9);
            DoubleInterval clip = new DoubleInterval(300e-9, 500e-9);

            long[] allPix = CutoutUtil.getEnergyBounds(wcs, all);
            Assert.assertNotNull(allPix);
            Assert.assertEquals("long[0]", 0, allPix.length);

            long[] noPix = CutoutUtil.getEnergyBounds(wcs, none);
            Assert.assertNull(noPix);

            long[] gapPix = CutoutUtil.getEnergyBounds(wcs, gap);
            Assert.assertNull(gapPix);

            long[] cutPix = CutoutUtil.getEnergyBounds(wcs, cut);
            Assert.assertNotNull(cutPix);
            Assert.assertEquals("long[2]", 2, cutPix.length);
            Assert.assertEquals("cut LB", 0, cutPix[0], 1);
            Assert.assertEquals("cut LB", 660, cutPix[1], 1);

            long[] clipPix = CutoutUtil.getEnergyBounds(wcs, clip);
            Assert.assertNotNull(clipPix);
            Assert.assertEquals("long[2]", 2, clipPix.length);
            Assert.assertEquals("clip LB", 0, clipPix[0], 1);
            Assert.assertEquals("clip LB", 660, clipPix[1], 1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetBoundsWaveFunc() {
        try {
            SpectralWCS wcs = getTestFunction(false, 0.5, 400, 2000.0, 0.1); // [400,600]
            DoubleInterval all = new DoubleInterval(300e-9, 800e-9);
            DoubleInterval none = new DoubleInterval(700e-9, 900e-9);
            DoubleInterval cut = new DoubleInterval(450e-9, 550e-9);

            long[] allPix = CutoutUtil.getEnergyBounds(wcs, all);
            Assert.assertNotNull(allPix);
            Assert.assertEquals("long[0]", 0, allPix.length);

            long[] noPix = CutoutUtil.getEnergyBounds(wcs, none);
            Assert.assertNull(noPix);

            long[] cutPix = CutoutUtil.getEnergyBounds(wcs, cut);
            Assert.assertNotNull(cutPix);
            Assert.assertEquals("long[2]", 2, cutPix.length);
            Assert.assertEquals("cut LB", 500, cutPix[0], 1);
            Assert.assertEquals("cut LB", 1500, cutPix[1], 1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetBoundsFreqBounds() {
        try {
            SpectralWCS wcs = getTestFreqBounds(false, 0.5, 10.0, 1000.0, 0.1); // [10-110] MHz in [1,333][666,1000]
            DoubleInterval all = new DoubleInterval(2.5, 60.0);
            DoubleInterval none = new DoubleInterval(1.0, 2.0);
            DoubleInterval cut = new DoubleInterval(2.8, 3.2);

            long[] allPix = CutoutUtil.getEnergyBounds(wcs, all);
            Assert.assertNotNull(allPix);
            Assert.assertEquals("long[0]", 0, allPix.length);

            long[] noPix = CutoutUtil.getEnergyBounds(wcs, none);
            Assert.assertNull(noPix);

            long[] cutPix = CutoutUtil.getEnergyBounds(wcs, cut);
            Assert.assertNotNull(cutPix);
            Assert.assertEquals("long[2]", 2, cutPix.length);
            log.debug("cut: " + cutPix[0] + ":" + cutPix[1]);
            Assert.assertEquals("cut LB", 660, cutPix[0], 1); // short wave end is in the upper sub-interval
            Assert.assertEquals("cut UB", 1000, cutPix[1], 1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetBoundsFreqFunc() {
        try {
            SpectralWCS wcs = getTestFreqFunction(false, 0.5, 10.0, 1000.0, 0.1); // [10-110] MHz
            DoubleInterval all = new DoubleInterval(2.5, 60.0);
            DoubleInterval none = new DoubleInterval(1.0, 2.0);
            DoubleInterval cut = new DoubleInterval(4.283, 7.5);

            long[] allPix = CutoutUtil.getEnergyBounds(wcs, all);
            Assert.assertNotNull(allPix);
            Assert.assertEquals("long[0]", 0, allPix.length);

            long[] noPix = CutoutUtil.getEnergyBounds(wcs, none);
            Assert.assertNull(noPix);

            long[] cutPix = CutoutUtil.getEnergyBounds(wcs, cut);
            Assert.assertNotNull(cutPix);
            Assert.assertEquals("long[2]", 2, cutPix.length);
            log.debug("cut: " + cutPix[0] + ":" + cutPix[1]);
            Assert.assertEquals("cut LB", 300, cutPix[0], 1);
            Assert.assertEquals("cut UB", 600, cutPix[1], 1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromCalibration() {
        log.debug("testComputeFromCalibration: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200L;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetRange(1, 1, 1, DataLinkSemantics.CALIBRATION);
            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromCalibration: " + actual);
            Assert.assertNull(actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromMixed() {
        log.debug("testComputeFromMixed: START");
        try {
            double expectedLB = 400e-9;
            double expectedUB = 600e-9;
            long expectedDimension = 200L;
            double expectedSS = 1.0e-9;
            double expectedRP = 33000.0;

            Plane plane;
            Energy actual;

            plane = getTestSetRange(1, 1, 1, DataLinkSemantics.THIS);

            // add some cal artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), DataLinkSemantics.CALIBRATION, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);

            actual = EnergyUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromMixed: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.getBounds());
            Assert.assertEquals(expectedLB, actual.getBounds().getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.getBounds().getUpper(), 0.01);
            Assert.assertFalse(actual.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.resolvingPower);
            Assert.assertEquals(expectedRP, actual.resolvingPower.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
            Assert.assertEquals(BANDPASS_NAME, actual.bandpassName);
            Assert.assertEquals(TRANSITION, actual.transition);
            Assert.assertTrue(actual.getEnergyBands().contains(EnergyBand.OPTICAL));
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
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;
        double tot = nx * ds;

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
                    nc.energy = getTestRange(true, px, sx + n * nx * ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetRange: " + n + " chunks");
        return plane;
    }

    Plane getTestSetBounds(int numA, int numP, int numC)
        throws URISyntaxException {
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;
        double tot = nx * ds;

        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), DataLinkSemantics.THIS, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // just shift to higher values of coordX for each subsequent chunk
                    nc.energy = getTestBounds(true, px, sx + n * nx * ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetBounds: " + n + " chunks");
        return plane;
    }

    Plane getTestSetFunction(int numA, int numP, int numC)
        throws URISyntaxException {
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;
        double tot = nx * ds;
        Plane plane = new Plane(URI.create("caom:FOO/foo"));
        int n = 0;
        for (int a = 0; a < numA; a++) {
            Artifact na = new Artifact(new URI("foo", "bar" + a, null), DataLinkSemantics.THIS, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p = 0; p < numP; p++) {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c = 0; c < numC; c++) {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // shift px to larger values
                    nc.energy = getTestFunction(true, px, sx + n * nx * ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetFunction: " + n + " chunks");
        return plane;
    }


    SpectralWCS getTestRange(boolean complete, double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis("WAVE", "nm"));
        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        if (complete) {
            wcs.bandpassName = BANDPASS_NAME;
            wcs.restwav = 6563.0e-10; // meters
            wcs.resolvingPower = 33000.0;
            wcs.transition = TRANSITION;
        }

        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx, sx + nx * ds);
        wcs.getAxis().range = new CoordRange1D(c1, c2);
        log.debug("test range: " + axis.range);
        return wcs;
    }

    SpectralWCS getTestBounds(boolean complete, double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis("WAVE", "nm"));
        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        if (complete) {
            wcs.bandpassName = BANDPASS_NAME;
            wcs.restwav = 6563.0e-10; // meters
            wcs.resolvingPower = 33000.0;
            wcs.transition = TRANSITION;
        }
        // divide into 2 samples with a gap between
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx * 0.33, sx + nx * 0.33 * ds);
        RefCoord c3 = new RefCoord(px + nx * 0.66, sx + nx * 0.66 * ds);
        RefCoord c4 = new RefCoord(px + nx, sx + nx * ds);
        wcs.getAxis().bounds = new CoordBounds1D();
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));
        log.debug("test bounds: " + axis.bounds);
        return wcs;
    }

    SpectralWCS getTestFunction(boolean complete, double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis("WAVE", "nm"));
        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        if (complete) {
            wcs.bandpassName = BANDPASS_NAME;
            wcs.restwav = 6563.0e-10; // meters
            wcs.resolvingPower = 33000.0;
            wcs.transition = TRANSITION;
        }

        RefCoord c1 = new RefCoord(px, sx);
        wcs.getAxis().function = new CoordFunction1D((long) nx, ds, c1);
        log.debug("test function: " + axis.function);
        return wcs;
    }

    SpectralWCS getTestFreqBounds(boolean complete, double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis("FREQ", "MHz"));
        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        if (complete) {
            wcs.bandpassName = BANDPASS_NAME;
            wcs.restfrq = econv.toHz(6536.0e-10, "m");
            wcs.resolvingPower = 33000.0;
            wcs.transition = TRANSITION;
        }

        // divide into 2 samples with a gap between
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx * 0.33, sx + nx * 0.33 * ds);
        RefCoord c3 = new RefCoord(px + nx * 0.66, sx + nx * 0.66 * ds);
        RefCoord c4 = new RefCoord(px + nx, sx + nx * ds);
        wcs.getAxis().bounds = new CoordBounds1D();
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));
        log.debug("test bounds: " + axis.bounds);
        return wcs;
    }

    SpectralWCS getTestFreqFunction(boolean complete, double px, double sx, double nx, double ds) {
        CoordAxis1D axis = new CoordAxis1D(new Axis("FREQ", "MHz"));
        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        if (complete) {
            wcs.bandpassName = BANDPASS_NAME;
            wcs.restwav = 6563.0e-10; // meters
            wcs.resolvingPower = 33000.0;
            wcs.transition = TRANSITION;
        }

        RefCoord c1 = new RefCoord(px, sx);
        wcs.getAxis().function = new CoordFunction1D((long) nx, ds, c1);
        log.debug("test function: " + axis.function);
        return wcs;
    }


}
