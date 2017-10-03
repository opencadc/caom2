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
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class TimeUtilTest 
{
    private static final Logger log = Logger.getLogger(TimeUtilTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.types", Level.INFO);
    }


    ////@Test
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
    public void testEmptyList()
    {
        try
        {
            Plane plane = new Plane("foo");
            Time tim = TimeUtil.compute(plane.getArtifacts());
            Assert.assertNotNull(tim);
            Assert.assertNull(tim.bounds);
            Assert.assertNull(tim.dimension);
            Assert.assertNull(tim.exposure);
            Assert.assertNull(tim.resolution);
            Assert.assertNull(tim.sampleSize);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromRange()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetRange(1, 1, 1);
            actual = TimeUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromRange: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.0001);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromBounds()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L * 2 / 3;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetBounds(1, 1, 1);
            actual = TimeUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromBounds: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.0001);
            Assert.assertEquals(2, actual.bounds.getSamples().size());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue(), 1L);
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testComputeFromFunction()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetFunction(1, 1, 1, false);
            actual = TimeUtil.compute(plane.getArtifacts());
            log.debug("testComputeFromFunction: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.0001);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.0001);

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testComputeFromMultipleFunction()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 2*200*0.01;
            long expectedDimension = 2*200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetFunction(1, 2, 1, true);
            actual = TimeUtil.compute(plane.getArtifacts());
            log.info("testComputeFromFunction: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.0001);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.0001);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.0001);

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testComputeFromMultipleFunctionOverlap()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetFunction(1, 2, 1, false);
            actual = TimeUtil.compute(plane.getArtifacts());
            log.info("testComputeFromMultipleFunctionOverlap: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.01);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.0001);

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testComputeFromScience()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetRange(1, 1, 1, ProductType.SCIENCE);
            
            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), ProductType.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);
            
            actual = TimeUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromScience: " + actual);

            
            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.01);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testComputeFromCalibration()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetRange(1, 1, 1, ProductType.CALIBRATION);
            
            // add some aux artifacts, should not effect result
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), ProductType.AUXILIARY, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);
            
            actual = TimeUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromScience: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.01);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testComputeFromMixed()
    {
        try
        {
            double expectedLB = 54321.0;
            double expectedUB = expectedLB + 200*0.01;
            long expectedDimension = 200L;
            double expectedExposure = 300.0;
            double expectedResolution = 0.1;
            double expectedSS = 0.01;

            Plane plane;
            Time actual;

            plane = getTestSetRange(1, 1, 1, ProductType.SCIENCE);
            
            // add some CAL artifacts, should not effect result since SCIENCE above
            Plane tmp = getTestSetRange(1, 1, 3);
            Artifact tmpA = tmp.getArtifacts().iterator().next();
            Artifact aux = new Artifact(new URI("ad:foo/bar/aux"), ProductType.CALIBRATION, ReleaseType.DATA);
            aux.getParts().addAll(tmpA.getParts());
            plane.getArtifacts().add(aux);
            
            actual = TimeUtil.compute(plane.getArtifacts());

            log.debug("testComputeFromScience: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.bounds);
            Assert.assertEquals(expectedLB, actual.bounds.getLower(), 0.01);
            Assert.assertEquals(expectedUB, actual.bounds.getUpper(), 0.01);
            Assert.assertFalse(actual.bounds.getSamples().isEmpty());
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(expectedDimension, actual.dimension.longValue());
            Assert.assertNotNull(actual.exposure);
            Assert.assertEquals(expectedExposure, actual.exposure.doubleValue(), 0.01);
            Assert.assertNotNull(actual.resolution);
            Assert.assertEquals(expectedResolution, actual.resolution.doubleValue(), 0.0001);
            Assert.assertNotNull(actual.sampleSize);
            Assert.assertEquals(expectedSS, actual.sampleSize, 0.01);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    Plane getTestSetRange(int numA, int numP, int numC)
        throws URISyntaxException
    {
        return getTestSetRange(numA, numP, numC, ProductType.SCIENCE);
    }

    Plane getTestSetRange(int numA, int numP, int numC, ProductType ptype)
        throws URISyntaxException
    {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane("foo");
        int n = 0;
        for (int a=0; a<numA; a++)
        {
            Artifact na = new Artifact(new URI("foo", "bar"+a, null), ptype, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p=0; p<numP; p++)
            {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c=0; c<numC; c++)
                {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // just shift to higher values of x for each subsequent chunk
                    nc.time = getTestRange(true, px, sx+n*nx*ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetRange: " + n + " chunks");
        return plane;
    }

    Plane getTestSetBounds(int numA, int numP, int numC)
        throws URISyntaxException
    {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane("foo");
        int n = 0;
        for (int a=0; a<numA; a++)
        {
            Artifact na = new Artifact(new URI("foo", "bar"+a, null), ProductType.SCIENCE, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p=0; p<numP; p++)
            {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c=0; c<numC; c++)
                {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    // just shift to higher values of x for each subsequent chunk
                    nc.time = getTestBounds(true, px, sx+n*nx*ds, nx, ds);
                    n++;
                }
            }
        }
        log.debug("getTestSetBounds: " + n + " chunks");
        return plane;
    }

    Plane getTestSetFunction(int numA, int numP, int numC, boolean shift)
        throws URISyntaxException
    {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;
        Plane plane = new Plane("foo");
        int n = 0;
        for (int a=0; a<numA; a++)
        {
            Artifact na = new Artifact(new URI("foo", "bar"+a, null), ProductType.SCIENCE, ReleaseType.DATA);
            plane.getArtifacts().add(na);
            for (int p=0; p<numP; p++)
            {
                Part np = new Part(new Integer(p));
                na.getParts().add(np);
                for (int c=0; c<numC; c++)
                {
                    Chunk nc = new Chunk();
                    np.getChunks().add(nc);
                    nc.time = getTestFunction(true, px, sx+n*nx*ds, nx, ds);
                    if (shift)
                        n++;
                }
            }
        }
        log.debug("getTestSetFunction: " + n + " chunks");
        return plane;
    }

    TemporalWCS getTestRange(boolean complete, double px, double sx, double nx, double ds)
    {
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);
        if (complete)
        {
            wcs.exposure = 300.0;
            wcs.resolution = 0.1;
        }
        
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx, sx + nx*ds);
        wcs.getAxis().range = new CoordRange1D(c1, c2);

        return wcs;
    }
    TemporalWCS getTestBounds(boolean complete, double px, double sx, double nx, double ds)
    {
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);
        if (complete)
        {
            wcs.exposure = 300.0;
            wcs.resolution = 0.1;
        }

        // divide into 2 samples with a gap between
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(px + nx*0.33, sx + nx*ds*0.33);
        RefCoord c3 = new RefCoord(px + nx*0.66, sx + nx*ds*0.66);
        RefCoord c4 = new RefCoord(px + nx,      sx + nx*ds);
        wcs.getAxis().bounds = new CoordBounds1D();
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));

        return wcs;
    }

    TemporalWCS getTestFunction(boolean complete, double px, double sx, double nx, double ds)
    {
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);
        if (complete)
        {
            wcs.exposure = 300.0;
            wcs.resolution = 0.1;
        }

        RefCoord c1 = new RefCoord(px, sx);
        wcs.getAxis().function = new CoordFunction1D((long) nx, ds, c1);
        
        return wcs;
    }

}
