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

package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class PolarizationUtilTest 
{
    private static final Logger log = Logger.getLogger(PolarizationUtilTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
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

    private Plane getTestPlane(ProductType ptype)
        throws URISyntaxException
    {
        Plane plane = new Plane("foo");
        Artifact na = new Artifact(new URI("foo", "bar", null), ptype, ReleaseType.DATA);
        plane.getArtifacts().add(na);
        Part np = new Part("baz");
        na.getParts().add(np);
        np.getChunks().add(new Chunk());
        return plane;
    }

    @Test
    public void testEmptyList()
    {
        try
        {
            Plane plane = new Plane("foo");
            Polarization pol = PolarizationUtil.compute(plane.getArtifacts());
            Assert.assertNotNull(pol);
            Assert.assertNull(pol.states);
            Assert.assertNull(pol.dimension);

            plane = getTestPlane(ProductType.SCIENCE);
            pol = PolarizationUtil.compute(plane.getArtifacts());
            Assert.assertNotNull(pol);
            Assert.assertNull(pol.states);
            Assert.assertNull(pol.dimension);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIllegalValues()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            double lowErr = -9.0;
            double highErr = 11.0;
            double zeroErr = 0.0;
            RefCoord c1, c2;
            
            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            c.polarization = w;
            
            c1 = new RefCoord(0.5, zeroErr);
            c2 = new RefCoord(1.5, zeroErr);
            w.getAxis().range = new CoordRange1D(c1, c2);
            
            try
            {
                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
                Assert.fail("zeroErr -- expected IllegalArgumentException, got: " + actual);
            }
            catch(IllegalArgumentException expected)
            {
                log.info("zeroErr -- caught expected: " + expected);
            }
            
            c1 = new RefCoord(0.5, lowErr);
            c2 = new RefCoord(1.5, lowErr);
            w.getAxis().range = new CoordRange1D(c1, c2);
            
            try
            {
                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
                Assert.fail("lowErr -- expected IllegalArgumentException, got: " + actual);
            }
            catch(IllegalArgumentException expected)
            {
                log.info("lowErr -- caught expected: " + expected);
            }
            
            c1 = new RefCoord(0.5, highErr);
            c2 = new RefCoord(1.5, highErr);
            w.getAxis().range = new CoordRange1D(c1, c2);
            
            try
            {
                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
                Assert.fail("highErr -- expected IllegalArgumentException, got: " + actual);
            }
            catch(IllegalArgumentException expected)
            {
                log.info("lowErr -- caught expected: " + expected);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSingleValueRange()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            for (PolarizationState pol : PolarizationState.values())
            {
                CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
                PolarizationWCS w = new PolarizationWCS(axis);
                RefCoord c1 = new RefCoord(0.5, pol.getValue());
                RefCoord c2 = new RefCoord(1.5, pol.getValue());
                w.getAxis().range = new CoordRange1D(c1, c2);

                c.polarization = w;
                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
                log.debug("testSingleValueRange: " + actual);

                Assert.assertNotNull(actual);
                Assert.assertNotNull(actual.states);
                Assert.assertEquals(1, actual.states.size());
                Assert.assertEquals(pol, actual.states.get(0));
                Assert.assertNotNull(actual.dimension);
                Assert.assertEquals(1, actual.dimension.intValue());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    
     
    @Test
    public void testRangeIQU()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(0.5, PolarizationState.I.getValue());
            RefCoord c2 = new RefCoord(3.5, PolarizationState.U.getValue());
            w.getAxis().range = new CoordRange1D(c1, c2);
            
            c.polarization = w;
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testRangeIQU: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(3, actual.states.size());
            Assert.assertEquals(PolarizationState.I, actual.states.get(0));
            Assert.assertEquals(PolarizationState.Q, actual.states.get(1));
            Assert.assertEquals(PolarizationState.U, actual.states.get(2));
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(3, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFunctionIQUV()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(0.5, PolarizationState.I.getValue());
            w.getAxis().function = new CoordFunction1D(new Long(4L), new Double(1.0), c1);

            c.polarization = w;
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testFunctionIQUV: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(4, actual.states.size());
            Assert.assertEquals(PolarizationState.I, actual.states.get(0));
            Assert.assertEquals(PolarizationState.Q, actual.states.get(1));
            Assert.assertEquals(PolarizationState.U, actual.states.get(2));
            Assert.assertEquals(PolarizationState.V, actual.states.get(3));

            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(4, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFunctionRR_LL()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(0.5, PolarizationState.RR.getValue());
            w.getAxis().function = new CoordFunction1D(new Long(2L), new Double(-1.0), c1);

            c.polarization = w;
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testFunctionRR_LL: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(2, actual.states.size());
            Assert.assertEquals(PolarizationState.RR, actual.states.get(0));
            Assert.assertEquals(PolarizationState.LL, actual.states.get(1));

            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(2, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFunctionVUQI()
    {
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(1.0, PolarizationState.V.getValue());
            w.getAxis().function = new CoordFunction1D(new Long(4L), new Double(-1.0), c1);

            c.polarization = w;
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testFunctionRR_LL: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(4, actual.states.size());
            Assert.assertEquals(PolarizationState.I, actual.states.get(0));
            Assert.assertEquals(PolarizationState.Q, actual.states.get(1));
            Assert.assertEquals(PolarizationState.U, actual.states.get(2));
            Assert.assertEquals(PolarizationState.V, actual.states.get(3));
            
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(4, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testRangeFromCalibrationIQ()
    {
        log.debug("testRangeFromCalibrationIQ - START");
        try
        {
            Plane plane = getTestPlane(ProductType.CALIBRATION);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(0.5, PolarizationState.I.getValue());
            RefCoord c2 = new RefCoord(2.5, PolarizationState.Q.getValue());
            w.getAxis().range = new CoordRange1D(c1, c2);
            c.polarization = w;
            
            // add some aux artifacts, should not effect result
            Artifact at = new Artifact(new URI("ad:foo/bar/aux"), ProductType.AUXILIARY, ReleaseType.DATA);
            plane.getArtifacts().add(at);
            Part pt = new Part("otherPart");
            at.getParts().add(pt);
            
            Chunk ch = new Chunk();
            pt.getChunks().add(ch);
            CoordAxis1D axist = new CoordAxis1D(new Axis("STOKES", null));
            ch.polarization = new PolarizationWCS(axist);
            RefCoord c1t = new RefCoord(0.5, PolarizationState.U.getValue());
            RefCoord c2t = new RefCoord(2.5, PolarizationState.V.getValue());
            ch.polarization.getAxis().range = new CoordRange1D(c1t, c2t);
            
            
            
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testRangeIQU: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(2, actual.states.size());
            Assert.assertEquals(PolarizationState.I, actual.states.get(0));
            Assert.assertEquals(PolarizationState.Q, actual.states.get(1));
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(2, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
     @Test
    public void testRangeFromMixedIQ()
    {
        log.debug("testRangeFromCalibrationIQ - START");
        try
        {
            Plane plane = getTestPlane(ProductType.SCIENCE);
            // ouch :-)
            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();

            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
            PolarizationWCS w = new PolarizationWCS(axis);
            RefCoord c1 = new RefCoord(0.5, PolarizationState.I.getValue());
            RefCoord c2 = new RefCoord(2.5, PolarizationState.Q.getValue());
            w.getAxis().range = new CoordRange1D(c1, c2);
            c.polarization = w;
            
            // add some cal artifacts, should not effect result
            Artifact at = new Artifact(new URI("ad:foo/bar/aux"), ProductType.CALIBRATION, ReleaseType.DATA);
            plane.getArtifacts().add(at);
            Part pt = new Part("otherPart");
            at.getParts().add(pt);
            Chunk ch = new Chunk();
            pt.getChunks().add(ch);
            CoordAxis1D axist = new CoordAxis1D(new Axis("STOKES", null));
            ch.polarization = new PolarizationWCS(axist);
            RefCoord c1t = new RefCoord(0.5, PolarizationState.U.getValue());
            RefCoord c2t = new RefCoord(2.5, PolarizationState.V.getValue());
            ch.polarization.getAxis().range = new CoordRange1D(c1t, c2t);
            
            
            
            Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
            log.debug("testRangeIQU: " + actual);

            Assert.assertNotNull(actual);
            Assert.assertNotNull(actual.states);
            Assert.assertEquals(2, actual.states.size());
            Assert.assertEquals(PolarizationState.I, actual.states.get(0));
            Assert.assertEquals(PolarizationState.Q, actual.states.get(1));
            Assert.assertNotNull(actual.dimension);
            Assert.assertEquals(2, actual.dimension.intValue());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
