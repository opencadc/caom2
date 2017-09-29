/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.*;
import ca.nrc.cadc.caom2.wcs.*;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author pdowler
 */
public class CaomWCSValidatorTest
{
    private static final Logger log = Logger.getLogger(CaomWCSValidatorTest.class);

    private static final String UNEXPECTED_EXCEPTION = "Unexpected exception ";
    private EnergyUtilTest euTest = new EnergyUtilTest();

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.compute", Level.INFO);
    }

    public CaomWCSValidatorTest() { }
    
//    @Test
    public void testWCSValidator()
    {
        // SpatialWCS
        try
        {
//            // Test set with bounds
//            // could loop through integer values in getTestSetBounds here if need be.
//            Plane plane = PositionUtilTest.getTestSetBounds(1,1);
//            validatePlane(plane);
//
//            plane = PositionUtilTest.getTestSetRange(1,2);
//            validatePlane(plane);
//
            Plane plane = PositionUtilTest.getTestSetFunction(2,2);
            validatePlane(plane);

        }
        catch(Exception unexpected)
        {
            log.error(UNEXPECTED_EXCEPTION + " getting test set range", unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " getting test set range: " + unexpected);
        }
    }

    @Test
    public void testSpatialWCSValidator()
    {
        double px = 0.5;
        double py = 0.5;
        double sx = 20.0;
        double sy = 10.0;
        double dp = 1000.0;
        double ds = 1.0;

        try
        {
            SpatialWCS position = PositionUtilTest.getTestFunction(px,py,sx,sy,false);

            try
            {
                CaomWCSValidator.validateSpatialWCS(position);
            }
            catch (Exception unexpected)
            {
                log.error(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString(), unexpected);
                Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString() + unexpected);
            }

        }
        catch(Exception unexpected)
        {
            log.error(UNEXPECTED_EXCEPTION + " validating SpatialWCS: ", unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + unexpected);
        }
        log.info("done testSpatialWCSValidator");
    }


    @Test
    public void testSpectralWCSValidator()
    {
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;

        try
        {
            // Test set with bounds
            SpectralWCS energy = euTest.getTestRange(true,  px, sx*nx*ds, nx, ds);

            try
            {
                CaomWCSValidator.validateSpectralWCS(energy);
            }
            catch (Exception unexpected)
            {
                log.error(UNEXPECTED_EXCEPTION + " validating SpectralWCS: " + energy.toString(), unexpected);
                Assert.fail(UNEXPECTED_EXCEPTION + " validating SpectralWCS: " + energy.toString() + unexpected);
            }

        }
        catch(Exception unexpected)
        {
            log.error(UNEXPECTED_EXCEPTION + " validating SpatialWCS: ", unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + unexpected);
        }
        log.info("done testSpectralWCSValidator");
    }


    private void validatePlane(Plane p)
    {
        for (Artifact a: p.getArtifacts())
        {
            try
            {
                CaomWCSValidator.validate(a);
            }
            catch (Exception unexpected)
            {
                log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
                Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
            }
        }
    }

//    private void validatePlane(Plane p)
//    {
//        for (Artifact a: p.getArtifacts())
//        {
//            try
//            {
//                CaomWCSValidator.validate(a);
//            }
//            catch (Exception unexpected)
//            {
//                log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
//                Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
//            }
//        }
//    }

    // Need:
    // - valid SpatialWCS
    // - valid SpectralWCS
    // - valid TemporalWCS
    // - valid PolarizationWCS
    // - invalid examples of all

    // TODO: this test needs to work all code paths in the validator,
    // which will be reflected in the number of different types of validation done.

    // CONSIDER: should the validator use the test object generation functions from the other tests,
    // or have it's own?


//    @Test
//    public void testIllegalValues()
//    {
//        // Test failure of each type of WCS
//        try
//        {
//            Plane plane = getTestPlane(ProductType.SCIENCE);
//            // ouch :-)
//            Chunk c = plane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
//
//            double lowErr = -9.0;
//            double highErr = 11.0;
//            double zeroErr = 0.0;
//            RefCoord c1, c2;
//
//            CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
//            PolarizationWCS w = new PolarizationWCS(axis);
//            c.polarization = w;
//
//            c1 = new RefCoord(0.5, zeroErr);
//            c2 = new RefCoord(1.5, zeroErr);
//            w.getAxis().range = new CoordRange1D(c1, c2);
//
//            try
//            {
//                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
//                Assert.fail("zeroErr -- expected IllegalArgumentException, got: " + actual);
//            }
//            catch(IllegalArgumentException expected)
//            {
//                log.info("zeroErr -- caught expected: " + expected);
//            }
//
//            c1 = new RefCoord(0.5, lowErr);
//            c2 = new RefCoord(1.5, lowErr);
//            w.getAxis().range = new CoordRange1D(c1, c2);
//
//            try
//            {
//                Polarization actual = PolarizationUtil.compute(plane.getArtifacts());
//                Assert.fail("lowErr -- expected IllegalArgumentException, got: " + actual);
//            }
//            catch(IllegalArgumentException expected)
//            {
//                log.info("lowErr -- caught expected: " + expected);
//            }
//
//            c1 = new RefCoord(0.5, highErr);
//            c2 = new RefCoord(1.5, highErr);
//            w.getAxis().range = new CoordRange1D(c1, c2);
//
//            try
//            {
//                CaomWCSValidator.validate()
//            }
//            catch(IllegalArgumentException expected)
//            {
//                log.info("lowErr -- caught expected: " + expected);grad
//            }
//        }
//        catch(Exception unexpected)
//        {
//            log.error("unexpected exception", unexpected);
//            Assert.fail("unexpected exception: " + unexpected);
//        }
//    }
//
//    private Plane getTestPlane(ProductType ptype)
//            throws URISyntaxException
//    {
//        Plane plane = new Plane("foo");
//        Artifact na = new Artifact(new URI("foo", "bar", null), ptype, ReleaseType.DATA);
//        plane.getArtifacts().add(na);
//        Part np = new Part("baz");
//        na.getParts().add(np);
//        np.getChunks().add(new Chunk());
//        return plane;
//    }
//
//
//    private SpatialWCS mkGoodSpatialWCS()
//    {
//        Axis axis1 = new Axis("RA---TAN", "deg");
//        Axis axis2 = new Axis("DEC--TAN", "deg");
//        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
//        SpatialWCS wcs = new SpatialWCS(axis);
//        wcs.equinox = null;
//        Dimension2D dim = new Dimension2D(1024, 1024);
//        Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
//        axis.function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 0.0); // singular CD matrix
//    }
//    private SpatialWCS mkBadSpatialWCS()
//    {
//
//    }
//
//
//    private SpectralWCS mkGoodSpectralWCS()
//    {
//
//    }
//    private SpectralWCS mkBadSpectralWCSRange()
//    {
//
//    }
//    private SpectralWCS mkBadSpectralWCSBounds()
//    {
//
//    }
//    private SpectralWCS mkBadSpectralWCSFn()
//    {
//
//    }
//
//    private TemporalWCS mkGoodTemporalWCS()
//    {
//
//    }
//    private TemporalWCS mkBadTemporalWCSRange()
//    {
//
//    }
//    private TemporalWCS mkBadTemporalWCSBounds()
//    {
//
//    }
//    private TemporalWCS mkBadTemporalWCSFn()
//    {
//
//    }
//
//
//    private PolarizationWCS mkGoodPolarizationWCS()
//    {
//
//    }
//    private PolarizationWCS mkBadPolarizationWCSRange()
//    {
//
//    }
//    private PolarizationWCS mkBadPolarizationWCSBounds()
//    {
//
//    }
//    private PolarizationWCS mkBadPolarizationWCSFn()
//    {
//
//    }



}
