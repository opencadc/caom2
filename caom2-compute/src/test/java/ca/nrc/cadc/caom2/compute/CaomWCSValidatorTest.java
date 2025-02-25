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

import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.Coord2D;
import org.opencadc.caom2.wcs.CoordAxis2D;
import org.opencadc.caom2.wcs.CoordFunction2D;
import org.opencadc.caom2.wcs.CustomWCS;
import org.opencadc.caom2.wcs.Dimension2D;
import org.opencadc.caom2.wcs.PolarizationWCS;
import org.opencadc.caom2.wcs.RefCoord;
import org.opencadc.caom2.wcs.SpatialWCS;
import org.opencadc.caom2.wcs.SpectralWCS;
import org.opencadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author jeevesh
 */
public class CaomWCSValidatorTest {
    private static final Logger log = Logger.getLogger(CaomWCSValidatorTest.class);

    private static final String UNEXPECTED_EXCEPTION = "Unexpected exception ";
    private static final String EXPECTED_EXCEPTION = "Exception thrown. ";
    private static final String VERIFIED_INVALID = " verified as invalid.";

    private ComputeDataGenerator dataGenerator = new ComputeDataGenerator();


    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.compute", Level.INFO);
    }

    @Test
    public void testNullWCS() {
        Artifact a = null;

        try {
            a = dataGenerator.getTestArtifact(DataLinkSemantics.THIS);
            Chunk c = a.getParts().iterator().next().getChunks().iterator().next();

            CaomWCSValidator.validate(a);
            // Should pass

            c.custom = null;
            c.customAxis = null;
            c.naxis = 4;
            CaomWCSValidator.validate(a);

            c.time = null;
            c.timeAxis = null;
            c.naxis = 3;
            CaomWCSValidator.validate(a);

            // Jump 2 because position takes 2 axes
            c.position = null;
            c.positionAxis1 = null;
            c.positionAxis2 = null;
            c.naxis = 1;
            CaomWCSValidator.validate(a);

            c.time = null;
            c.timeAxis = null;
            c.naxis = null;
            CaomWCSValidator.validate(a);

        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
        }
    }

    @Test
    public void testValidWCS() {
        Artifact a = null;
        try {
            a = dataGenerator.getTestArtifact(DataLinkSemantics.THIS);
            Chunk c = a.getParts().iterator().next().getChunks().iterator().next();

            // Populate all WCS with good values
            c.position = dataGenerator.mkGoodSpatialWCS();
            c.energy = dataGenerator.mkGoodSpectralWCS();
            c.time = dataGenerator.mkGoodTemporalWCS();
            c.polarization = dataGenerator.mkGoodPolarizationWCS();
            c.custom = dataGenerator.mkGoodCustomWCS();

            CaomWCSValidator.validate(a);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
        }

    }

    @Test
    public void testNullAxis() {
        Artifact a = null;
        try {
            a = dataGenerator.getTestArtifact(DataLinkSemantics.THIS);
            Chunk c = a.getParts().iterator().next().getChunks().iterator().next();

            c.naxis = null;
            CaomWCSValidator.validate(a);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
        }
    }


    @Test
    public void testInvalidNullMetadata() {
        Artifact a = null;
        try {
            a = dataGenerator.getTestArtifact(DataLinkSemantics.THIS);
            Chunk c = a.getParts().iterator().next().getChunks().iterator().next();
            c.timeAxis = null;
            CaomWCSValidator.validate(a);
        } catch (IllegalArgumentException expected) {
            log.info("expected exception: " + expected.toString());
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating artifact: " + a.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + a.toString() + unexpected);
        }
    }
    @Test
    public void testSpatialWCSValidator() {
        SpatialWCS position = null;

        try {
            position = dataGenerator.mkGoodSpatialWCS();
            CaomWCSValidator.validateSpatialWCS("test", position);

            // Null value is acceptable
            CaomWCSValidator.validateSpatialWCS("test", null);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString() + unexpected);
        }

        log.info("done testSpatialWCSValidator");
    }

    @Test
    public void testAxisValidator () {
        try {
            // Basic axis definition
            Chunk c = dataGenerator.getFreshChunk();
            CaomWCSValidator.validateAxes(c);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating axes: ", unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + unexpected);
        }

        log.info("done testAxisValidator");
    }

    @Test
    public void testInvalidAxes() {
        try {
            // Test data axes are set up to pass validation
            Chunk c = dataGenerator.getFreshChunk();

            // 1) pos1 & pos2 axis must both be defined
            // position can't be null
            c.positionAxis2 = null;
            try {
                CaomWCSValidator.validateAxes(c);
                Assert.fail("Position axis 2 not defined. Error expected but not thrown.");
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: positionAxis1 or positionAxis2 is null.", iae.getMessage());
                log.info("position axis 2 error expected: " + iae.getMessage());
            }

            c.positionAxis2 = 3;
            c.position = null;
            try {
                CaomWCSValidator.validateAxes(c);
                Assert.fail("Position not defined. Error expected but not thrown.");
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: positionAxis1 found (2) but metadata not found", iae.getMessage());
                log.info("position null error expected: " + iae.getMessage());
            }

            // 2) Duplicate axis definition
            c = dataGenerator.getFreshChunk();
            c.observableAxis = c.positionAxis1;
            try {
                CaomWCSValidator.validateAxes(c);
            } catch (IllegalArgumentException iae) {
                log.info("Duplicate axis expected: " + iae.getMessage());
                Assert.assertEquals( "Duplicate axis found: (2) positionAxis1 & observableAxis", iae.getMessage());
            }

            // 3) Axis list not contiguous - axis definition is missing
            c = dataGenerator.getFreshChunk();
            c.observableAxis = null;
            try {
                CaomWCSValidator.validateAxes(c);
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: missing axis 1", iae.getMessage());
                log.info("missing axis error expected: " + iae.getMessage());
            }

            // 4) Axis list too short
            c = dataGenerator.getFreshChunk();
            c.timeAxis = null;
            try {
                CaomWCSValidator.validateAxes(c);
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: missing axis 4", iae.getMessage());
                log.info("Not enough axes defined expected: " + iae.getMessage());
            }

            // 5) Invalid definition (axis = 0)
            c = dataGenerator.getFreshChunk();
            c.positionAxis2 = 0;
            try {
                CaomWCSValidator.validateAxes(c);
                Assert.fail("axis definition (0) not allowed: positionAxis2.");
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: axis definition (0) not allowed: positionAxis2", iae.getMessage());
                log.info("Invalid axis (0) expected: " + iae.getMessage());
            }

            // 1) pos1 & pos2 axis must both b defined
            // position can't be null
            c = dataGenerator.getFreshChunk();
            c.positionAxis2 = 0;
            c.naxis=null;
            try {
                CaomWCSValidator.validateAxes(c);
                Assert.fail("axis definition (0) not allowed: positionAxis2.");
            } catch (IllegalArgumentException iae) {
                Assert.assertEquals( "Invalid Axes: axis definition (0) not allowed: positionAxis2", iae.getMessage());
                log.info("Invalid axis (0) expected: " + iae.getMessage());
            }


        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION, unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + unexpected);
        }

        log.info("done testInvalidAxes");
    }

    @Test
    public void testInvalidSpatialWCS() {
        SpatialWCS position = null;

        try {
            position = dataGenerator.mkBadSpatialWCS();
            CaomWCSValidator.validateSpatialWCS("test", position);
        } catch (IllegalArgumentException | WCSLibRuntimeException expected) {
            log.info(EXPECTED_EXCEPTION + "SpatialWCS" + VERIFIED_INVALID + position.toString());
            Assert.assertTrue(EXPECTED_EXCEPTION + "SpatialWCS" + VERIFIED_INVALID + position.toString() + expected, true);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpatialWCS: " + position.toString() + unexpected);
        }

        log.info("done testSpatialWCSValidator");
    }


    @Test
    public void testSpectralWCSValidator() {
        SpectralWCS energy = null;

        try {
            energy = dataGenerator.mkGoodSpectralWCS();
            CaomWCSValidator.validateSpectralWCS("test", energy);

            // Null value is acceptable
            CaomWCSValidator.validateSpectralWCS("test", null);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating SpectralWCS: " + energy.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating SpectralWCS: " + energy.toString() + unexpected);
        }

        log.info("done testSpectralWCSValidator");
    }


    @Test
    public void testIvalidSpectralWCS() {
        SpectralWCS energy = null;

        try {
            energy = dataGenerator.mkBadSpectralWCSFn();
            CaomWCSValidator.validateSpectralWCS("test", energy);

            Assert.fail("expected WCSlibRuntimeException");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testTemporalWCSValidator() {
        TemporalWCS time = null;

        try {
            time = dataGenerator.mkGoodTemporalWCS();
            CaomWCSValidator.validateTemporalWCS("test", time);

            // Null value is acceptable
            CaomWCSValidator.validateTemporalWCS("test", null);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating TemporalWCS: " + time.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating TemporalWCS: " + time.toString() + unexpected);
        }

        log.info("done testTemporalWCSValidator");
    }


    @Test
    public void testInvalidTemporalWCS() {
        TemporalWCS time = null;

        try {
            try {
                time = dataGenerator.mkBadTemporalWCSCunit();
                CaomWCSValidator.validateTemporalWCS("test", time);
                Assert.fail("Expected an exception, but none was thrown: " + time.toString());
            } catch (IllegalArgumentException iae) {
                log.info(EXPECTED_EXCEPTION + " bad cunit.");
            }

            try {
                time = dataGenerator.mkBadTemporalWCSRange();
                CaomWCSValidator.validateTemporalWCS("test", time);
                Assert.fail("Expected an exception, but none was thrown: " + time.toString());
            } catch (IllegalArgumentException iae) {
                log.info(EXPECTED_EXCEPTION + " bad range.");
            }
            
            try {
                time = dataGenerator.mkBadTemporalWCSFunction();
                CaomWCSValidator.validateTemporalWCS("test", time);
                Assert.fail("Expected an exception, but none was thrown: " + time.toString());
            } catch (IllegalArgumentException iae) {
                log.info(EXPECTED_EXCEPTION + " bad function.");
            }
        } catch (Exception unexpected) {
            log.info(UNEXPECTED_EXCEPTION + " validating TemporalWCS: " + time.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating TemporalWCS: " + time.toString() + unexpected);
        }

        log.info("done testTemporalWCSValidator");
    }


    @Test
    public void testPolarizationWCSValidator() {
        PolarizationWCS polarization = null;

        try {
            polarization = dataGenerator.mkGoodPolarizationWCS();
            CaomWCSValidator.validatePolarizationWCS("test", polarization);

            // Null value is acceptable
            CaomWCSValidator.validatePolarizationWCS("test", null);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating PolarizationWCS: " + polarization.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating PolarizationWCS: " + polarization.toString() + unexpected);
        }

        log.info("done testPolarizationWCSValidator");
    }


    @Test
    public void testInvalidPolarizationlWCS()
        throws URISyntaxException {
        PolarizationWCS w = dataGenerator.mkBadPolarizationWCS();

        try {
            CaomWCSValidator.validatePolarizationWCS("test", w);
            Assert.fail("zeroErr -- expected IllegalArgumentException. Validator passed when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("zeroErr -- caught expected: " + expected);
        }
    }


    @Test
    public void testCustomWCSValidator() {
        CustomWCS custom = null;

        try {
            custom = dataGenerator.mkGoodCustomWCS();
            CaomWCSValidator.validateCustomWCS("test", custom);

            // Null value is acceptable
            CaomWCSValidator.validateCustomWCS("test", null);
        } catch (Exception unexpected) {
            log.error(UNEXPECTED_EXCEPTION + " validating CustomWCS: " + custom.toString(), unexpected);
            Assert.fail(UNEXPECTED_EXCEPTION + " validating CustomWCS: " + custom.toString() + unexpected);
        }

        log.info("done testCustomWCSValidator");
    }


    @Test
    public void testInvalidCustomWCS() {
        try {
            CustomWCS c = dataGenerator.mkBadCtypeCustomWCS();
            CaomWCSValidator.validateCustomWCS("test", c);
            Assert.fail("expected IllegalArgumentException. Validator passed ctype when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }

        try {
            CustomWCS c2 = dataGenerator.mkBadCunitCustomWCS();
            CaomWCSValidator.validateCustomWCS("test", c2);
            Assert.fail("expected IllegalArgumentException. Validator passed cunit when it should not have.");
        } catch (IllegalArgumentException expected) {
            log.info("caught expected: " + expected);
        }

        log.info("done testInvalidCustomWCS");
    }



    //@Test
    public void testHPX2()
    {
        try
        {
            // values from a JCMT scuba2 healpix product
            Axis axis1 = new Axis("RA---HPX", "deg");
            Axis axis2 = new Axis("DEC--HPX", "deg");
            CoordAxis2D a2d = new CoordAxis2D(axis1, axis2);
            SpatialWCS wcs = new SpatialWCS(a2d);
            wcs.coordsys = "ICRS";
            wcs.getAxis().function = new CoordFunction2D(
                    new Dimension2D(66120, 1), new Coord2D(new RefCoord(-46659.5, 0.0), new RefCoord(2820.5, 0.0)),
                    -6.86645537834E-4, -6.86645537833E-4, -6.86645537834E-4, 6.86645537833E-4);
            
            CaomWCSValidator.validateSpatialWCS("testHPX2", wcs);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
