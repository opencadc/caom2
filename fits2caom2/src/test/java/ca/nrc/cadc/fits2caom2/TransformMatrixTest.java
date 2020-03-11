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
 *  : 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.FitsValuesMap;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TransformMatrixTest {
    private static Logger log = Logger.getLogger(TransformMatrixTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    private static final String COLLECTION = "test_collection";
    private static final String OBSERVATION_ID = "test_obsID";
    private static final String PRODUCT_ID = "test_productID";

    public TransformMatrixTest() { }

    @Test
    public void testCDMatrix() throws Exception
    {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CDELT1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CDELT2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        defaults.setKeywordValue("CD1_1", "1.000000000000E+00");
        defaults.setKeywordValue("CD1_2", "0.000000000000E+00");
        defaults.setKeywordValue("CD2_1", "0.000000000000E+00");
        defaults.setKeywordValue("CD2_2", "1.000000000000E+00");

        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris, config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);

        SpatialWCS position = chunk.position;
        Assert.assertNotNull(position.getAxis());
        Assert.assertEquals("RA---SIN", position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("DEC--SIN", position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis2().getCunit());
        Assert.assertNull(position.getAxis().bounds);
        Assert.assertNull(position.getAxis().range);
        Assert.assertNotNull(position.getAxis().function);

        CoordFunction2D func = position.getAxis().function;
        Assert.assertEquals(11934, func.getDimension().naxis1);
        Assert.assertEquals(10399, func.getDimension().naxis2);
        Assert.assertEquals(9887.0, func.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(321.7181583333, func.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(2529, func.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(-52.34973888889, func.getRefCoord().getCoord2().val, 0.0);

        Assert.assertEquals(1.0, func.getCd11(), 0.0);
        Assert.assertEquals(0.0, func.getCd12(), 0.0);
        Assert.assertEquals(0.0, func.getCd21(), 0.0);
        Assert.assertEquals(1.0, func.getCd22(), 0.0);
    }

    @Test
    public void testPCMatrixStandardKeywords() throws Exception
    {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CDELT1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CDELT2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        defaults.setKeywordValue("PC1_1", "1.000000000000E+00");
        defaults.setKeywordValue("PC2_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_1", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_2", "1.000000000000E+00");
        defaults.setKeywordValue("PC3_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_2", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_3", "1.000000000000E+00");
        defaults.setKeywordValue("PC4_3", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_4", "1.000000000000E+00");

        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris, config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);

        SpatialWCS position = chunk.position;
        Assert.assertNotNull(position.getAxis());
        Assert.assertEquals("RA---SIN", position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("DEC--SIN", position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis2().getCunit());
        Assert.assertNull(position.getAxis().bounds);
        Assert.assertNull(position.getAxis().range);
        Assert.assertNotNull(position.getAxis().function);

        CoordFunction2D func = position.getAxis().function;
        Assert.assertEquals(11934, func.getDimension().naxis1);
        Assert.assertEquals(10399, func.getDimension().naxis2);
        Assert.assertEquals(9887.0, func.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(321.7181583333, func.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(2529, func.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(-52.34973888889, func.getRefCoord().getCoord2().val, 0.0);

        Assert.assertEquals(-0.0005555555555556, func.getCd11(), 0.0);
        Assert.assertEquals(0.0, func.getCd12(), 0.0);
        Assert.assertEquals(0.0, func.getCd21(), 0.0);
        Assert.assertEquals(0.0005555555555556, func.getCd22(), 0.0);
    }

    @Test
    public void testPCMatrixNonStandardKeywords() throws Exception
    {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CDELT1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CDELT2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        defaults.setKeywordValue("PC01_01", "1.000000000000E+00");
        defaults.setKeywordValue("PC02_01", "0.000000000000E+00");
        defaults.setKeywordValue("PC03_01", "0.000000000000E+00");
        defaults.setKeywordValue("PC04_01", "0.000000000000E+00");

        defaults.setKeywordValue("PC01_02", "0.000000000000E+00");
        defaults.setKeywordValue("PC02_02", "1.000000000000E+00");
        defaults.setKeywordValue("PC03_02", "0.000000000000E+00");
        defaults.setKeywordValue("PC04_02", "0.000000000000E+00");

        defaults.setKeywordValue("PC01_03", "0.000000000000E+00");
        defaults.setKeywordValue("PC02_03", "0.000000000000E+00");
        defaults.setKeywordValue("PC03_03", "1.000000000000E+00");
        defaults.setKeywordValue("PC04_03", "0.000000000000E+00");

        defaults.setKeywordValue("PC01_04", "0.000000000000E+00");
        defaults.setKeywordValue("PC02_04", "0.000000000000E+00");
        defaults.setKeywordValue("PC03_04", "0.000000000000E+00");
        defaults.setKeywordValue("PC04_04", "1.000000000000E+00");

        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris, config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);

        SpatialWCS position = chunk.position;
        Assert.assertNotNull(position.getAxis());
        Assert.assertEquals("RA---SIN", position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("DEC--SIN", position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis2().getCunit());
        Assert.assertNull(position.getAxis().bounds);
        Assert.assertNull(position.getAxis().range);
        Assert.assertNotNull(position.getAxis().function);

        CoordFunction2D func = position.getAxis().function;
        Assert.assertEquals(11934, func.getDimension().naxis1);
        Assert.assertEquals(10399, func.getDimension().naxis2);
        Assert.assertEquals(9887.0, func.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(321.7181583333, func.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(2529, func.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(-52.34973888889, func.getRefCoord().getCoord2().val, 0.0);

        Assert.assertEquals(-0.0005555555555556, func.getCd11(), 0.0);
        Assert.assertEquals(0.0, func.getCd12(), 0.0);
        Assert.assertEquals(0.0, func.getCd21(), 0.0);
        Assert.assertEquals(0.0005555555555556, func.getCd22(), 0.0);
    }

    @Test
    public void testPCMatrixWithNoCDELT() throws Exception {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CROTA1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CROTA2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        defaults.setKeywordValue("PC1_1", "1.000000000000E+00");
        defaults.setKeywordValue("PC2_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_1", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_2", "1.000000000000E+00");
        defaults.setKeywordValue("PC3_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_2", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_3", "1.000000000000E+00");
        defaults.setKeywordValue("PC4_3", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_4", "1.000000000000E+00");

        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris, config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);
    }

    @Test
    public void testPCMatrixWithCROTA() throws Exception
    {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CDELT1", "1.0");
        defaults.setKeywordValue("CROTA1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CDELT2", " 1.0");
        defaults.setKeywordValue("CROTA2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        defaults.setKeywordValue("PC1_1", "1.000000000000E+00");
        defaults.setKeywordValue("PC2_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_1", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_1", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_2", "1.000000000000E+00");
        defaults.setKeywordValue("PC3_2", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_2", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_3", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_3", "1.000000000000E+00");
        defaults.setKeywordValue("PC4_3", "0.000000000000E+00");

        defaults.setKeywordValue("PC1_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC2_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC3_4", "0.000000000000E+00");
        defaults.setKeywordValue("PC4_4", "1.000000000000E+00");

        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris, config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);

        SpatialWCS position = chunk.position;
        Assert.assertNotNull(position.getAxis());
        Assert.assertEquals("RA---SIN", position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("DEC--SIN", position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis2().getCunit());
        Assert.assertNull(position.getAxis().bounds);
        Assert.assertNull(position.getAxis().range);
        Assert.assertNotNull(position.getAxis().function);

        CoordFunction2D func = position.getAxis().function;
        Assert.assertEquals(11934, func.getDimension().naxis1);
        Assert.assertEquals(10399, func.getDimension().naxis2);
        Assert.assertEquals(9887.0, func.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(321.7181583333, func.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(2529, func.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(-52.34973888889, func.getRefCoord().getCoord2().val, 0.0);

        Assert.assertEquals(1.0, func.getCd11(), 0.1);
        Assert.assertEquals(0.0, func.getCd12(), 0.0);
        Assert.assertEquals(0.0, func.getCd21(), 0.0);
        Assert.assertEquals(1.0, func.getCd22(), 0.1);
    }

    @Test
    public void testDefaultPCMatrix() throws Exception {
        FitsValuesMap defaults = new FitsValuesMap(null, null);
        defaults.setKeywordValue("NAXIS", "4");
        defaults.setKeywordValue("NAXIS1", "11934");
        defaults.setKeywordValue("NAXIS2", "10399");
        defaults.setKeywordValue("NAXIS3", "1");
        defaults.setKeywordValue("NAXIS4", "144");
        defaults.setKeywordValue("BSCALE", "1.000000000000E+00");
        defaults.setKeywordValue("BZERO", "0.000000000000E+00");

        defaults.setKeywordValue("BUNIT", "Jy/beam");
        defaults.setKeywordValue("EQUINOX", "2.000000000000E+03");
        defaults.setKeywordValue("RADESYS", "FK5");
        defaults.setKeywordValue("LONPOLE", "1.800000000000E+02");
        defaults.setKeywordValue("LATPOLE", "-5.234973888889E+01");

        defaults.setKeywordValue("CTYPE1", "RA---SIN");
        defaults.setKeywordValue("CRVAL1", "3.217181583333E+02");
        defaults.setKeywordValue("CDELT1", "-5.555555555556E-04");
        defaults.setKeywordValue("CRPIX1", "9.887000000000E+03");
        defaults.setKeywordValue("CUNIT1", "deg");

        defaults.setKeywordValue("CTYPE2", "DEC--SIN");
        defaults.setKeywordValue("CRVAL2", "-5.234973888889E+01");
        defaults.setKeywordValue("CDELT2", "5.555555555556E-04");
        defaults.setKeywordValue("CRPIX2", "2.529000000000E+03");
        defaults.setKeywordValue("CUNIT2", "deg");

        defaults.setKeywordValue("CTYPE3", "STOKES");
        defaults.setKeywordValue("CRVAL3", "1.000000000000E+00");
        defaults.setKeywordValue("CDELT3", "1.000000000000E+00");
        defaults.setKeywordValue("CRPIX3", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT3", "");

        defaults.setKeywordValue("CTYPE4", "FREQ");
        defaults.setKeywordValue("CRVAL4", "1.294990740741E+09");
        defaults.setKeywordValue("CDELT4", "9.999999999998E+05");
        defaults.setKeywordValue("CRPIX4", "1.000000000000E+00");
        defaults.setKeywordValue("CUNIT4", "Hz");

        defaults.setKeywordValue("RESTFRQ", "1.420405751786E+09");
        defaults.setKeywordValue("SPECSYS", "TOPOCENT");

        String userConfig = null;
        Map<String, String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);

        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);

        Ingest ingest = new Ingest(COLLECTION, OBSERVATION_ID, PRODUCT_ID, uris,
            config);

        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);

        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.polarizationAxis);
        Assert.assertNotNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.observableAxis);

        Assert.assertTrue(chunk.positionAxis1 == 1);
        Assert.assertTrue(chunk.positionAxis2 == 2);
        Assert.assertTrue(chunk.polarizationAxis == 3);
        Assert.assertTrue(chunk.energyAxis == 4);

        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.energy);
        Assert.assertNotNull(chunk.polarization);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.observable);

        SpatialWCS position = chunk.position;
        Assert.assertNotNull(position.getAxis());
        Assert.assertEquals("RA---SIN", position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("DEC--SIN", position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("deg", position.getAxis().getAxis2().getCunit());
        Assert.assertNull(position.getAxis().bounds);
        Assert.assertNull(position.getAxis().range);
        Assert.assertNotNull(position.getAxis().function);

        CoordFunction2D func = position.getAxis().function;
        Assert.assertEquals(11934, func.getDimension().naxis1);
        Assert.assertEquals(10399, func.getDimension().naxis2);
        Assert.assertEquals(9887.0, func.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(321.7181583333, func.getRefCoord().getCoord1().val,
            0.0);
        Assert.assertEquals(2529, func.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(-52.34973888889, func.getRefCoord().getCoord2().val,
            0.0);

        Assert.assertEquals(-5.555555555556E-04, func.getCd11(), 0.1);
        Assert.assertEquals(0.0, func.getCd12(), 0.0);
        Assert.assertEquals(0.0, func.getCd21(), 0.0);
        Assert.assertEquals(5.555555555556E-04, func.getCd22(), 0.1);
    }

}
