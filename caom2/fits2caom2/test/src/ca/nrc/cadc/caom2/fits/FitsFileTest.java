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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.caom2.fits;

import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class FitsFileTest
{
    private static Logger log = Logger.getLogger(FitsFileTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }
    static FitsMapping mapping;
    static BasicHDU[] simple1Headers;
    static BasicHDU[] simple1HeadersGZ;
    static BasicHDU[] simple2Headers;
    static BasicHDU[] mefHeaders;
    
    public FitsFileTest() { }

    @BeforeClass
    public static void setUpClass()
        throws Exception
    {
        Map<String,String> config = Util.loadConfig("config/fits2caom2.config");
        File df = new File("test/config/fits2caom2/fits2caom2-simple.default");
        FitsValuesMap defaults = new FitsValuesMap(new FileReader(df), "default");
        
        mapping = new FitsMapping(config, defaults, null);
        
        File simple1File = new File("test/files/simple1.fits");
        File simple1FileGZ = new File("test/files/simple1.fits.gz");
        File simple2File = new File("test/files/simple2.fits");
        File mefFile = new File("test/files/mef.fits");
        
        Fits simple1Fits = new Fits(simple1File, FitsUtil.isCompressed(simple1File.getAbsolutePath()));
        Fits simple1FitsGZ = new Fits(simple1FileGZ, FitsUtil.isCompressed(simple1FileGZ.getAbsolutePath()));
        Fits simple2Fits = new Fits(simple2File, FitsUtil.isCompressed(simple2File.getAbsolutePath()));
        Fits mefFits = new Fits(mefFile, FitsUtil.isCompressed(mefFile.getAbsolutePath()));
        
        simple1Headers = simple1Fits.read();
        simple1HeadersGZ = simple1FitsGZ.read();
        simple2Headers = simple2Fits.read();
        mefHeaders = mefFits.read();
    }
    
    @Test
    public void testSimple1()
        throws Exception
    {
        mapping.primary = simple1Headers[0].getHeader();
        mapping.header = simple1Headers[0].getHeader();
        
        mapping.positionAxis1 = 1;
        mapping.positionAxis2 = 2;
        mapping.energyAxis = 3;
        mapping.polarizationAxis = 4;
        mapping.timeAxis = 5;
        
        Assert.assertEquals("5", mapping.getMapping("Chunk.naxis"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis1"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis2"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.energy.axis.function.naxis"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.polarization.axis.function.naxis"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.time.axis.function.naxis"));
        
        Assert.assertEquals("FK5", mapping.getMapping("Chunk.position.coordsys"));
        Assert.assertEquals("2000.0", mapping.getMapping("Chunk.position.equinox"));
        
        Assert.assertEquals("GLON-CAR", mapping.getMapping("Chunk.position.axis.axis1.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.position.axis.axis1.cunit"));
        Assert.assertEquals("1.420000000000E+02", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.val"));
        Assert.assertEquals("513.00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.pix"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.position.axis.error1.syser"));
        Assert.assertEquals("1.50", mapping.getMapping("Chunk.position.axis.error1.rnder"));
        Assert.assertEquals("1.10", mapping.getMapping("Chunk.position.axis.function.cd11"));
        Assert.assertEquals("1.20", mapping.getMapping("Chunk.position.axis.function.cd12"));
        Assert.assertEquals("2.10", mapping.getMapping("Chunk.position.axis.function.cd21"));
        Assert.assertEquals("2.20", mapping.getMapping("Chunk.position.axis.function.cd22"));
        
        Assert.assertEquals("GLAT-CAR", mapping.getMapping("Chunk.position.axis.axis2.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.position.axis.axis2.cunit"));
        Assert.assertEquals("1.000000000000E-00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.val"));
        Assert.assertEquals("513.00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.pix"));
        Assert.assertEquals("2.00", mapping.getMapping("Chunk.position.axis.error2.syser"));
        Assert.assertEquals("2.50", mapping.getMapping("Chunk.position.axis.error2.rnder"));
        
        Assert.assertEquals("FREQ", mapping.getMapping("Chunk.energy.axis.axis.ctype"));
        Assert.assertEquals("4.080000000000E+08", mapping.getMapping("Chunk.energy.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.energy.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.energy.axis.function.delta"));
        Assert.assertEquals("3.00", mapping.getMapping("Chunk.energy.axis.error.syser"));
        Assert.assertEquals("3.50", mapping.getMapping("Chunk.energy.axis.error.rnder"));
        
        Assert.assertEquals("BARYCENT", mapping.getMapping("Chunk.energy.specsys"));
        Assert.assertEquals("TOPOCENT", mapping.getMapping("Chunk.energy.ssysobs"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.energy.restfrq"));
        Assert.assertEquals("2.00", mapping.getMapping("Chunk.energy.restwav"));
        Assert.assertEquals("3.00", mapping.getMapping("Chunk.energy.velosys"));
        Assert.assertEquals("4.00", mapping.getMapping("Chunk.energy.zsource"));
        Assert.assertEquals("BARYCENT", mapping.getMapping("Chunk.energy.ssyssrc"));
        Assert.assertEquals("5.00", mapping.getMapping("Chunk.energy.velang"));
        
        Assert.assertEquals("STOKES", mapping.getMapping("Chunk.polarization.axis.axis.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.polarization.axis.axis.cunit"));
        Assert.assertEquals("1.000000000000E+00", mapping.getMapping("Chunk.polarization.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.polarization.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.polarization.axis.function.delta"));
        
        Assert.assertEquals("STOKES", mapping.getMapping("Chunk.time.axis.axis.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.time.axis.axis.cunit"));
        Assert.assertEquals("1.000000000000E+00", mapping.getMapping("Chunk.time.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.time.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.time.axis.function.delta"));
        Assert.assertEquals("5.00", mapping.getMapping("Chunk.time.axis.error.syser"));
        Assert.assertEquals("5.50", mapping.getMapping("Chunk.time.axis.error.rnder"));        
    }

        @Test
    public void testSimple1GZIP()
        throws Exception
    {
        mapping.primary = simple1HeadersGZ[0].getHeader();
        mapping.header = simple1HeadersGZ[0].getHeader();
        
        mapping.positionAxis1 = 1;
        mapping.positionAxis2 = 2;
        mapping.energyAxis = 3;
        mapping.polarizationAxis = 4;
        mapping.timeAxis = 5;
        
        Assert.assertEquals("5", mapping.getMapping("Chunk.naxis"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis1"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis2"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.energy.axis.function.naxis"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.polarization.axis.function.naxis"));
        Assert.assertEquals("1", mapping.getMapping("Chunk.time.axis.function.naxis"));
        
        Assert.assertEquals("FK5", mapping.getMapping("Chunk.position.coordsys"));
        Assert.assertEquals("2000.0", mapping.getMapping("Chunk.position.equinox"));
        
        Assert.assertEquals("GLON-CAR", mapping.getMapping("Chunk.position.axis.axis1.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.position.axis.axis1.cunit"));
        Assert.assertEquals("1.420000000000E+02", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.val"));
        Assert.assertEquals("513.00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.pix"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.position.axis.error1.syser"));
        Assert.assertEquals("1.50", mapping.getMapping("Chunk.position.axis.error1.rnder"));
        Assert.assertEquals("1.10", mapping.getMapping("Chunk.position.axis.function.cd11"));
        Assert.assertEquals("1.20", mapping.getMapping("Chunk.position.axis.function.cd12"));
        Assert.assertEquals("2.10", mapping.getMapping("Chunk.position.axis.function.cd21"));
        Assert.assertEquals("2.20", mapping.getMapping("Chunk.position.axis.function.cd22"));
        
        Assert.assertEquals("GLAT-CAR", mapping.getMapping("Chunk.position.axis.axis2.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.position.axis.axis2.cunit"));
        Assert.assertEquals("1.000000000000E-00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.val"));
        Assert.assertEquals("513.00", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.pix"));
        Assert.assertEquals("2.00", mapping.getMapping("Chunk.position.axis.error2.syser"));
        Assert.assertEquals("2.50", mapping.getMapping("Chunk.position.axis.error2.rnder"));
        
        Assert.assertEquals("FREQ", mapping.getMapping("Chunk.energy.axis.axis.ctype"));
        Assert.assertEquals("4.080000000000E+08", mapping.getMapping("Chunk.energy.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.energy.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.energy.axis.function.delta"));
        Assert.assertEquals("3.00", mapping.getMapping("Chunk.energy.axis.error.syser"));
        Assert.assertEquals("3.50", mapping.getMapping("Chunk.energy.axis.error.rnder"));
        
        Assert.assertEquals("BARYCENT", mapping.getMapping("Chunk.energy.specsys"));
        Assert.assertEquals("TOPOCENT", mapping.getMapping("Chunk.energy.ssysobs"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.energy.restfrq"));
        Assert.assertEquals("2.00", mapping.getMapping("Chunk.energy.restwav"));
        Assert.assertEquals("3.00", mapping.getMapping("Chunk.energy.velosys"));
        Assert.assertEquals("4.00", mapping.getMapping("Chunk.energy.zsource"));
        Assert.assertEquals("BARYCENT", mapping.getMapping("Chunk.energy.ssyssrc"));
        Assert.assertEquals("5.00", mapping.getMapping("Chunk.energy.velang"));
        
        Assert.assertEquals("STOKES", mapping.getMapping("Chunk.polarization.axis.axis.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.polarization.axis.axis.cunit"));
        Assert.assertEquals("1.000000000000E+00", mapping.getMapping("Chunk.polarization.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.polarization.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.polarization.axis.function.delta"));
        
        Assert.assertEquals("STOKES", mapping.getMapping("Chunk.time.axis.axis.ctype"));
        Assert.assertEquals("DEG", mapping.getMapping("Chunk.time.axis.axis.cunit"));
        Assert.assertEquals("1.000000000000E+00", mapping.getMapping("Chunk.time.axis.function.refCoord.val"));
        Assert.assertEquals("1.00", mapping.getMapping("Chunk.time.axis.function.refCoord.pix"));
        Assert.assertEquals("1.0000000E+00", mapping.getMapping("Chunk.time.axis.function.delta"));
        Assert.assertEquals("5.00", mapping.getMapping("Chunk.time.axis.error.syser"));
        Assert.assertEquals("5.50", mapping.getMapping("Chunk.time.axis.error.rnder"));        
    }
 
    @Test
    public void testSimple2()
        throws Exception
    {
        mapping.primary = simple2Headers[0].getHeader();
        mapping.header = simple2Headers[0].getHeader();
        
        Assert.assertEquals("2", mapping.getMapping("Chunk.naxis"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis1"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis2"));
        
        Assert.assertEquals("FK5", mapping.getMapping("Chunk.position.coordsys"));
        Assert.assertEquals("2000.00", mapping.getMapping("Chunk.position.equinox"));

        Assert.assertEquals("GLON-TAN", mapping.getMapping("Chunk.position.axis.axis1.ctype"));
        Assert.assertEquals("deg", mapping.getMapping("Chunk.position.axis.axis1.cunit"));
        Assert.assertEquals("59.258495", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.val"));
        Assert.assertEquals("261.", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.pix"));
        Assert.assertEquals("-0.00500000", mapping.getMapping("Chunk.position.axis.function.cd11"));
        Assert.assertEquals("0.00500000", mapping.getMapping("Chunk.position.axis.function.cd22"));
        
        Assert.assertEquals("GLAT-TAN", mapping.getMapping("Chunk.position.axis.axis2.ctype"));
        Assert.assertEquals("deg", mapping.getMapping("Chunk.position.axis.axis2.cunit"));
        Assert.assertEquals("0.360181", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.val"));
        Assert.assertEquals("261.", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.pix"));
    }
    
    @Test
    public void testMef()
        throws Exception
    {
        mapping.primary = mefHeaders[1].getHeader();
        mapping.header = mefHeaders[1].getHeader();
        
        Assert.assertEquals("2", mapping.getMapping("Chunk.naxis"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis1"));
        Assert.assertEquals("30", mapping.getMapping("Chunk.position.axis.function.dimension.naxis2"));
        
        Assert.assertEquals("RA---TNX", mapping.getMapping("Chunk.position.axis.axis1.ctype"));
        Assert.assertEquals("76.3387499968215", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.val"));
        Assert.assertEquals("-1095.17845710054", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.pix"));
        Assert.assertEquals("1.77195075985530E-04", mapping.getMapping("Chunk.position.axis.function.cd11"));
        Assert.assertEquals("4.76033337876970E-07", mapping.getMapping("Chunk.position.axis.function.cd12"));
        Assert.assertEquals("1.64815679136849E-07", mapping.getMapping("Chunk.position.axis.function.cd21"));
        Assert.assertEquals("1.77262471401612E-04", mapping.getMapping("Chunk.position.axis.function.cd22"));
        
        Assert.assertEquals("DEC--TNX", mapping.getMapping("Chunk.position.axis.axis2.ctype"));
        Assert.assertEquals("-69.08717903879", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.val"));
        Assert.assertEquals("2177.5992406621", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.pix"));
        
        mapping.header = mefHeaders[2].getHeader();
        
        Assert.assertEquals("RA---TNX", mapping.getMapping("Chunk.position.axis.axis1.ctype"));
        Assert.assertEquals("76.3387499968215", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.val"));
        Assert.assertEquals("-149.530454330166", mapping.getMapping("Chunk.position.axis.function.refCoord.coord1.pix"));
        Assert.assertEquals("1.77617138827926E-04", mapping.getMapping("Chunk.position.axis.function.cd11"));
        Assert.assertEquals("3.24313927479335E-07", mapping.getMapping("Chunk.position.axis.function.cd12"));
        Assert.assertEquals("1.68278241908201E-08", mapping.getMapping("Chunk.position.axis.function.cd21"));
        Assert.assertEquals("1.77414393439292E-04", mapping.getMapping("Chunk.position.axis.function.cd22"));
        
        Assert.assertEquals("DEC--TNX", mapping.getMapping("Chunk.position.axis.axis2.ctype"));
        Assert.assertEquals("-69.08717903879", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.val"));
        Assert.assertEquals("2175.628062863", mapping.getMapping("Chunk.position.axis.function.refCoord.coord2.pix"));
    }
    
}
