/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.util.Map;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class PositionTest
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }

    public static Map<String,String> config;
    
    public PositionTest() { }
    
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }
    
    /**
     * Test of getPosition method, of class Position.
     */
    @Test
    public void testGetPosition()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis1.cunit", "cunit1");
        mapping.setArgumentProperty("utype.axis.axis1.ctype", "ctype1");
        mapping.setArgumentProperty("utype.axis.axis2.cunit", "cunit2");
        mapping.setArgumentProperty("utype.axis.axis2.ctype", "ctype2");
        mapping.setArgumentProperty("utype.coordsys", "coordsys");
        mapping.setArgumentProperty("utype.equinox", "1.0");
        mapping.setArgumentProperty("utype.resolution", "2.0");
        
        SpatialWCS spatialWCS = Position.getPosition("utype", mapping);
        
        Assert.assertNull(spatialWCS);

        
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis1.cunit", "cunit1");
        mapping.setArgumentProperty("utype.axis.axis1.ctype", "ctype1");
        mapping.setArgumentProperty("utype.axis.axis2.cunit", "cunit2");
        mapping.setArgumentProperty("utype.axis.axis2.ctype", "ctype2");
        mapping.setArgumentProperty("utype.axis.range.start.coord1.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.coord1.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.start.coord2.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.start.coord2.val", "4.0");
        mapping.setArgumentProperty("utype.axis.range.end.coord1.pix", "5.0");
        mapping.setArgumentProperty("utype.axis.range.end.coord1.val", "6.0");
        mapping.setArgumentProperty("utype.axis.range.end.coord2.pix", "7.0");
        mapping.setArgumentProperty("utype.axis.range.end.coord2.val", "8.0");
        mapping.setArgumentProperty("utype.coordsys", "coordsys");
        mapping.setArgumentProperty("utype.equinox", "1.0");
        mapping.setArgumentProperty("utype.resolution", "2.0");
        
        spatialWCS = Position.getPosition("utype", mapping);
        
        Assert.assertNotNull(spatialWCS);
        Assert.assertNotNull(spatialWCS.getAxis());
        Assert.assertNotNull(spatialWCS.getAxis().getAxis1());
        Assert.assertNotNull(spatialWCS.getAxis().getAxis2());
        Assert.assertEquals("cunit1", spatialWCS.getAxis().getAxis1().getCunit());
        Assert.assertEquals("ctype1", spatialWCS.getAxis().getAxis1().getCtype());
        Assert.assertEquals("cunit2", spatialWCS.getAxis().getAxis2().getCunit());
        Assert.assertEquals("ctype2", spatialWCS.getAxis().getAxis2().getCtype());
        Assert.assertEquals(1.0, spatialWCS.getAxis().range.getStart().getCoord1().pix, 0.0);
        Assert.assertEquals(2.0, spatialWCS.getAxis().range.getStart().getCoord1().val, 0.0);
        Assert.assertEquals(3.0, spatialWCS.getAxis().range.getStart().getCoord2().pix, 0.0);
        Assert.assertEquals(4.0, spatialWCS.getAxis().range.getStart().getCoord2().val, 0.0);
        Assert.assertEquals(5.0, spatialWCS.getAxis().range.getEnd().getCoord1().pix, 0.0);
        Assert.assertEquals(6.0, spatialWCS.getAxis().range.getEnd().getCoord1().val, 0.0);
        Assert.assertEquals(7.0, spatialWCS.getAxis().range.getEnd().getCoord2().pix, 0.0);
        Assert.assertEquals(8.0, spatialWCS.getAxis().range.getEnd().getCoord2().val, 0.0);        
        Assert.assertEquals("coordsys", spatialWCS.coordsys);
        Assert.assertEquals(1.0, spatialWCS.equinox, 0.0);
        Assert.assertEquals(2.0, spatialWCS.resolution, 0.0);
        
    }
    
}
