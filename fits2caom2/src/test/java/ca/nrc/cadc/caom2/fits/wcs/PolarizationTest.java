/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.exceptions.PartialWCSException;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
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
public class PolarizationTest
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }

    public static Map<String,String> config;

    public PolarizationTest() { }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }

    /**
     * Test of getPolarization method, of class Polarization.
     */
    @Test
    public void testGetPolarization() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.ctype", "STOKES");
        
        PolarizationWCS polarizationWCS = Polarization.getPolarization("utype", mapping);
        
        Assert.assertNull(polarizationWCS);
        
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.ctype", "STOKES");
        mapping.setArgumentProperty("utype.axis.range.start.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.end.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.end.val", "4.0");
        
        polarizationWCS = Polarization.getPolarization("utype", mapping);
        
        Assert.assertNotNull(polarizationWCS);
        Assert.assertNotNull(polarizationWCS.getAxis());
        
        Assert.assertEquals("STOKES", polarizationWCS.getAxis().getAxis().getCtype());
        Assert.assertNull("cunit", polarizationWCS.getAxis().getAxis().getCunit());
        Assert.assertEquals(1.0, polarizationWCS.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(2.0, polarizationWCS.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(3.0, polarizationWCS.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(4.0, polarizationWCS.getAxis().range.getEnd().val, 0.0);
    }

    @Test
    public void testGetPolarizationTolerant() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.ctype", "STOKES");
        
        PolarizationWCS polarizationWCS = Polarization.getPolarization("utype", mapping);
        
        Assert.assertNull(polarizationWCS);
        
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.ctype", "STOKES");
        mapping.setArgumentProperty("utype.axis.axis.cunit", "      "); // check that we tolerate this abuse of CUNIT
        mapping.setArgumentProperty("utype.axis.range.start.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.end.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.end.val", "4.0");
        
        polarizationWCS = Polarization.getPolarization("utype", mapping);
        
        Assert.assertNotNull(polarizationWCS);
        Assert.assertNotNull(polarizationWCS.getAxis());
        
        Assert.assertEquals("STOKES", polarizationWCS.getAxis().getAxis().getCtype());
        Assert.assertNull("cunit", polarizationWCS.getAxis().getAxis().getCunit());
        Assert.assertEquals(1.0, polarizationWCS.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(2.0, polarizationWCS.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(3.0, polarizationWCS.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(4.0, polarizationWCS.getAxis().range.getEnd().val, 0.0);
    }
    
}
