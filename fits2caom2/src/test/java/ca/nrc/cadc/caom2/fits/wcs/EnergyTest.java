/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.exceptions.PartialWCSException;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.util.Map;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 *
 * @author jburke
 */
public class EnergyTest
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }
    
    public static Map<String,String> config;
    
    public EnergyTest() { }
    
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }

    /**
     * Test of getEnergy method, of class Energy.
     */
    @Test
    public void testGetEnergy() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.specsys", "specsys");
        
        SpectralWCS spectralWCS = Energy.getEnergy("utype", mapping);
        
        Assert.assertNull(spectralWCS);
        
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.axis.range.start.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.end.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.end.val", "4.0");
        mapping.setArgumentProperty("utype.specsys", "specsys");
        mapping.setArgumentProperty("utype.ssysobs", "ssysobs");
        mapping.setArgumentProperty("utype.ssyssrc", "ssyssrc");
        mapping.setArgumentProperty("utype.restfrq", "1.0");
        mapping.setArgumentProperty("utype.restwav", "2.0");
        mapping.setArgumentProperty("utype.velosys", "3.0");
        mapping.setArgumentProperty("utype.zsource", "4.0");
        mapping.setArgumentProperty("utype.velang", "5.0");
        mapping.setArgumentProperty("utype.bandpassName", "bandpassName");
        mapping.setArgumentProperty("utype.resolvingPower", "6.0");
        
        spectralWCS = Energy.getEnergy("utype", mapping);
        
        Assert.assertNotNull(spectralWCS);
        Assert.assertNotNull(spectralWCS.getAxis());
        Assert.assertEquals("cunit", spectralWCS.getAxis().getAxis().getCunit());
        Assert.assertEquals("ctype", spectralWCS.getAxis().getAxis().getCtype());
        Assert.assertEquals(1.0, spectralWCS.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(2.0, spectralWCS.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(3.0, spectralWCS.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(4.0, spectralWCS.getAxis().range.getEnd().val, 0.0);
        Assert.assertEquals("specsys", spectralWCS.getSpecsys());
        Assert.assertEquals("ssysobs", spectralWCS.ssysobs);
        Assert.assertEquals("ssyssrc", spectralWCS.ssyssrc);
        Assert.assertEquals(1.0, spectralWCS.restfrq, 0.0);
        Assert.assertEquals(2.0, spectralWCS.restwav, 0.0);
        Assert.assertEquals(3.0, spectralWCS.velosys, 0.0);
        Assert.assertEquals(4.0, spectralWCS.zsource, 0.0);
        Assert.assertEquals(5.0, spectralWCS.velang, 0.0);      
        Assert.assertEquals("bandpassName", spectralWCS.bandpassName);
        Assert.assertEquals(6.0, spectralWCS.resolvingPower, 0.0);
    }

    @Test
    public void testPartialWCSException()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.axis.range.start.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.end.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.end.val", "4.0");
        mapping.setArgumentProperty("utype.specsys", "specsys");
        mapping.setArgumentProperty("utype.restfrq", "foo");

        try
        {
            SpectralWCS spectralWCS = Energy.getEnergy("utype", mapping);
            fail("partail WCS should throw PartialWCSException");
        }
        catch (PartialWCSException expected) {}

    }

}
