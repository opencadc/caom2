/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits;

import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class CtypesTest
{
    public CtypesTest() { }
    
    @Test
    public void testIsSpatial() throws Exception
    {
        Assert.assertTrue(Ctypes.isPositionCtype("GLON"));
        Assert.assertFalse(Ctypes.isPositionCtype("TIME"));
    }
    
    @Test
    public void testIsSpectral() throws Exception
    {
        Assert.assertTrue(Ctypes.isEnergyCtype("WAVE"));
        Assert.assertFalse(Ctypes.isEnergyCtype("STOKES"));
    }
    
    @Test
    public void testIsTemporal() throws Exception
    {
        Assert.assertTrue(Ctypes.isTimeCtype("TIME"));
        Assert.assertFalse(Ctypes.isTimeCtype("FREQ"));
    }
    
    @Test
    public void testIsPolarization() throws Exception
    {
        Assert.assertTrue(Ctypes.isPolarizationCtype("STOKES"));
        Assert.assertFalse(Ctypes.isPolarizationCtype("TIME"));
    }
    
}
