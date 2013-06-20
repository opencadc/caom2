/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
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
public class ObservableTest
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }

    public static Map<String,String> config;
    
    public ObservableTest() { }
    
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }
    
    /**
     * Test of getObservableAxis method, of class Observable.
     */
    @Test
    public void testGetObservableAxis()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.dependent.axis.cunit", "cunit1");
        mapping.setArgumentProperty("utype.dependent.axis.ctype", "ctype1");
        mapping.setArgumentProperty("utype.dependent.bin", "3");
        mapping.setArgumentProperty("utype.independent.axis.cunit", "cunit2");
        mapping.setArgumentProperty("utype.independent.axis.ctype", "ctype2");
        mapping.setArgumentProperty("utype.independent.bin", "3");
        
        ObservableAxis observable = Observable.getObservableAxis("utype", mapping);
        
        Assert.assertNotNull(observable);
        Assert.assertEquals("cunit1", observable.getDependent().getAxis().getCunit());
        Assert.assertEquals("ctype1", observable.getDependent().getAxis().getCtype());
        Assert.assertEquals(3, observable.getDependent().getBin(), 0.0);
        Assert.assertEquals("cunit2", observable.independent.getAxis().getCunit());
        Assert.assertEquals("ctype2", observable.independent.getAxis().getCtype());
        Assert.assertEquals(3, observable.independent.getBin(), 0.0);
    }
    
    /**
     * Test of getObservable method, of class Observable.
     */
    @Test
    public void testGetObservable()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.observableAxis = new Integer(3);
        mapping.setArgumentProperty("CTYPE3", "ctype3");
        mapping.setArgumentProperty("CUNIT3", "cunit3");
        mapping.setArgumentProperty("CRPIX3", "3");
        
        ObservableAxis observable = Observable.getObservable("utype", mapping);
        
        Assert.assertNotNull(observable);
        Assert.assertEquals("ctype3", observable.getDependent().getAxis().getCtype());
        Assert.assertEquals("cunit3", observable.getDependent().getAxis().getCunit());
        Assert.assertEquals(3, observable.getDependent().getBin(), 0.0);
        Assert.assertNull(observable.independent);
    }

}
