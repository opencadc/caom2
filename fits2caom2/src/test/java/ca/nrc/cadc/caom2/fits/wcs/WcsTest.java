/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.util.List;
import java.util.Map;
import nom.tam.fits.Header;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class WcsTest
{    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }

    public static Map<String,String> config;
    
    public WcsTest() { }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }

    @Test
    public void testGetPositionAxis() throws Exception
    {
        Integer naxis = 5;
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.header = new Header();
        mapping.header.addValue("CTYPE1", "RA", null);
        mapping.header.addValue("CTYPE2", "DEC", null);
        mapping.header.addValue("CTYPE3", "WAVE", null);
        mapping.header.addValue("CTYPE4", "STOKES", null);
        mapping.header.addValue("CTYPE5", "TIME", null);
        
        Integer[] axes = Wcs.getPositionAxis(naxis, mapping);
        
        junit.framework.Assert.assertNotNull(axes);
        junit.framework.Assert.assertNotNull(axes[0]);
        junit.framework.Assert.assertNotNull(axes[1]);
        junit.framework.Assert.assertEquals(1, axes[0], 0.0);
        junit.framework.Assert.assertEquals(2, axes[1], 0.0);
    }
    
    @Test
    public void testGetEnergyAxis() throws Exception
    {
        Integer naxis = 5;
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.header = new Header();
        mapping.header.addValue("CTYPE1", "RA", null);
        mapping.header.addValue("CTYPE2", "DEC", null);
        mapping.header.addValue("CTYPE3", "WAVE", null);
        mapping.header.addValue("CTYPE4", "STOKES", null);
        mapping.header.addValue("CTYPE5", "TIME", null);
        
        Integer axis = Wcs.getEnergyAxis(naxis, mapping);
        
        junit.framework.Assert.assertNotNull(axis);
        junit.framework.Assert.assertEquals(3, axis, 0.0);
    }
    
    @Test
    public void testGetPolarizationAxis() throws Exception
    {
        Integer naxis = 5;
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.header = new Header();
        mapping.header.addValue("CTYPE1", "RA", null);
        mapping.header.addValue("CTYPE2", "DEC", null);
        mapping.header.addValue("CTYPE3", "WAVE", null);
        mapping.header.addValue("CTYPE4", "STOKES", null);
        mapping.header.addValue("CTYPE5", "TIME", null);
        
        Integer axis = Wcs.getPolarizationAxis(naxis, mapping);
        
        junit.framework.Assert.assertNotNull(axis);
        junit.framework.Assert.assertEquals(4, axis, 0.0);
    }
    
    @Test
    public void testGetTimeAxis() throws Exception
    {
        Integer naxis = 5;
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.header = new Header();
        mapping.header.addValue("CTYPE1", "RA", null);
        mapping.header.addValue("CTYPE2", "DEC", null);
        mapping.header.addValue("CTYPE3", "WAVE", null);
        mapping.header.addValue("CTYPE4", "TIME", null);
        mapping.header.addValue("CTYPE5", "STOKES", null);
        
        
        Integer axis = Wcs.getTimeAxis(naxis, mapping);
        
        junit.framework.Assert.assertNotNull(axis);
        junit.framework.Assert.assertEquals(4, axis, 0.0);

        // swap to check boundary condition
        mapping.header.addValue("CTYPE4", "STOKES", null);
        mapping.header.addValue("CTYPE5", "TIME", null);

        axis = Wcs.getTimeAxis(naxis, mapping);

        junit.framework.Assert.assertNotNull(axis);
        junit.framework.Assert.assertEquals(5, axis, 0.0);
    }
    
    /**
     * Test of getEnergyTransition method, of class Wcs.
     */
    @Test
    public void testGetEnergyTransition()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.species", "species");
        mapping.setArgumentProperty("utype.transition", "transition");
        
        EnergyTransition energyTransition = Wcs.getEnergyTransition("utype", mapping);
        
        Assert.assertNotNull(energyTransition);
        Assert.assertEquals("species", energyTransition.getSpecies());
        Assert.assertEquals("transition", energyTransition.getTransition());
    }
    
    /**
     * Test of getAxis method, of class Wcs.
     */
    @Test
    public void testGetAxis()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.cunit", "cunit");
        mapping.setArgumentProperty("utype.ctype", "ctype");
        
        Axis axis = Wcs.getAxis("utype", mapping);
        
        Assert.assertNotNull(axis);
        Assert.assertEquals("cunit", axis.getCunit());
        Assert.assertEquals("ctype", axis.getCtype());
    }

    /**
     * Test of getCoord2D method, of class Wcs.
     */
    @Test
    public void testGetCoord2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.coord1.pix", "1.0");
        mapping.setArgumentProperty("utype.coord1.val", "2.0");
        mapping.setArgumentProperty("utype.coord2.pix", "3.0");
        mapping.setArgumentProperty("utype.coord2.val", "4.0");
        
        Coord2D coord2D = Wcs.getCoord2D("utype", mapping);
        
        Assert.assertNotNull(coord2D);
        Assert.assertEquals(1.0, coord2D.getCoord1().pix, 0.0);
        Assert.assertEquals(2.0, coord2D.getCoord1().val, 0.0);
        Assert.assertEquals(3.0, coord2D.getCoord2().pix, 0.0);
        Assert.assertEquals(4.0, coord2D.getCoord2().val, 0.0);
    }

    /**
     * Test of getCoordAxis1D method, of class Wcs.
     */
    @Test
    public void testGetCoordAxis1D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.ctype", "ctype");
        
        CoordAxis1D coordAxis1D = Wcs.getCoordAxis1D("utype", mapping);
        
        Assert.assertNotNull(coordAxis1D);
        Assert.assertNotNull(coordAxis1D.getAxis());
        Assert.assertEquals("cunit", coordAxis1D.getAxis().getCunit());
        Assert.assertEquals("ctype", coordAxis1D.getAxis().getCtype());
    }

    /**
     * Test of getCoordAxis2D method, of class Wcs.
     */
    @Test
    public void testGetCoordAxis2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis1.cunit", "cunit1");
        mapping.setArgumentProperty("utype.axis1.ctype", "ctype1");
        mapping.setArgumentProperty("utype.axis2.cunit", "cunit2");
        mapping.setArgumentProperty("utype.axis2.ctype", "ctype2");
        
        CoordAxis2D coordAxis2D = Wcs.getCoordAxis2D("utype", mapping);
        
        Assert.assertNotNull(coordAxis2D);
        Assert.assertNotNull(coordAxis2D.getAxis1());
        Assert.assertNotNull(coordAxis2D.getAxis2());
        Assert.assertEquals("cunit1", coordAxis2D.getAxis1().getCunit());
        Assert.assertEquals("ctype1", coordAxis2D.getAxis1().getCtype());
        Assert.assertEquals("cunit2", coordAxis2D.getAxis2().getCunit());
        Assert.assertEquals("ctype2", coordAxis2D.getAxis2().getCtype());
    }

    /**
     * Test of getCoordBounds1D method, of class Wcs.
     */
    @Test
    public void testGetCoordBounds1D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        CoordBounds1D coordBounds1D = Wcs.getCoordBounds1D("utype", mapping);
        
        Assert.assertNull(coordBounds1D);
    }

    /**
     * Test of getCoordBounds2D method, of class Wcs.
     */
    @Test
    public void testGetCoordBounds2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.center.coord1", "1.0");
        mapping.setArgumentProperty("utype.center.coord2", "2.0");
        mapping.setArgumentProperty("utype.radius", "5.0");

        CoordBounds2D coordBounds2D = Wcs.getCoordBounds2D("utype", mapping);
        
        Assert.assertNotNull(coordBounds2D);
        Assert.assertTrue(coordBounds2D instanceof CoordCircle2D);
        
        CoordCircle2D coordCircle2D = (CoordCircle2D) coordBounds2D;
        Assert.assertEquals(1.0, coordCircle2D.getCenter().coord1, 0.0);
        Assert.assertEquals(2.0, coordCircle2D.getCenter().coord2, 0.0);
        Assert.assertEquals(5.0, coordCircle2D.getRadius(), 0.0);
    }

    /**
     * Test of getCoordCircle2D method, of class Wcs.
     */
    @Test
    public void testGetCoordCircle2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.center.coord1", "1.0");
        mapping.setArgumentProperty("utype.center.coord2", "2.0");
        mapping.setArgumentProperty("utype.radius", "5.0");

        CoordCircle2D coordCircle2D = Wcs.getCoordCircle2D("utype", mapping);
        
        Assert.assertNotNull(coordCircle2D);
        Assert.assertNotNull(coordCircle2D.getCenter());
        Assert.assertEquals(1.0, coordCircle2D.getCenter().coord1, 0.0);
        Assert.assertEquals(2.0, coordCircle2D.getCenter().coord2, 0.0);
        Assert.assertEquals(5.0, coordCircle2D.getRadius(), 0.0);
    }
    
    @Test
    public void testgetCoordPolygon2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.vertices", "1.0 1.0 3.0 1.0 2.0 2.0");
        
        try
        {
            CoordPolygon2D coordPolygon2D = Wcs.getCoordPolygon2D("utype", mapping);
            Assert.fail("expected unsupportedOperationException, got: " + coordPolygon2D);
        }
        catch(UnsupportedOperationException expected) { }
    }

    /**
     * Test of getCoordError method, of class Wcs.
     */
    @Test
    public void testGetCoordError()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.syser", "1.0");
        mapping.setArgumentProperty("utype.rnder", "2.0");

        CoordError coordError = Wcs.getCoordError("utype", mapping);
        
        Assert.assertNotNull(coordError);
        Assert.assertEquals(1.0, coordError.syser, 0.0);
        Assert.assertEquals(2.0, coordError.rnder, 0.0);
    }

    /**
     * Test of getCoordFunction1D method, of class Wcs.
     */
    @Test
    public void testGetCoordFunction1D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.naxis", "1");
        mapping.setArgumentProperty("utype.delta", "2.0");
        mapping.setArgumentProperty("utype.refCoord.pix", "3.0");
        mapping.setArgumentProperty("utype.refCoord.val", "4.0");

        CoordFunction1D coordFunction1D = Wcs.getCoordFunction1D("utype", mapping);
        
        Assert.assertNotNull(coordFunction1D);
        Assert.assertEquals(1, coordFunction1D.getNaxis(), 0.0);
        Assert.assertEquals(2.0, coordFunction1D.getDelta(), 0.0);
        Assert.assertEquals(3.0, coordFunction1D.getRefCoord().pix, 0.0);
        Assert.assertEquals(4.0, coordFunction1D.getRefCoord().val, 0.0);
    }

    /**
     * Test of getCoordFunction2D method, of class Wcs.
     */
    @Test
    public void testGetCoordFunction2D() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.dimension.naxis1", "1");
        mapping.setArgumentProperty("utype.dimension.naxis2", "2");
        mapping.setArgumentProperty("utype.refCoord.coord1.pix", "3.0");
        mapping.setArgumentProperty("utype.refCoord.coord1.val", "4.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.pix", "5.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.val", "6.0");
        mapping.setArgumentProperty("utype.cd11", "7.0");
        mapping.setArgumentProperty("utype.cd12", "8.0");
        mapping.setArgumentProperty("utype.cd21", "9.0");
        mapping.setArgumentProperty("utype.cd22", "10.0");

        CoordFunction2D coordFunction2D = Wcs.getCoordFunction2D("utype", mapping);
        
        Assert.assertNotNull(coordFunction2D);
        Assert.assertEquals(1, coordFunction2D.getDimension().naxis1, 0.0);
        Assert.assertEquals(2, coordFunction2D.getDimension().naxis2, 0.0);
        Assert.assertEquals(3.0, coordFunction2D.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(4.0, coordFunction2D.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(5.0, coordFunction2D.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(6.0, coordFunction2D.getRefCoord().getCoord2().val, 0.0);
        Assert.assertEquals(7.0, coordFunction2D.getCd11(), 0.0);
        Assert.assertEquals(8.0, coordFunction2D.getCd12(), 0.0);
        Assert.assertEquals(9.0, coordFunction2D.getCd21(), 0.0);
        Assert.assertEquals(10.0, coordFunction2D.getCd22(), 0.0);
        
        // Test setting cd12 and cd21 when null and cd11 and cd22 are set.
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.dimension.naxis1", "1");
        mapping.setArgumentProperty("utype.dimension.naxis2", "2");
        mapping.setArgumentProperty("utype.refCoord.coord1.pix", "3.0");
        mapping.setArgumentProperty("utype.refCoord.coord1.val", "4.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.pix", "5.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.val", "6.0");
        mapping.setArgumentProperty("utype.cd11", "7.0");
        mapping.setArgumentProperty("utype.cd22", "8.0");

        coordFunction2D = Wcs.getCoordFunction2D("utype", mapping);
        
        Assert.assertNotNull(coordFunction2D);
        Assert.assertEquals(1, coordFunction2D.getDimension().naxis1, 0.0);
        Assert.assertEquals(2, coordFunction2D.getDimension().naxis2, 0.0);
        Assert.assertEquals(3.0, coordFunction2D.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(4.0, coordFunction2D.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(5.0, coordFunction2D.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(6.0, coordFunction2D.getRefCoord().getCoord2().val, 0.0);
        Assert.assertEquals(7.0, coordFunction2D.getCd11(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd12(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd21(), 0.0);
        Assert.assertEquals(8.0, coordFunction2D.getCd22(), 0.0);
        
        // Test setting the cd matrix using CDELT and CROTA when cd?? are null.
        mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.dimension.naxis1", "1");
        mapping.setArgumentProperty("utype.dimension.naxis2", "2");
        mapping.setArgumentProperty("utype.refCoord.coord1.pix", "3.0");
        mapping.setArgumentProperty("utype.refCoord.coord1.val", "4.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.pix", "5.0");
        mapping.setArgumentProperty("utype.refCoord.coord2.val", "6.0");
        
        Header header = new Header();
        header.addValue("CDELT1", 7.0, "");
        header.addValue("CDELT2", 8.0, "");
        header.addValue("CROTA1", 0.0, "");
        header.addValue("CROTSIGN", 1.0, "");
        mapping.header = header;
        mapping.positionAxis1 = 1;
        mapping.positionAxis2 = 2;

        coordFunction2D = Wcs.getCoordFunction2D("utype", mapping);
        
        Assert.assertNotNull(coordFunction2D);
        Assert.assertEquals(1, coordFunction2D.getDimension().naxis1, 0.0);
        Assert.assertEquals(2, coordFunction2D.getDimension().naxis2, 0.0);
        Assert.assertEquals(3.0, coordFunction2D.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(4.0, coordFunction2D.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(5.0, coordFunction2D.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(6.0, coordFunction2D.getRefCoord().getCoord2().val, 0.0);
        Assert.assertEquals(7.0, coordFunction2D.getCd11(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd12(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd21(), 0.0);
        Assert.assertEquals(8.0, coordFunction2D.getCd22(), 0.0);
        
        // Reserve the rotation.
        header = new Header();
        header.addValue("CDELT1", 7.0, "");
        header.addValue("CDELT2", 8.0, "");
        header.addValue("CROTA1", 0.0, "");
        header.addValue("CROTSIGN", -1.0, "");
        mapping.header = header;
        
        coordFunction2D = Wcs.getCoordFunction2D("utype", mapping);
        
        Assert.assertNotNull(coordFunction2D);
        Assert.assertEquals(1, coordFunction2D.getDimension().naxis1, 0.0);
        Assert.assertEquals(2, coordFunction2D.getDimension().naxis2, 0.0);
        Assert.assertEquals(3.0, coordFunction2D.getRefCoord().getCoord1().pix, 0.0);
        Assert.assertEquals(4.0, coordFunction2D.getRefCoord().getCoord1().val, 0.0);
        Assert.assertEquals(5.0, coordFunction2D.getRefCoord().getCoord2().pix, 0.0);
        Assert.assertEquals(6.0, coordFunction2D.getRefCoord().getCoord2().val, 0.0);
        Assert.assertEquals(-7.0, coordFunction2D.getCd11(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd12(), 0.0);
        Assert.assertEquals(0.0, coordFunction2D.getCd21(), 0.0);
        Assert.assertEquals(-8.0, coordFunction2D.getCd22(), 0.0);
    }

    /**
     * Test of getCoordPolygon2D method, of class Wcs.
     */
    @Test
    public void testGetCoordPolygon2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        CoordPolygon2D coordPolygon2D = Wcs.getCoordPolygon2D("utype", mapping);
        
        Assert.assertNull(coordPolygon2D);
    }

    /**
     * Test of getCoordRange1D method, of class Wcs.
     */
    @Test
    public void testGetCoordRange1D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.start.pix", "1.0");
        mapping.setArgumentProperty("utype.start.val", "2.0");
        mapping.setArgumentProperty("utype.end.pix", "3.0");
        mapping.setArgumentProperty("utype.end.val", "4.0");
        
        CoordRange1D coordRange1D = Wcs.getCoordRange1D("utype", mapping);
        
        Assert.assertNotNull(coordRange1D);
        Assert.assertEquals(1.0, coordRange1D.getStart().pix, 0.0);
        Assert.assertEquals(2.0, coordRange1D.getStart().val, 0.0);
        Assert.assertEquals(3.0, coordRange1D.getEnd().pix, 0.0);
        Assert.assertEquals(4.0, coordRange1D.getEnd().val, 0.0);
    }

    /**
     * Test of getCoordRange2D method, of class Wcs.
     */
    @Test
    public void testGetCoordRange2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.start.coord1.pix", "1.0");
        mapping.setArgumentProperty("utype.start.coord1.val", "2.0");
        mapping.setArgumentProperty("utype.start.coord2.pix", "3.0");
        mapping.setArgumentProperty("utype.start.coord2.val", "4.0");
        mapping.setArgumentProperty("utype.end.coord1.pix", "5.0");
        mapping.setArgumentProperty("utype.end.coord1.val", "6.0");
        mapping.setArgumentProperty("utype.end.coord2.pix", "7.0");
        mapping.setArgumentProperty("utype.end.coord2.val", "8.0");
       
        CoordRange2D coordRange2D = Wcs.getCoordRange2D("utype", mapping);
        
        Assert.assertNotNull(coordRange2D);
        Assert.assertEquals(1.0, coordRange2D.getStart().getCoord1().pix, 0.0);
        Assert.assertEquals(2.0, coordRange2D.getStart().getCoord1().val, 0.0);
        Assert.assertEquals(3.0, coordRange2D.getStart().getCoord2().pix, 0.0);
        Assert.assertEquals(4.0, coordRange2D.getStart().getCoord2().val, 0.0);
        Assert.assertEquals(5.0, coordRange2D.getEnd().getCoord1().pix, 0.0);
        Assert.assertEquals(6.0, coordRange2D.getEnd().getCoord1().val, 0.0);
        Assert.assertEquals(7.0, coordRange2D.getEnd().getCoord2().pix, 0.0);
        Assert.assertEquals(8.0, coordRange2D.getEnd().getCoord2().val, 0.0);
    }

    /**
     * Test of getDimension2D method, of class Wcs.
     */
    @Test
    public void testGetDimension2D()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.naxis1", "1");
        mapping.setArgumentProperty("utype.naxis2", "2");

        Dimension2D dimension2D = Wcs.getDimension2D("utype", mapping);
        
        Assert.assertNotNull(dimension2D);
        Assert.assertEquals(1, dimension2D.naxis1, 0.0);
        Assert.assertEquals(2, dimension2D.naxis2, 0.0);
    }

    @Test
    public void testGetValueCoord()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.coord1", "1.0");
        mapping.setArgumentProperty("utype.coord2", "2.0");
        
        ValueCoord2D val = Wcs.getValueCoord2D("utype", mapping);
        
        Assert.assertNotNull(val);
        Assert.assertEquals(1.0, val.coord1, 0.0);
        Assert.assertEquals(2.0, val.coord2, 0.0);
    }
    
    /**
     * Test of getRefCoord method, of class Wcs.
     */
    @Test
    public void testGetRefCoord()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.pix", "1.0");
        mapping.setArgumentProperty("utype.val", "2.0");
        
        RefCoord refCoord = Wcs.getRefCoord("utype", mapping);
        
        Assert.assertNotNull(refCoord);
        Assert.assertEquals(1.0, refCoord.pix, 0.0);
        Assert.assertEquals(2.0, refCoord.val, 0.0);
    }

    /**
     * Test of getSlice method, of class Wcs.
     */
    @Test
    public void testGetSlice()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.bin", "3");
        
        Slice slice = Wcs.getSlice("utype", mapping);
        
        Assert.assertNotNull(slice);
        Assert.assertEquals("cunit", slice.getAxis().getCunit());
        Assert.assertEquals("ctype", slice.getAxis().getCtype());
        Assert.assertEquals(3, slice.getBin(), 0.0);
    }

    /**
     * Test of getStringValue method, of class Wcs.
     */
    @Test
    public void testGetStringValue()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype", "value");
        String value = Wcs.getStringValue("utype", mapping);
        Assert.assertNotNull(value);
        Assert.assertEquals("value", value);
    }

    /**
     * Test of getDoubleValue method, of class Wcs.
     */
    @Test
    public void testGetDoubleValue()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype", "1.0");
        Double value = Wcs.getDoubleValue("utype", mapping);
        Assert.assertNotNull(value);
        Assert.assertEquals(1.0, value, 0.0);
    }

    /**
     * Test of getLongValue method, of class Wcs.
     */
    @Test
    public void testGetLongValue()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype", "1");
        Long value = Wcs.getLongValue("utype", mapping);
        Assert.assertNotNull(value);
        Assert.assertEquals(1, value, 0.0);
    }

    /**
     * Test of getVertices method, of class Wcs.
     */
    @Test
    public void testGetVertices()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        List<ValueCoord2D> vertices = Wcs.getVertices("utype.vertices", mapping);
        Assert.assertNull(vertices);
        try
        {
            mapping.setArgumentProperty("utype.vertices", "any string content will fail");
            vertices = Wcs.getVertices("utype.vertices", mapping);
            Assert.fail("expected unsupportedOperationException, got: " + vertices);
        }
        catch(UnsupportedOperationException expected) { }
    }

    /**
     * Test of getSamples method, of class Wcs.
     */
    @Test
    public void testGetSamples()
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        List<CoordRange1D> samples = Wcs.getSamples("utype.samples", mapping);
        Assert.assertNull(samples);
        
        try
        {
            mapping.setArgumentProperty("utype.samples", "any string content will fail");
            samples = Wcs.getSamples("utype.samples", mapping);
            Assert.fail("expected unsupportedOperationException, got: " + samples);
        }
        catch(UnsupportedOperationException expected) { }
    }
}
