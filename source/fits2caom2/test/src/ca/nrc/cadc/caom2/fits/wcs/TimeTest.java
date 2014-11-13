/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.exceptions.PartialWCSException;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import nom.tam.fits.Header;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class TimeTest extends Time
{    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }

    public static Map<String,String> config;
    
    public TimeTest() { }
    
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        config = Util.loadConfig("config/fits2caom2.config");
    }

    /**
     * Test of getTime method, of class Time.
     */
    @Test
    public void testGetTime() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.cunit", "cunit");
        mapping.setArgumentProperty("utype.axis.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.exposure", "1.0");
        mapping.setArgumentProperty("utype.resolution", "2.0");
        
        TemporalWCS time = Time.getTime("utype", mapping);
        
        Assert.assertNull(time);
        
        // MJD-OBS and MJD-END
        mapping = new FitsMapping(config, null, null);
        Header header = new Header();
        header.addValue("MJD-OBS", 50000, "");
        header.addValue("MJD-END", 50001, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // EXPSTART and EXPEND
        header = new Header();
        header.addValue("EXPSTART", 50000, "");
        header.addValue("EXPEND", 50001, "");
        mapping.header = header;
        
        time = getEXPTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // DATE-OBS and DATE-END as datetime.
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07T10:50:30.000", "");
        header.addValue("DATE-END", "2012-06-07T10:51:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(56085.451736, time.getAxis().range.getStart().val, 0.000001);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(56085.452431, time.getAxis().range.getEnd().val, 0.000001); 
    }
    
    @Test
    public void testGetMJDTime() throws Exception
    {
        // Not mapping and null exposure.
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        TemporalWCS time = getMJDTime(mapping, null);
        
        Assert.assertNull(time);
        
        // MJD-OBS
        Header header = new Header();
        header.addValue("MJD-OBS", 50000, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, null);
        
        Assert.assertNull(time);
        
        // MJD-OBS and MJD-END
        header = new Header();
        header.addValue("MJD-OBS", 50000, "");
        header.addValue("MJD-END", 50001, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // MJD-OBS and MJD-END
        header = new Header();
        header.addValue("MJD-OBS", 50000, "");
        header.addValue("MJD-END", 50001, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // MJDDATE and MJD-END
        header = new Header();
        header.addValue("MJDDATE", 50000, "");
        header.addValue("MJD-END", 50001, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // MJD-OBS and exposure time
        header = new Header();
        header.addValue("MJD-OBS", 50000, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, 86400.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
     
        // MJDDATE and exposure time
        header = new Header();
        header.addValue("MJDDATE", 50000, "");
        mapping.header = header;
        
        time = getMJDTime(mapping, 86400.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
    }
    
    @Test
    public void testGetEXPTime() throws Exception
    {
        // Not mapping and null exposure.
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        TemporalWCS time = getEXPTime(mapping, null);
        
        Assert.assertNull(time);
        
        // EXPSTART
        Header header = new Header();
        header.addValue("EXPSTART", 50000, "");
        mapping.header = header;
        
        time = getEXPTime(mapping, null);
        
        Assert.assertNull(time);
        
        // EXPSTART and EXPEND
        header = new Header();
        header.addValue("EXPSTART", 50000, "");
        header.addValue("EXPEND", 50001, "");
        mapping.header = header;
        
        time = getEXPTime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
        
        // EXPSTART and exposure time
        header = new Header();
        header.addValue("EXPSTART", 50000, "");
        mapping.header = header;
        
        time = getEXPTime(mapping, 86400.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(50000.0, time.getAxis().range.getStart().val, 0.0);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(50001.0, time.getAxis().range.getEnd().val, 0.0); 
    }
        
    @Test
    public void testGetDATETime() throws Exception
    {
        // Not mapping and null exposure.
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        TemporalWCS time = getDATETime(mapping, null);
        
        Assert.assertNull(time);
        
        // DATE-OBS
        Header header = new Header();
        header.addValue("DATE-OBS", "2012-06-07T10:51:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, null);
        
        Assert.assertNull(time);
        
        // DATE-OBS and DATE-END as datetime.
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07T10:50:30.000", "");
        header.addValue("DATE-END", "2012-06-07T10:51:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, null);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(56085.451736, time.getAxis().range.getStart().val, 0.000001);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(56085.452431, time.getAxis().range.getEnd().val, 0.000001); 
        
        // DATE-OBS and exposure time of 60 sec.
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07T10:50:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, 60.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(56085.451736, time.getAxis().range.getStart().val, 0.000001);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(56085.452431, time.getAxis().range.getEnd().val, 0.000001); 
        
        // DATE-OBS and TIME-OBS
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07", "");
        header.addValue("TIME-OBS", "10:51:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, null);
        
        Assert.assertNull(time);

        // DATE-OBS, UTIME
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07", "");
        header.addValue("UTIME", "10:51:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, null);
        
        Assert.assertNull(time);
        
        // DATE-OBS, TIME-OBS, exposure time.        
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07", "");
        header.addValue("TIME-OBS", "10:50:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, 60.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(56085.451736, time.getAxis().range.getStart().val, 0.000001);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(56085.452431, time.getAxis().range.getEnd().val, 0.000001); 
        
        // DATE-OBS , UTIME, exposure time.        
        header = new Header();
        header.addValue("DATE-OBS", "2012-06-07", "");
        header.addValue("UTIME", "10:50:30.000", "");
        mapping.header = header;
        
        time = getDATETime(mapping, 60.0);
        
        Assert.assertNotNull(time);
        Assert.assertNotNull(time.getAxis());
        Assert.assertEquals("d", time.getAxis().getAxis().getCunit());
        Assert.assertEquals("UTC", time.getAxis().getAxis().getCtype());
        Assert.assertEquals(0.5, time.getAxis().range.getStart().pix, 0.0);
        Assert.assertEquals(56085.451736, time.getAxis().range.getStart().val, 0.000001);
        Assert.assertEquals(1.5, time.getAxis().range.getEnd().pix, 0.0);
        Assert.assertEquals(56085.452431, time.getAxis().range.getEnd().val, 0.000001);
    }
    
    @Test
    public void testGetExposureTime() throws Exception
    {
        // Exposure from utype
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.exposure", "1.5");
        
        Double exposure = getExposureTime("utype", mapping);
        
        Assert.assertNotNull(exposure);
        Assert.assertEquals(1.5, exposure, 0.0);
        
        // Exposure from EXPTIME keyword
        mapping = new FitsMapping(config, null, null); 
        
        Header header = new Header();
        header.addValue("EXPTIME", "2.5", "");
        mapping.header = header;
        
        exposure = getExposureTime("utype", mapping);
        
        Assert.assertNotNull(exposure);
        Assert.assertEquals(2.5, exposure, 0.0);
        
        // Exposure from INTTIME keyword
        mapping = new FitsMapping(config, null, null);
        
        header = new Header();
        header.addValue("INTTIME", "3.5", "");
        mapping.header = header;
        
        exposure = getExposureTime("utype", mapping);
        
        Assert.assertNotNull(exposure);
        Assert.assertEquals(3.5, exposure, 0.0);
    }
    
    @Test
    public void testGetModifiedJulianDate() throws Exception
    {
        // null parameter
        Double mjd = getModifiedJulianDate("keyword", null);
        
        Assert.assertNull(mjd);
        
        // MJD date
        mjd = getModifiedJulianDate("keyword", "50000");
        
        Assert.assertNotNull(mjd);
        Assert.assertEquals(50000, mjd, 0.0);
        
        // JD date
        mjd = getModifiedJulianDate("keyword", "2450000.5");
        
        Assert.assertNotNull(mjd);
        Assert.assertEquals(50000, mjd, 0.0);
        
        // Out of range dates.
        mjd = getModifiedJulianDate("keyword", "3450000.5");
        
        Assert.assertNull(mjd);
        
        mjd = getModifiedJulianDate("keyword", "-500000");
        
        Assert.assertNull(mjd);
    }
    
    @Test
    public void testParseDateTimeFormats() throws Exception
    {
        // IVOA date
        Date date = parseDateTimeFormats("2013-06-13T13:05:59.123");
        
        Assert.assertNotNull(date);
        
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(2013, cal.get(Calendar.YEAR));
        Assert.assertEquals(5, cal.get(Calendar.MONTH));
        Assert.assertEquals(13, cal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(13, cal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(5, cal.get(Calendar.MINUTE));
        Assert.assertEquals(59, cal.get(Calendar.SECOND));
        Assert.assertEquals(123, cal.get(Calendar.MILLISECOND));
        
        // ISO date
        date = parseDateTimeFormats("2013-06-13 13:05:59.123");
        
        Assert.assertNotNull(date);
        
        cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(2013, cal.get(Calendar.YEAR));
        Assert.assertEquals(5, cal.get(Calendar.MONTH));
        Assert.assertEquals(13, cal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(13, cal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(5, cal.get(Calendar.MINUTE));
        Assert.assertEquals(59, cal.get(Calendar.SECOND));
        Assert.assertEquals(123, cal.get(Calendar.MILLISECOND));
        
        // ISO date with timezone
        date = parseDateTimeFormats("2013-06-13 13:05:59.123PST");
        
        Assert.assertNotNull(date);
        
        cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(2013, cal.get(Calendar.YEAR));
        Assert.assertEquals(5, cal.get(Calendar.MONTH));
        Assert.assertEquals(13, cal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(13, cal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(5, cal.get(Calendar.MINUTE));
        Assert.assertEquals(59, cal.get(Calendar.SECOND));
        Assert.assertEquals(123, cal.get(Calendar.MILLISECOND));
    }
    
    @Test
    public void testParseDateFormats() throws Exception
    {
        // yyyy-MM-dd date format
        Date date = parseDateFormats("2013-06-13");
        
        Assert.assertNotNull(date);
        
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(2013, cal.get(Calendar.YEAR));
        Assert.assertEquals(5, cal.get(Calendar.MONTH));
        Assert.assertEquals(13, cal.get(Calendar.DAY_OF_MONTH));
        
        // yyyy/MM/dd date format
        date = parseDateFormats("2013/06/13");
        
        Assert.assertNotNull(date);
        
        cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(2013, cal.get(Calendar.YEAR));
        Assert.assertEquals(5, cal.get(Calendar.MONTH));
        Assert.assertEquals(13, cal.get(Calendar.DAY_OF_MONTH));
    }
    
    @Test
    public void testParseTimeFormats() throws Exception
    {
        // HH:mm:ss date format
        Date date = parseTimeFormats("13:05:59");
        
        Assert.assertNotNull(date);
        
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(13, cal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(5, cal.get(Calendar.MINUTE));
        Assert.assertEquals(59, cal.get(Calendar.SECOND));
        
        // HH:mm:ss.SSS date format
        date = parseTimeFormats("13:05:59.123");
        
        Assert.assertNotNull(date);
        
        cal = Calendar.getInstance();
        cal.setTimeZone(DateUtil.UTC);
        cal.setTime(date);
        
        Assert.assertEquals(13, cal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(5, cal.get(Calendar.MINUTE));
        Assert.assertEquals(59, cal.get(Calendar.SECOND));
        Assert.assertEquals(123, cal.get(Calendar.MILLISECOND));
    }

    @Test
    public void testPartialWCSException() throws Exception
    {
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.setArgumentProperty("utype.axis.axis.ctype", "ctype");
        mapping.setArgumentProperty("utype.axis.range.start.pix", "1.0");
        mapping.setArgumentProperty("utype.axis.range.start.val", "2.0");
        mapping.setArgumentProperty("utype.axis.range.end.pix", "3.0");
        mapping.setArgumentProperty("utype.axis.range.end.val", "4.0");
        mapping.setArgumentProperty("utype.exposure", "1.0");
        mapping.setArgumentProperty("utype.resolution", "2.0");
        
        TemporalWCS time = null;
        try
        {
            time = Time.getTime("utype", mapping);
            Assert.fail("null cunit should've thrown PartialWCSException");
        }
        catch (PartialWCSException e)
        {
            Assert.assertNull(time);
        }
        
        mapping.setArgumentProperty("utype.axis.axis.cunit", "cunit");
        
        time = Time.getTime("utype", mapping);
        
        Assert.assertNotNull(time);
    }
}

