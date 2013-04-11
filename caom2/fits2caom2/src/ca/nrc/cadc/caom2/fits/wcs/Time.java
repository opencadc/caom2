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
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.date.DateUtil;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class Time
{
    private static Logger log = Logger.getLogger(FitsMapping.class);

    private static final String YYYYMMDD_DASH_FORMAT = "yyyy-MM-dd";
    private static final String YYYYMMDD_SLASH_FORMAT = "yyyy/MM/dd";
    private static final String TIME_SHORT_FORMAT = "HH:mm:ss";
    private static final String TIME_LONG_FORMAT = "HH:mm:ss.SSS";
    
    private static final boolean DESCRIBED = true;
     
    public static TemporalWCS getTime(String utype, FitsMapping mapping)
    {
        if ( FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.time")) )
            return null;
        
        try
        {
            TemporalWCS time;
            CoordAxis1D axis = Wcs.getCoordAxis1D(utype + ".axis", mapping);
            if (axis != null)
            {
                time = new TemporalWCS(axis);
            }
            else
            {
                Double exposureTime = getExposureTime(utype, mapping);

                // MJD keywords
                time = getMJDTime(mapping, exposureTime);

                // EXP keywords
                if (time == null)
                    time = getEXPTime(mapping, exposureTime);

                // DATE keywords
                if (time == null)
                    time = getDATETime(mapping, exposureTime);
            }

            if (time != null)
            {
                time.timesys = Wcs.getStringValue(utype + ".timesys", mapping);
                time.mjdref = Wcs.getDoubleValue(utype + "mjdref", mapping);
                time.trefpos = Wcs.getStringValue(utype+  ".trefpos", mapping);
                time.exposure = Wcs.getDoubleValue(utype + ".exposure", mapping);
                time.resolution = Wcs.getDoubleValue(utype + ".resolution", mapping);
            }

            // If the axis is not descrbed with a bounds, function, or range,
            // set the instance to null.
            if (time != null && DESCRIBED)
            {
                if (time.getAxis().bounds == null &&
                    time.getAxis().function == null &&
                    time.getAxis().range == null)
                    time = null;
            }

            return time;
        }
        catch(IllegalArgumentException ex)
        {
            throw new IllegalArgumentException("failed to create TemporalWCS: " + ex.getMessage(), ex);
        }
    }
    
    protected static TemporalWCS getMJDTime(FitsMapping mapping, Double exposureTime)
    {
        TemporalWCS time = null;
        Double end = null;
        
        Double start = getModifiedJulianDate("MJD-OBS", mapping.getKeywordValue("MJD-OBS"));
        if (start == null)
            start = getModifiedJulianDate("MJDDATE", mapping.getKeywordValue("MJDDATE"));
        if (start != null)
        {
            end = getModifiedJulianDate("MJD-END", mapping.getKeywordValue("MJD-END"));
            if (end == null && exposureTime != null)
            {
                end = start + exposureTime/86400.0;
            }  
        }
        
        if (start != null && end != null)
        {
            time = new TemporalWCS(new CoordAxis1D(new Axis("UTC", "d")));
            time.getAxis().range = new CoordRange1D(new RefCoord(0.5, start), new RefCoord(1.5, end));
        }
        return time;
    }
    
    protected static TemporalWCS getEXPTime(FitsMapping mapping, Double exposureTime)
    {
        TemporalWCS time = null;
        Double end = null;
        
        Double start = getModifiedJulianDate("EXPSTART", mapping.getKeywordValue("EXPSTART"));
        if (start != null)
        {
            end = getModifiedJulianDate("EXPEND", mapping.getKeywordValue("EXPEND"));
            if (end == null && exposureTime != null)
            {
                end = start + exposureTime/86400.0;
            }  
        }
        
        if (start != null && end != null)
        {
            time = new TemporalWCS(new CoordAxis1D(new Axis("UTC", "d")));
            time.getAxis().range = new CoordRange1D(new RefCoord(0.5, start), new RefCoord(1.5, end));
        }
        return time;
    }
    
    protected static TemporalWCS getDATETime(FitsMapping mapping, Double exposureTime)
    {
        TemporalWCS time = null;
        
        String dateObs = mapping.getKeywordValue("DATE-OBS");
        if (dateObs == null)
            return time;
        
        Double start = null;
        Date startDate = parseDateTimeFormats(dateObs);
        if (startDate == null)
        {
            startDate = parseDateFormats(dateObs);
            if (startDate != null)
            {
                String timeObs = mapping.getKeywordValue("TIME-OBS");
                if (timeObs == null)
                    timeObs = mapping.getKeywordValue("UTIME");
                if (timeObs != null)
                {
                    Date startTime = parseTimeFormats(timeObs);
                    if (startTime != null)
                    {
                        // Add startTime to start
                        Calendar dateCal = Calendar.getInstance(DateUtil.UTC);
                        dateCal.setTime(startDate);
                        
                        Calendar timeCal = Calendar.getInstance(DateUtil.UTC);
                        timeCal.setTime(startTime);
                        
                        dateCal.set(Calendar.HOUR_OF_DAY, timeCal.get(Calendar.HOUR_OF_DAY));
                        dateCal.set(Calendar.MINUTE, timeCal.get(Calendar.MINUTE));
                        dateCal.set(Calendar.SECOND, timeCal.get(Calendar.SECOND));
                        dateCal.set(Calendar.MILLISECOND, timeCal.get(Calendar.MILLISECOND));
                        startDate = dateCal.getTime();
                    }
                    else
                    {
                        start = null;
                    }
                }
            }
        }
        if (startDate != null)
            start = DateUtil.toModifiedJulianDate(startDate);
        
        Double end = null;
        if (start != null)
        {
            String dateEnd = mapping.getKeywordValue("DATE-END");
            if (dateEnd != null)
            {
                Date endDate = parseDateTimeFormats(dateEnd);
                if (endDate != null)
                {
                    end = DateUtil.toModifiedJulianDate(endDate);
                }
            }
            if (end == null && exposureTime != null)
            {
                end = start + exposureTime/86400.0;
            }
        }
        
        if (start != null && end != null)
        {
            time = new TemporalWCS(new CoordAxis1D(new Axis("UTC", "d")));
            time.getAxis().range = new CoordRange1D(new RefCoord(0.5, start), new RefCoord(1.5, end));
        }        
        return time;
    }
    
    protected static Double getExposureTime(String utype, FitsMapping mapping)
    {
        Double exposureTime = Wcs.getDoubleValue(utype + ".exposure", mapping);
        if (exposureTime == null)
        {
            String value = mapping.getKeywordValue("EXPTIME");
            if (value == null)
                value = mapping.getKeywordValue("INTTIME");
            if (value != null)
            {
                try
                {
                    exposureTime = Double.valueOf(value);
                }
                catch (NumberFormatException e)
                {
                    log.debug("Uable to parse EXPTIME '" + value + " to double.");
                }
            }
        }
        return exposureTime;
    }
    
    protected static Double getModifiedJulianDate(String keyword, String value)
    {
        if (value == null)
            return null;
        
        Double mjd = null;
        try
        {
            mjd = Double.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            log.debug("Unable to parse " + keyword + " '" + value + "' to double.");
        }
        
        if (mjd != null)
        {        
            // A JD?
            if (mjd >= 2400000.5)
                mjd = mjd - 2400000.5;
            
            // 2023-2-25 = 60,000MJD
            if (mjd < 0 || mjd > 60000)
                mjd = null;
        }
        return mjd;
    }
    
    protected static Date parseDateTimeFormats(String s)
    {
        DateFormat format = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        Date date = null;
        try
        {
            date = format.parse(s);
        }
        catch (ParseException e) {}
        
        if (date == null)
        {
            try
            {
                format = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
                date = format.parse(s);
            }
            catch (ParseException e) {}
        }
        
        if (date == null)
        {
            try
            {
                format = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT_TZ, DateUtil.UTC);
                date = format.parse(s);
            }
            catch (ParseException e) {}
        }
        return date;
    }
    
    protected static Date parseDateFormats(String s)
    {
        DateFormat format = DateUtil.getDateFormat(YYYYMMDD_DASH_FORMAT, DateUtil.UTC);
        Date date = null;
        try
        {
            date = format.parse(s);
        }
        catch (ParseException e) {}
        
        if (date == null)
        {
            format = DateUtil.getDateFormat(YYYYMMDD_SLASH_FORMAT, DateUtil.UTC);
            try
            {
                date = format.parse(s);
            }
        catch (ParseException e) {}
        }
        return date;
    }
    
    protected static Date parseTimeFormats(String s)
    {
        DateFormat format = DateUtil.getDateFormat(TIME_LONG_FORMAT, DateUtil.UTC);
        Date date = null;
        try
        {
            date = format.parse(s);
        }
        catch (ParseException e) {}
        
        if (date == null)
        {
            format = DateUtil.getDateFormat(TIME_SHORT_FORMAT, DateUtil.UTC);
            try
            {
                date = format.parse(s);
            }
            catch (ParseException e) {}
        }
        return date;
    }
    
}
