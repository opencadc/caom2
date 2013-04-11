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

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.fits.exceptions.CastException;
import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Logger;

/**
 * Cast a String value to a specified Class.
 * 
 * @author jburke
 */
public abstract class Cast
{
    private static Logger log = Logger.getLogger(Cast.class);
    
    // Date format used by BLAST.
    private static final String BLAST_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    
    /**
     * Cast the specified value to the specified Class.
     * 
     * @param value the value to cast.
     * @param c the Class to cast to.
     * @param utype utype of the value.
     * @return an instance of c with the given value.
     * @throws CastException if the value cannot be cast to the given Class.
     */
    public static Object cast(String value, Class c, String utype)
        throws CastException
    {
        if (value != null && value.isEmpty())
            throw new CastException("Null or empty value parameter");
        if (c == null)
            throw new CastException("Null Class parameter");
        
        Object castValue = null;
        if (value == null)
        {
            // HACK: return the object equivalent to default values for primitives
            // so that auto-unboxing will assign it as usual
            if (c == boolean.class)
            {
                castValue = Boolean.FALSE;
            }
            else if (c == double.class)
            {
                castValue = new Double(0.0);
            }
            else if (c == float.class)
            {
                castValue = new Float(0.0f);
            }
            else if (c == int.class)
            {
                castValue = new Integer(0);
            }
            else if (c == long.class)
            {
                castValue = new Long(0L);
            }
        }
        else if (c == boolean.class || c == Boolean.class)
        {
            castValue = toBoolean(value);
        }
        else if (c == double.class || c == Double.class)
        {
            castValue = toDouble(value);
        }
        else if (c == float.class || c == Float.class)
        {
            castValue = toFloat(value);
        }
        else if (c == int.class || c == Integer.class)
        {
            castValue = toInteger(value);
        }
        else if (c == long.class || c == Long.class)
        {
            castValue = toLong(value);
        }
        else if (c == String.class)
        {
            castValue = toString(value);
        }
        else if (c == Date.class)
        {
            castValue = toDate(value);
        }
        else if (c == int[].class)
        {
            castValue = toIntArray(value);
        }
        else if (List.class.isAssignableFrom(c))
        {
            castValue = toList(value, utype);
        }
        else if (Set.class.isAssignableFrom(c))
        {
            castValue = toSet(value, utype);
        }
        else if (c == URI.class)
        {
            castValue = toURI(value);
        }
        else
        {
            throw new CastException("Unsupported Class type " + c.getName());
        }
        
//        log.debug("cast " + value + " to " + c.getName());
        return castValue;
    }
    
    protected static Boolean toBoolean(String value)
        throws CastException
    {   
        return Boolean.valueOf(value);
    }
    
    protected static Double toDouble(String value)
        throws CastException
    {
        try
        {
            return Double.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new CastException(value + " cannot be cast to a Double: " + e.getMessage(), e);
        }
    }
    
    protected static Float toFloat(String value)
        throws CastException
    {   
        try
        {
            return Float.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new CastException(value + " cannot be cast to a Float: " + e.getMessage(), e);
        }
    }
    
    protected static Integer toInteger(String value)
        throws CastException
    {
        try
        {
            return Integer.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new CastException(value + " cannot be cast to a Integer: " + e.getMessage(), e);
        }
    }
    
    protected static Long toLong(String value)
        throws CastException
    { 
        try
        {
            return Long.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new CastException(value + " cannot be cast to a Long: " + e.getMessage(), e);
        }
    }
    
    protected static String toString(String value)
        throws CastException
    {
        return value;
    }
    
    protected static Date toDate(String value)
        throws CastException
    {        
        // Try ISO date format without time zone.
        try
        {
            return DateUtil.flexToDate(value, DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC));
        }
        catch (ParseException pe)
        {
            try
            {
                // Try BLAST format.
                return DateUtil.flexToDate(value, DateUtil.getDateFormat(BLAST_DATE_FORMAT, DateUtil.UTC));
            }
            catch (ParseException pe2)
            {
                throw new CastException(value + " cannot be cast to a Date: " + pe2.getMessage(), pe2);
            }
        }
    }
    
    // Comma delimited String is encoded as ints separated by ';'s.
    // Added each int to an int[] and replace the ';' with min Integer value.
    protected static int[] toIntArray(String value)
        throws CastException
    {        
        try
        {
            int index = 0;
            String s = "";
            char[] chars = value.toCharArray();
            int[] array = new int[chars.length];
            for (int i = 0; i < chars.length; i++)
            {
                char c = chars[i];
                if (c == ',')
                {
                    if (s.length() > 0)
                    {
                        array[index++] = Integer.parseInt(s.trim());
                        s = "";
                    }
                }
                else if (c == ';')
                {
                    if (s.length() > 0)
                    {
                        array[index++] = Integer.parseInt(s.trim());
                        s = "";
                    }
                    array[index++] = Integer.MIN_VALUE;
                }
                else
                {
                    s += c;
                }
            }
            if (s.length() > 0)
                array[index++] = Integer.parseInt(s.trim());

            int[] result = new int[index];
            System.arraycopy(array, 0, result, 0, index);
            return result;
        }
        catch (Exception e)
        {
            throw new CastException(value + " cannot be cast to an int[]: " + e.getMessage(), e);
        }
    }
    
    protected static List toList(String value, String utype)
    {           
        // By default split on an comma.
        String delimiter = ",";
        if (utype != null && utype.endsWith(".keywords"))
        {
            // Keywords are split by a space character.
            delimiter = "[\\s]+";    
        }
        String[] keywords = value.split(delimiter);
        return Arrays.asList(keywords);
    }
    
    protected static Set toSet(String value, String utype)
        throws CastException
    {
        if (utype == null || utype.isEmpty())
            throw new CastException("Cast to a Set requires that the utype not be null or empty");
        
        // ObservationURI
        if (utype.endsWith("members"))
        {
            try
            {
                Set<ObservationURI> set = new TreeSet<ObservationURI>();
                String[] values = value.split(",");
                for (int i = 0; i < values.length; i++)
                {
                    String val = values[i];
                    if (val != null && !val.isEmpty())
                        set.add(new ObservationURI(new URI(val.trim())));
                }
                return set;
            }
            catch (URISyntaxException e)
            {
                throw new CastException(value + " cannot be cast to a ObservationURI: " + e.getMessage(), e);
            }
        }

        //PlaneURI
        else if (utype.endsWith("inputs"))
        {
            try
            {                
                Set<PlaneURI> set = new TreeSet<PlaneURI>();
                String[] values = value.split(",");
                for (int i = 0; i < values.length; i++)
                {
                    String val = values[i];
                    if (val != null && !val.isEmpty())
                        set.add(new PlaneURI(new URI(val.trim())));
                }
                return set;
            }
            catch (URISyntaxException e)
            {
                throw new CastException(value + " cannot be cast to a PlaneURI: " + e.getMessage(), e);
            }
        }
        else
        {
            throw new CastException("Cast of utype " + utype + " to a Set is not supported");
        }
    }
    
    protected static URI toURI(String value)
        throws CastException
    {
        try
        {
            return new URI(value);
        }
        catch (URISyntaxException e)
        {
            throw new CastException(value + " cannot be cast to a URI: " + e.getMessage(), e);
        }
    }
    
}
