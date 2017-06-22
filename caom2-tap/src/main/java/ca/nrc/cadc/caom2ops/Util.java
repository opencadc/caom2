
package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.util.CaomUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author yeunga
 */
public class Util extends CaomUtil
{
    public static Object getObject(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        return data.get(col);
    }
    
    public static String getString(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (String) o;
    }
    
    public static URI getURI(List<Object> data, Integer col)
        throws URISyntaxException
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        String s = (String) o;
        return new URI(s);
    }
    
    public static Boolean getBoolean(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        if (o instanceof Integer)
        {
            Integer i = (Integer) o;
            if (i == 1)
                return Boolean.TRUE;
            return Boolean.FALSE;
        }
        return (Boolean) o;
    }
    public static UUID getUUID(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        if (o instanceof UUID)
            return (UUID) o;
        if (o instanceof Long)
        {
            Long lsb = (Long) o;
            return new UUID(0L, lsb);
        }
        throw new UnsupportedOperationException("cannot convert " + o.getClass().getName() + " to UUID");
    }
    public static Long getLong(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (Long) o;
    }
    public static Integer getInteger(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (Integer) o;
    }
    public static Float getFloat(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (Float) o;
    }
    public static Double getDouble(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (Double) o;
    }
    public static List<Double> getDoubleList(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        double[] vals = (double[]) o;
        List<Double> ret=  new ArrayList<Double>(vals.length);
        for (double d : vals)
            ret.add(d);
        return ret;
    }
    public static Date getDate(List<Object> data, Integer col)
    {
        if (col == null)
            return null;
        Object o = data.get(col);
        if (o == null)
            return null;
        return (Date) o;
    }
}
