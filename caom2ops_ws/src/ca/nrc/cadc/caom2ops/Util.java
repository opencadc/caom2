
package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.util.CaomUtil;
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
        Long lsb = (Long) o;
        // backwards compatibility of Long ID values
        return new UUID(0L, lsb);
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
