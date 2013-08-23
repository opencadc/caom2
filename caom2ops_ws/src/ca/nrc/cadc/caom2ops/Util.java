
package ca.nrc.cadc.caom2ops;

import java.lang.reflect.Field;
import java.util.Date;

import ca.nrc.cadc.caom2.AbstractCaomEntity;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author yeunga
 */
public class Util
{
    private static Logger log = Logger.getLogger(Util.class);
    
    // methods to assign to private field in AbstractCaomEntity
    public static void assignID(Object ce, Long id)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField("id");
            f.setAccessible(true);
            f.set(ce, id);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }

    public static void assignLastModified(Object ce, Date d, String fieldName)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(ce, d);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }

    public static Date getLastModified(Object ce)
    {
        return getLastModified(ce, "lastModified");
    }
    
    public static Date getLastModified(Object ce, String fieldName)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            return (Date) f.get(ce);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
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

    public static CoordRange1D decodeCoordRange1D(String s)
    {
        //log.warn("decodeCoordRange1D not implemented");
        return null;
    }

    public static CoordBounds1D decodeCoordBounds1D(String s)
    {
        //log.warn("decodeCoordBounds1D not implemented");
        return null;
    }

    // copied from caom2persistence Util.java
    public static CoordFunction1D decodeCoordFunction1D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            Long naxis = Long.parseLong(c[0]);
            Double delta = Double.parseDouble(c[1]);
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            return new CoordFunction1D(naxis, delta, c2);
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }

    public static CoordRange2D decodeCoordRange2D(String s)
    {
        //log.warn("decodeCoordRange2D not implemented");
        return null;
    }

    public static CoordBounds2D decodeCoordBounds2D(String s)
    {
        //log.warn("decodeCoordBounds2D not implemented");
        return null;
    }

    // copied from caom2persistence Util.java
    public static CoordFunction2D decodeCoordFunction2D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            Dimension2D dim = new Dimension2D(Long.parseLong(c[0]), Long.parseLong(c[1]));
            RefCoord c1 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[4]), Double.parseDouble(c[5]));
            Coord2D rc = new Coord2D(c1, c2);
            return new CoordFunction2D(dim, rc,
                    Double.parseDouble(c[6]), Double.parseDouble(c[7]),
                    Double.parseDouble(c[8]), Double.parseDouble(c[9]));
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordFunction2D from " + s, bug);
        }

    }
}
