
package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.AbstractCaomEntity;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.persistence.skel.ArtifactSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ChunkSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PartSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PlaneSkeleton;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.date.DateUtil;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class Util
{
    private static Logger log = Logger.getLogger(Util.class);

    static final String STRING_SET_SEPARATOR = " ";
    static final String POL_STATE_SEPARATOR = "/"; // IVOA ObsCore-1.0 Data Model, B.6.6

    public static String formatSQL(String[] sql)
    {
        StringBuilder sb = new StringBuilder();
        for (String s : sql)
        {
            sb.append("\n");
            sb.append(formatSQL(s));
        }
        return sb.toString();
    }
    
    public static String formatSQL(String sql)
    {
        sql = sql.replaceAll("SELECT ", "\nSELECT ");
        sql = sql.replaceAll("FROM ",   "\nFROM ");
        sql = sql.replaceAll("LEFT ",   "\n  LEFT ");
        sql = sql.replaceAll("RIGHT ",  "\n  RIGHT ");
        sql = sql.replaceAll("WHERE ",  "\nWHERE ");
        sql = sql.replaceAll("AND ",    "\n  AND ");
        sql = sql.replaceAll("OR ",     "\n  OR ");
        sql = sql.replaceAll("ORDER",  "\nORDER");
        sql = sql.replaceAll("GROUP ",  "\nGROUP ");
        sql = sql.replaceAll("HAVING ", "\nHAVING ");
        sql = sql.replaceAll("UNION ",  "\nUNION ");

        // note: \\s* matches one or more whitespace chars
        //sql = sql.replaceAll("OUTER JOIN", "\n  OUTER JOIN");
        return sql;
    }
    
    public static String escapeChar(String s, char p)
    {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<s.length(); i++)
        {
            char c = s.charAt(i);
            if (c == p )
                sb.append(c); // an extra one
            sb.append(c);
        }
        return sb.toString();
    }
    
    public static String replaceAll(String s, char p, char r)
    {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<s.length(); i++)
        {
            char c = s.charAt(i);
            if (c == p )
                sb.append(r);
            else
                sb.append(c);
        }
        return sb.toString();
    }

    // IVOA ObsCore-1.0 Data Model, B.6.6
    public static String encodeStates(List<PolarizationState> states)
    {
        if (states == null || states.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<PolarizationState> i = states.iterator();
        sb.append(POL_STATE_SEPARATOR); // leading
        while ( i.hasNext() )
        {
            PolarizationState s = i.next();
            sb.append(s.stringValue());
            sb.append(POL_STATE_SEPARATOR); // trailing
        }
        return sb.toString();
    }
    
    public static String encodeListString(List<String> strs)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<String> i = strs.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodeListString(String val, List<String> out)
    {
        if (val == null)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
            out.add(s);
    }

    public static String encodeObservationURIs(Set<ObservationURI> set)
    {
        if (set.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<ObservationURI> i = set.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next().getURI().toASCIIString());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodeObservationURIs(String val, Set<ObservationURI> out)
        throws SQLException
    {
        if (val == null)
            return;
        val = val.trim();
        if (val.length() == 0)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
        {
            if (s.length() > 0)
                try
                {
                    URI uri = new URI(s);
                    ObservationURI puri = new ObservationURI(uri);
                    out.add(puri);
                }
                catch(URISyntaxException ex)
                {
                    throw new RuntimeException("failed to decode URI: " + s, ex);
                }
        }
    }
    public static String encodePlaneURIs(Set<PlaneURI> set)
    {
        if (set.isEmpty())
           return null;
        StringBuilder sb = new StringBuilder();
        Iterator<PlaneURI> i = set.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next().getURI().toASCIIString());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodePlaneURIs(String val, Set<PlaneURI> out)
        throws SQLException
    {
        if (val == null)
            return;
        val = val.trim();
        if (val.length() == 0)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
        {
            try
            {
                URI uri = new URI(s);
                PlaneURI puri = new PlaneURI(uri);
                out.add(puri);
            }
            catch(URISyntaxException ex)
            {
                throw new RuntimeException("failed to decode URI: " + s, ex);
            }
        }
    }

    public static String encodeCoordRange1D(CoordRange1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getStart().pix);
        sb.append("/");
        sb.append(cr.getStart().val);
        sb.append("/");
        sb.append(cr.getEnd().pix);
        sb.append("/");
        sb.append(cr.getEnd().val);
        return sb.toString();
    }
    public static CoordRange1D decodeCoordRange1D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            RefCoord c1 = new RefCoord(Double.parseDouble(c[0]), Double.parseDouble(c[1]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            return new CoordRange1D(c1, c2);
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }
    public static String encodeCoordBounds1D(CoordBounds1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<CoordRange1D> i = cr.getSamples().iterator();
        while ( i.hasNext() )
        {
            String s = encodeCoordRange1D(i.next());
            sb.append(s);
            if ( i.hasNext() )
                sb.append(",");
        }
        return sb.toString();
    }
    public static CoordBounds1D decodeCoordBounds1D(String s)
    {
        if (s == null)
            return null;
        s = s.trim();
        
        CoordBounds1D ret = new CoordBounds1D();
        if (s.length() == 0)
            return ret; // empty sample list
        
        String[] c = s.split(",");
        for (String r : c)
        {
            CoordRange1D cr = decodeCoordRange1D(r);
            ret.getSamples().add(cr);
        }
        return ret;
    }
    public static String encodeCoordFunction1D(CoordFunction1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getNaxis());
        sb.append("/");
        sb.append(cr.getDelta());
        sb.append("/");
        sb.append(cr.getRefCoord().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().val);
        sb.append("/");
        return sb.toString();
    }
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

    public static String encodeCoordRange2D(CoordRange2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getStart().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getStart().getCoord1().val);
        sb.append("/");
        sb.append(cr.getStart().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getStart().getCoord2().val);
        sb.append("/");
        sb.append(cr.getEnd().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getEnd().getCoord1().val);
        sb.append("/");
        sb.append(cr.getEnd().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getEnd().getCoord2().val);
        return sb.toString();
    }
    public static CoordRange2D decodeCoordRange2D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            RefCoord c1 = new RefCoord(Double.parseDouble(c[0]), Double.parseDouble(c[1]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            RefCoord c3 = new RefCoord(Double.parseDouble(c[4]), Double.parseDouble(c[5]));
            RefCoord c4 = new RefCoord(Double.parseDouble(c[6]), Double.parseDouble(c[7]));
            return new CoordRange2D(new Coord2D(c1, c2), new Coord2D(c3, c4));
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }
    public static String encodeCoordBounds2D(CoordBounds2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        if (cr instanceof CoordCircle2D)
        {
            CoordCircle2D circ = (CoordCircle2D) cr;
            sb.append("C/");
            sb.append(circ.getCenter().getCoord1().pix);
            sb.append("/");
            sb.append(circ.getCenter().getCoord1().val);
            sb.append("/");
            sb.append(circ.getCenter().getCoord2().pix);
            sb.append("/");
            sb.append(circ.getCenter().getCoord2().val);
            sb.append("/");
            sb.append(circ.getRadius());
        }
        else
        {
            CoordPolygon2D poly = (CoordPolygon2D) cr;
            sb.append("P/");
            Iterator<Coord2D> i = poly.getVertices().iterator();
            while ( i.hasNext() )
            {
                Coord2D c = i.next();
                sb.append(c.getCoord1().pix);
                sb.append(",");
                sb.append(c.getCoord1().val);
                sb.append(",");
                sb.append(c.getCoord2().pix);
                sb.append(",");
                sb.append(c.getCoord2().val);
                if (i.hasNext())
                    sb.append("/");
            }
        }
        return sb.toString();
    }
    public static CoordBounds2D decodeCoordBounds2D(String s)
    {
        if (s == null)
            return null;
        s = s.trim();

        String[] c = s.split("/");

        if ("C".equals(c[0]))
        {
            try
            {
                RefCoord c1 = new RefCoord(Double.parseDouble(c[1]), Double.parseDouble(c[2]));
                RefCoord c2 = new RefCoord(Double.parseDouble(c[3]), Double.parseDouble(c[4]));
                Double rad = Double.parseDouble(c[5]);
                return new CoordCircle2D(new Coord2D(c1, c2), rad);
            }
            catch(NumberFormatException bug)
            {
                throw new RuntimeException("BUG: failed to decode CoordCircle2D from " + s, bug);
            }
        }
        if ("P".equals(c[0]))
        {
            CoordPolygon2D poly = new CoordPolygon2D();
            for (int i=1; i<c.length; i++)
            {
                String[] cc = c[i].split(",");
                try
                {
                    RefCoord c1 = new RefCoord(Double.parseDouble(cc[0]), Double.parseDouble(cc[1]));
                    RefCoord c2 = new RefCoord(Double.parseDouble(cc[2]), Double.parseDouble(cc[3]));
                    poly.getVertices().add(new Coord2D(c1, c2));
                }
                catch(Exception bug)
                {
                    throw new RuntimeException("BUG: failed to decode CoordPolygon2D from " + s, bug);
                }
            }
            return poly;
        }
        throw new RuntimeException("BUG: failed to decode CoordBounds2D from " + s);
    }
    public static String encodeCoordFunction2D(CoordFunction2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getDimension().naxis1);
        sb.append("/");
        sb.append(cr.getDimension().naxis2);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord1().val);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord2().val);
        sb.append("/");
        sb.append(cr.getCd11());
        sb.append("/");
        sb.append(cr.getCd12());
        sb.append("/");
        sb.append(cr.getCd21());
        sb.append("/");
        sb.append(cr.getCd22());

        return sb.toString();
    }
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

    public static URI getURI(ResultSet rs, int col)
        throws SQLException
    {
        String o = rs.getString(col);
        if (o == null)
            return null;
        try
        {
            return new URI((String)o);
        }
        catch(Throwable t)
        {
            throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to URI", t);
        }
        //throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to URI");
    }        
    
    public static Object getObject(ResultSet rs, int col)
        throws SQLException
    {
        Object obj = rs.getObject(col);
        if (obj == null)
            return null;
        if (obj instanceof Date)
            return getDate(rs, col, null);
        return obj;
    }

    public static Boolean getBoolean(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;

        // try boolean
        if (o instanceof Boolean)
        {
            return (Boolean) o;
        }

        // try integer
        Integer i = null;
        if (o instanceof Integer)
        {
            i = (Integer) o;

        }
        else if(o instanceof Number)
        {
            i = new Integer(((Number)o).intValue());
        }
        if (i != null)
        {
            if (i.intValue() == 0)
                return Boolean.FALSE;
            if (i.intValue() == 1)
                return Boolean.TRUE;
        }

        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Boolean");
    }

    public static Integer getInteger(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof Integer)
            return (Integer) o;
        if (o instanceof Number)
            return new Integer(((Number)o).intValue());
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Integer");
    }

    public static Long getLong(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof Long)
            return (Long) o;
        if (o instanceof Number)
            return new Long(((Number)o).longValue());
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Long");
    }
    
    public static Float getFloat(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof Float)
            return (Float) o;
        if (o instanceof Number)
            return new Float(((Number)o).floatValue());
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Float");
    }
    
    public static Double getDouble(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof Double)
            return (Double) o;
        if (o instanceof Number)
            return new Double(((Number)o).doubleValue());
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Double");
    }

    // truncate to even number of seconds
    public static Date truncate(Date d)
    {
        if (d == null)
            return null;
        return new Date(1000L * (d.getTime() / 1000L));
    }
    // round to nearest second
    public static Date getRoundedDate(ResultSet rs, int col, Calendar cal)
        throws SQLException
    {
        Date ret = getDate(rs, col, cal);
        if (ret == null)
            return null;
        double t = (double) ret.getTime();
        t /= 1000.0;
        ret = new Date(1000L * Math.round(t));
        return ret;
    }
    public static Date getDate(ResultSet rs, int col, Calendar cal)
        throws SQLException
    {
        Object o = rs.getTimestamp(col, cal);
        return DateUtil.toDate(o);
    }
    
    public static byte[] getByteArray(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof byte[])
            return (byte[]) o;
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to byte[]");
    }

    /*
    public static int[] getIntArray(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        return toIntArray(o);
    }
    
    static int[] toIntArray(Object o)
        throws SQLException
    {
        if (o == null)
            return null;
        if (o instanceof Array)
        {
            Array a = (Array) o;
            o = a.getArray();
        }
        if (o instanceof int[])
            return (int[]) o;
        if (o instanceof byte[])
            return CaomUtil.decodeIntArray((byte[]) o);
        if (o instanceof Integer[])
        {
            Integer[] ia = (Integer[]) o;
            int[] ret = new int[ia.length];
            for (int i=0; i<ia.length; i++)
                ret[i] = ia[i].intValue();
            return ret;
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to int[]");
    }
    */
    
    public static Shape getShape(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof byte[])
        {
            byte[] b = (byte[]) o;
            //return GeomUtil.decode(b);
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Shape");
    }
    
    // note: only bounds, not samples
    public static Interval getInterval(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        log.debug("getInterval: col=" + col + ", value=" + o);
        if (o == null)
            return null;
       
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to Interval");
    }
    
    // the interval subsamples
    public static List getSubIntervals(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof byte[])
            //return CaomUtil.decodeSubIntervals((byte[]) o, 0);
            throw new UnsupportedOperationException();
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to List of SubInterval");
    }
    
    // the wcs lookup samples
    public static List getSamples(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof byte[])
            //return CaomUtil.decodeSamples((byte[]) o, 0);
            throw new UnsupportedOperationException();
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to List of Sample");
    }
    
    public static void sqlLog(String[] sql, boolean format)
    {
        if (sql == null)
            return;
        if (format)
        {
            for (int i=0; i<sql.length; i++)
                log.debug(Util.formatSQL(sql[i]));
        }
        else
        {
            for (int i=0; i<sql.length; i++)
                log.debug(sql[i]);
        }
    }
    
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

    /*
    public static void assignStateCode(Object ce, Integer sc)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField("stateCode");
            f.setAccessible(true);
            f.set(ce, sc);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }
    */
    
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

    public static Plane findPlane(Set<Plane> set, Long id)
    {
        for (Plane e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    public static PlaneSkeleton findPlaneSkel(List<PlaneSkeleton> set, Long id)
    {
        for (PlaneSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Artifact findArtifact(Set<Artifact> set, Long id)
    {
        for (Artifact e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    public static ArtifactSkeleton findArtifactSkel(List<ArtifactSkeleton> set, Long id)
    {
        for (ArtifactSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Part findPart(Set<Part> set, Long id)
    {
        for (Part e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    public static PartSkeleton findPartSkel(List<PartSkeleton> set, Long id)
    {
        for (PartSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Chunk findChunk(Set<Chunk> set, Long id)
    {
        for (Chunk e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    public static ChunkSkeleton findChunkSkel(List<ChunkSkeleton> set, Long id)
    {
        for (ChunkSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }
}
