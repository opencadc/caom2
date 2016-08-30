
package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.persistence.skel.ArtifactSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ChunkSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PartSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PlaneSkeleton;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.HexUtil;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public  class Util extends CaomUtil
{
    private static final long serialVersionUID = 201401131515L;
    
    private static Logger log = Logger.getLogger(Util.class);

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
    
    public static UUID getUUID(ResultSet rs, int col)
        throws SQLException
    {
        Object o = rs.getObject(col);
        if (o == null)
            return null;
        if (o instanceof UUID)
            return (UUID) o;
        if (o instanceof Long)
            return new UUID(0L, (Long) o);
        if (o instanceof Number)
            return new UUID(0L, new Long(((Number)o).longValue()));
        if (o instanceof byte[])
        {
            byte[] b = (byte[]) o;
            long msb = HexUtil.toLong(b, 0);
            long lsb = HexUtil.toLong(b, 8);
            return new UUID(msb, lsb);
        }
        throw new UnsupportedOperationException("converting " + o.getClass().getName() + " " + o + " to UUID");
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
    
    public static Plane findPlane(Set<Plane> set, UUID id)
    {
        for (Plane e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    
    public static PlaneSkeleton findPlaneSkel(List<PlaneSkeleton> set, UUID id)
    {
        for (PlaneSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Artifact findArtifact(Set<Artifact> set, UUID id)
    {
        for (Artifact e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    
    public static ArtifactSkeleton findArtifactSkel(List<ArtifactSkeleton> set, UUID id)
    {
        for (ArtifactSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Part findPart(Set<Part> set, UUID id)
    {
        for (Part e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    
    public static PartSkeleton findPartSkel(List<PartSkeleton> set, UUID id)
    {
        for (PartSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }

    public static Chunk findChunk(Set<Chunk> set, UUID id)
    {
        for (Chunk e : set)
            if (e.getID().equals(id))
                return e;
        return null;
    }
    
    public static ChunkSkeleton findChunkSkel(List<ChunkSkeleton> set, UUID id)
    {
        for (ChunkSkeleton e : set)
            if (e.id.equals(id))
                return e;
        return null;
    }
}
