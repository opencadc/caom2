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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.types.Vertex;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class PostgreSQLGenerator extends BaseSQLGenerator
{
    private static final Logger log = Logger.getLogger(PostgreSQLGenerator.class);
    
    public PostgreSQLGenerator(String database, String schema)
    {
        super(database, schema);
        this.useIntegerForBoolean = true;
        this.persistComputed = true;
        this.persistOptimisations = true;
        this.persistReadAccessWithAsset = true;
        this.useLongForUUID = false;
        super.init();
    }

    @Override
    protected String getLimitConstraint(Integer batchSize)
    {
        if (batchSize == null)
            return null;
        return "LIMIT " + batchSize;
    }
    
    @Override
    protected String getUpdateAssetSQL(Class asset, Class ra, boolean add)
    {
        StringBuilder sb = new StringBuilder();
        String col = getReadAccessCol(ra);
        
            
        sb.append("UPDATE ");
        sb.append(getTable(asset));
        sb.append(" SET ").append(col).append(" = ");
        if (add)
        {
            sb.append("(").append(col).append(" || ?::tsvector)");
        }
        else // remove
        {
            sb.append("regexp_replace(");
            sb.append("regexp_replace(").append(col).append("::text, ?, '')"); // remove group name
            sb.append(", $$''$$, '', 'g')::tsvector"); // remove all tick marks before cast
        }
        sb.append(" WHERE ");
        if (PlaneMetaReadAccess.class.equals(ra) && !Plane.class.equals(asset))
            sb.append(getPrimaryKeyColumn(Plane.class)); // HACK: only works because column name is the same in all tables
        else      
            sb.append(getPrimaryKeyColumn(asset));
        sb.append(" = ?");

        return sb.toString();
    }
    
    @Override
    protected String literal(UUID value)
    {
        // uuid datatype accepts a string with the standard hex string format
        return "'" + value.toString() + "'";
    }

    @Override
    protected void safeSetPoint(StringBuilder sb, PreparedStatement ps, int col, Point val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPoint] in: " + val);
            StringBuilder sval = new StringBuilder();
            sval.append("(");
            sval.append(val.cval1);
            sval.append("d,");
            sval.append(val.cval2);
            sval.append("d)");
            PGobject pgo = new PGobject();
            String spoint = sval.toString();
            pgo.setType("spoint");
            pgo.setValue(spoint);
            ps.setObject(col, pgo);
            if (sb != null)
            {
                sb.append(spoint);
                sb.append(",");
            }
        }
    }

    @Override
    protected void safeSetPointList(StringBuilder sb, PreparedStatement ps, int col, List<Point> val) 
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPolygon] in: " + val);
            Double[] dval = new Double[2 * val.size()]; // 2 numbers per point
            int i = 0;
            for (Point p : val)
            {
                dval[i++] = p.cval1;
                dval[i++] = p.cval2;
            }
            java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
            ps.setObject(col, arr);
            if (sb != null)
            {
                sb.append("[");
                for (double d : dval)
                    sb.append(d).append(",");
                sb.setCharAt(sb.length()-1, ']'); // replace last comma with closing ]
            }
        }
    }
    
    @Override
    protected void safeSetPolygon(StringBuilder sb, PreparedStatement ps, int col, Polygon val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPolygon] in: " + val);
            // pg_sphere only supports simple polygons
            Polygon poly = val;
            log.debug("[safeSetPolygon] hull: " + poly);
            if (poly == null)
            {
                ps.setObject(col, null);
                log.debug("failed to compute simple outer hull from " + val);
                if (sb != null)
                    sb.append("null,");
            }
            else
            {
                StringBuilder sval = new StringBuilder();
                sval.append("{");
                for (Point p : poly.getPoints())
                {
                    sval.append("(");
                    sval.append(Math.toRadians(p.cval1));
                    sval.append(",");
                    sval.append(Math.toRadians(p.cval2));
                    sval.append(")");
                    sval.append(",");
                }
                sval.setCharAt(sval.length()-1, '}'); // replace last comma with closing }
                String spoly = sval.toString();
                log.debug("[in] spoly in radians: " + spoly);
                PGobject pgo = new PGobject();
                pgo.setType("spoly");
                pgo.setValue(spoly);
                ps.setObject(col, pgo);
                if (sb != null)
                {
                    sb.append(spoly);
                    sb.append(",");
                }
            }
        }
    }

    @Override
    protected void safeSetMultiPolygon(StringBuilder sb, PreparedStatement ps, int col, MultiPolygon val) 
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPolygon] in: " + val);
            Double[] dval = new Double[3 * val.getVertices().size()]; // 3 numbers per vertex
            int i = 0;
            for (Vertex v : val.getVertices())
            {
                dval[i++] = v.cval1;
                dval[i++] = v.cval2;
                dval[i++] = v.getType().getValue().doubleValue();
            }
            java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
            ps.setObject(col, arr);
            if (sb != null)
            {
                sb.append("[");
                for (double d : dval)
                    sb.append(d).append(",");
                sb.setCharAt(sb.length()-1, ']'); // replace last comma with closing ]
            }
        }
    }
    
    
    @Override
    protected void safeSetInterval(StringBuilder sb, PreparedStatement ps, int col, Interval val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetInterval] in: " + val);
            PGpolygon poly = generatePolygon2D(val, null);
            ps.setObject(col, poly);
            if (sb != null)
            {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }

    @Override
    protected void safeSetSubIntervalList(StringBuilder sb, PreparedStatement ps, int col, List<SubInterval> subs)
        throws SQLException
    {
        if (subs == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetInterval] in: " + subs.size() + " SubIntervals");
            PGpolygon poly = generatePolygon2D(null, subs);
            ps.setObject(col, poly);
            if (sb != null)
            {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }

    // working impl but cannot exactly round-trip through spoly column due to degrees to
    // radians, precision loss in db, and radians to degrees
    protected List<Point> getPointListFromSPolyString(ResultSet rs, int col) throws SQLException
    {
        // spoly string format: {(a,b),(c,d),(e,f) ... }
        String s = rs.getString(col);
        if (s == null)
            return null;

        // Get the string inside the enclosing brackets.
        int open = s.indexOf("{");
        int close = s.indexOf("}");
        if (open == -1 || close == -1)
            throw new IllegalArgumentException("Missing opening or closing brackets " + s);

        // Get the string inside the enclosing parentheses.
        s = s.substring(open + 1, close);
        open = s.indexOf("(");
        close = s.lastIndexOf(")");
        if (open == -1 || close == -1)
            throw new IllegalArgumentException("Missing opening or closing parentheses " + s);

        // Each set of vertices is '),(' separated.
        s = s.substring(open + 1, close);
        String[] vertices = s.split("\\){1}?\\s*,\\s*{1}\\({1}?");

        // Check minimum vertices to make a polygon.
        if (vertices.length < 3)
            throw new IllegalArgumentException("Minimum 3 vertices required to form a Polygon " + s);

        // Create STC Polygon and set some defaults.
        List<Point> ret = new ArrayList<Point>(vertices.length);

        // Loop through each set of vertices.
        for (int i = 0; i < vertices.length; i++)
        {
            // Each vertex is 2 values separated by a comma.
            String vertex = vertices[i];
            String[] values = vertex.split(",");
            if (values.length != 2)
                throw new IllegalArgumentException("Each set of vertices must have only 2 values " + vertex);

            // Coordinates.
            double x = Double.parseDouble(values[0]);
            double y = Double.parseDouble(values[1]);

            log.debug("[out] spoly in radians: " + x + "," + y);
            // convert to degrees and add to Polygon.
            x = Math.toDegrees(x);
            y = Math.toDegrees(y);
            ret.add(new Point(x, y));
        }
        return ret;
    }

    @Override
    protected List<Point> getPointList(ResultSet rs, int col) throws SQLException
    {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null)
            return null;
        
        List<Point> ret = new ArrayList<Point>();
        for (int i=0; i<coords.length; i+=2)
        {
            double cval1 = coords[i];
            double cval2 = coords[i+1];
            
            Point p = new Point(cval1, cval2);
            ret.add(p);
        }
        return ret;
    }

    
    @Override
    protected MultiPolygon getMultiPolygon(ResultSet rs, int col) throws SQLException
    {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null)
            return null;
        
        MultiPolygon ret = new MultiPolygon();
        for (int i=0; i<coords.length; i+=3)
        {
            double cval1 = coords[i];
            double cval2 = coords[i+1];
            int s = (int) coords[i+2];
            SegmentType t = SegmentType.toValue(s);
            Vertex v = new Vertex(cval1, cval2, t);
            ret.getVertices().add(v);
        }
        ret.validate();
        return ret;
    }

    @Override
    protected Interval getInterval(ResultSet rs, int col) throws SQLException
    {
        String s = rs.getString(col);
        if (s == null)
            return null;
        
        List<Double> vals = parsePolygon2D(s);
        if (vals.size() == 2)
            return new Interval(vals.get(0), vals.get(1));
        throw new RuntimeException("BUG: found " + vals.size() + " values for Interval bounds");
    }
    
    @Override
    protected List<SubInterval> getSubIntervalList(ResultSet rs, int col) throws SQLException
    {
        String s = rs.getString(col);
        if (s == null)
            return null;
        
        List<SubInterval> ret = new ArrayList<SubInterval>();
        List<Double> vals = parsePolygon2D(s);
        for (int i=0; i<vals.size(); i+=2)
        {
            ret.add(new SubInterval(vals.get(i), vals.get(i+1)));
        }
        return ret;
    }

    
    PGpolygon generatePolygon2D(Interval val, List<SubInterval> subs)
    {
        // a query will be a point or line segment at y=0
        double y1 = -2.0;  // bottom of comb
        double ym = -1.0;  // bottom of teeth
        double y2 = 1.0;   // top of teeth
        
        List<PGpoint> verts = new ArrayList<PGpoint>();
        
        // draw a 2D polygon that looks like a tooth-up-comb with each tooth having x-range that
        // corresponds to one (sub) interval... it is a simple box for an Interval with no sub-samples
        
        if (subs != null)
        {
            // full-span line at y1
            double lb = subs.get(0).getLower();
            double ub = subs.get(subs.size() - 1).getUpper();
            verts.add(new PGpoint(lb, y1));
            verts.add(new PGpoint(ub, y1));
            
            LinkedList<SubInterval> samples = new LinkedList<SubInterval>(subs);
            Iterator<SubInterval> iter = samples.descendingIterator();
            SubInterval prev = null;
            while ( iter.hasNext() )
            {
                SubInterval si = iter.next();
                if (prev != null)
                {
                    verts.add(new PGpoint(prev.getLower(), ym));
                    verts.add(new PGpoint(si.getUpper(), ym));
                }
                verts.add(new PGpoint(si.getUpper(), y2));
                verts.add(new PGpoint(si.getLower(), y2));
                prev = si;
            }
        }
        else
        {
            // full-span line at y1
            verts.add(new PGpoint(val.getLower(), y1));
            verts.add(new PGpoint(val.getUpper(), y1));
        
            // just the basic bounds interval
            verts.add(new PGpoint(val.getUpper(), y2));
            verts.add(new PGpoint(val.getLower(), y2));
        }
        
        return new PGpolygon(verts.toArray(new PGpoint[verts.size()]));
    }
    
    // this parses a postgresql polygon internal representation of a caom2
    // Interval; the generating code is in the caom2persistence where the basic 
    // concept is that the intwerval is a comb-shape and the teeth that stick up
    // above y=0.0 represent the subintervals... a simple interval is just a box;
    // to reconstruct, simply find all the X values for y > 0.0 and sort them
    private List<Double> parsePolygon2D(String s)
    {
        
        s = s.replaceAll("[()]", ""); // remove all ( )
        //log.debug("strip: '" + s + "'");
        String[] points = s.split(",");
        List<Double> vals = new ArrayList<Double>();
        for (int i=0; i<points.length; i+=2)
        {
            String xs = points[i];
            String ys = points[i+1];
            //log.debug("check: " + xs + "," + ys);
            double y = Double.parseDouble(ys);
            if (y > 0.0)
            {
                vals.add(new Double(xs));
            }
        }
        // sort so we don't care about winding direction of the polygon impl
        Collections.sort(vals);
        
        return vals;
    }
}
