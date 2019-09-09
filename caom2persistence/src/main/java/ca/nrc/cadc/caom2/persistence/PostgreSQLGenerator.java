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

import ca.nrc.cadc.caom2.types.CartesianTransform;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.dali.postgresql.PgInterval;
import ca.nrc.cadc.dali.postgresql.PgSpoint;
import ca.nrc.cadc.dali.postgresql.PgSpoly;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class PostgreSQLGenerator extends SQLGenerator {

    private static final Logger log = Logger.getLogger(PostgreSQLGenerator.class);
    
    public PostgreSQLGenerator(String database, String schema) {
        super(database, schema);
        this.useIntegerForBoolean = true;
        this.persistOptimisations = true;
        this.persistReadAccessWithAsset = true;
        this.useLongForUUID = false;
        super.init();
    }

    @Override
    protected String getLimitConstraint(Integer batchSize) {
        if (batchSize == null) {
            return null;
        }
        return "LIMIT " + batchSize;
    }

    @Override
    protected void safeSetGroupOptimisation(StringBuilder sb, PreparedStatement ps, int col, Collection<URI> val) throws SQLException {
        // not null list; empty becomes zero-length string
        String gnames = Util.extractGroupNames(val);
        PGobject pgo = new PGobject();
        pgo.setType("tsvector");
        pgo.setValue(gnames);
        ps.setObject(col, pgo);
        if (sb != null) {
            sb.append(pgo.getValue());
            sb.append(",");
        }
    }

    
    @Override
    protected String literal(UUID value) {
        // uuid datatype accepts a string with the standard hex string format
        return "'" + value.toString() + "'";
    }

    /**
     * Store point value of an spoint column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param val
     * @throws SQLException 
     */
    @Override
    protected void safeSetPoint(StringBuilder sb, PreparedStatement ps, int col, Point val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetPoint] in: " + val);
            PgSpoint pgs = new PgSpoint();
            PGobject pgo = pgs.generatePoint(new ca.nrc.cadc.dali.Point(val.cval1, val.cval2));
            ps.setObject(col, pgo);
            if (sb != null) {
                sb.append(pgo.getValue());
                sb.append(",");
            }
        }
    }

    /**
     * Store list of points value in a double[] column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param val
     * @throws SQLException 
     */
    @Override
    protected void safeSetShape(StringBuilder sb, PreparedStatement ps, int col, Shape val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        } 
        
        log.debug("[safeSetShape] in: " + val);
        if (val instanceof Polygon) {
            Polygon poly = (Polygon) val;
            Double[] dval = new Double[2 * poly.getPoints().size()]; // 2 numbers per point
            int i = 0;
            for (Point p : ((Polygon) val).getPoints()) {
                dval[i++] = p.cval1;
                dval[i++] = p.cval2;
            }
            java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
            ps.setObject(col, arr);
            if (sb != null) {
                sb.append("[");
                for (double d : dval) {
                    sb.append(d).append(",");
                }
                sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
            }
            return;
        }
        
        if (val instanceof Circle) {
            Circle circ = (Circle) val;
            Double[] dval = new Double[3];
            dval[0] = val.getCenter().cval1;
            dval[1] = val.getCenter().cval2;
            dval[2] = circ.getRadius();
            java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
            ps.setObject(col, arr);
            if (sb != null) {
                sb.append("[");
                for (double d : dval) {
                    sb.append(d).append(",");
                }
                sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
            }
        }
    }
    
    /**
     * Store polygon value in an spoly column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param val
     * @throws SQLException 
     */
    @Override
    protected void safeSetShapeAsPolygon(StringBuilder sb, PreparedStatement ps, int col, Shape val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        } 
        
        log.debug("[safeSetShapeAsPolygon] in: " + val);
        if (val instanceof Polygon) {
            Polygon vp = (Polygon) val;
            ca.nrc.cadc.dali.Polygon poly = new ca.nrc.cadc.dali.Polygon();
            for (Point p : vp.getPoints()) {
                poly.getVertices().add(new ca.nrc.cadc.dali.Point(p.cval1, p.cval2));
            }
            PgSpoly pgs = new PgSpoly();
            PGobject pgo = pgs.generatePolygon(poly);
            ps.setObject(col, pgo);
            if (sb != null) {
                sb.append(pgo.getValue());
                sb.append(",");
            }
            return;
        }
        
        if (val instanceof Circle) {
            Circle cv = (Circle) val;
            ca.nrc.cadc.dali.Polygon poly = generatePolygonApproximation(cv, 13);
            PgSpoly pgs = new PgSpoly();
            PGobject pgo = pgs.generatePolygon(poly);
            ps.setObject(col, pgo);
            if (sb != null) {
                sb.append(pgo.getValue());
                sb.append(",");
            }
        }
    }

    /**
     * Store a MultiPolygon in a double[] column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param val
     * @throws SQLException 
     */
    @Override
    protected void safeSetMultiPolygon(StringBuilder sb, PreparedStatement ps, int col, MultiPolygon val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetMultiPolygon] in: " + val);
            Double[] dval = new Double[3 * val.getVertices().size()]; // 3 numbers per vertex
            int i = 0;
            for (Vertex v : val.getVertices()) {
                dval[i++] = v.cval1;
                dval[i++] = v.cval2;
                dval[i++] = v.getType().getValue().doubleValue();
            }
            java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
            ps.setObject(col, arr);
            if (sb != null) {
                sb.append("[");
                for (double d : dval) {
                    sb.append(d).append(",");
                }
                sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
            }
        }
    }

    /**
     * Store an interval in a polygon column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param val
     * @throws SQLException 
     */
    @Override
    protected void safeSetInterval(StringBuilder sb, PreparedStatement ps, int col, Interval val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetInterval] in: " + val);
            PgInterval pgi = new PgInterval();
            PGpolygon poly = pgi.generatePolygon2D(new ca.nrc.cadc.dali.DoubleInterval(val.getLower(), val.getUpper()));
            ps.setObject(col, poly);
            if (sb != null) {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }

    /**
     * Store a list of intervals in a polygon column.
     * 
     * @param sb
     * @param ps
     * @param col
     * @param subs
     * @throws SQLException 
     */
    @Override
    protected void safeSetSubIntervalList(StringBuilder sb, PreparedStatement ps, int col, List<SubInterval> subs)
            throws SQLException {
        if (subs == null || subs.isEmpty()) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetInterval] in: " + subs.size() + " SubIntervals");
            ca.nrc.cadc.dali.DoubleInterval[] dis = new ca.nrc.cadc.dali.DoubleInterval[subs.size()];
            int i = 0;
            for (SubInterval si : subs) {
                dis[i++] = new ca.nrc.cadc.dali.DoubleInterval(si.getLower(), si.getUpper());
            }
            PgInterval pgi = new PgInterval();
            PGpolygon poly = pgi.generatePolygon2D(dis);
            ps.setObject(col, poly);
            if (sb != null) {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }

    // working impl but cannot exactly round-trip through spoly column due to degrees to
    // radians, precision loss in db, and radians to degrees
    protected List<Point> getPointListFromSPolyString(ResultSet rs, int col) throws SQLException {
        // spoly string format: {(a,b),(c,d),(e,f) ... }
        String s = rs.getString(col);
        if (s == null) {
            return null;
        }
        
        PgSpoly pgs = new PgSpoly();
        List<Point> ret = new ArrayList<Point>();
        ca.nrc.cadc.dali.Polygon poly = pgs.getPolygon(s);
        for (ca.nrc.cadc.dali.Point p : poly.getVertices()) {
            ret.add(new Point(p.getLongitude(), p.getLatitude()));
        }
        return ret;
    }

    @Override
    protected List<Point> getPointList(ResultSet rs, int col) throws SQLException {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null) {
            return null;
        }

        if (coords.length >= 6) {
            List<Point> ret = new ArrayList<Point>();
            for (int i = 0; i < coords.length; i += 2) {
                double cval1 = coords[i];
                double cval2 = coords[i + 1];

                Point p = new Point(cval1, cval2);
                ret.add(p);
            }
            return ret;
        }
        throw new IllegalStateException("array length " + coords.length + "too short for List<Point>");
    }
    
    @Override
    protected Circle getCircle(ResultSet rs, int col) throws SQLException {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null) {
            return null;
        }
        if (coords.length == 3) {
            double cval1 = coords[0];
            double cval2 = coords[1];
            double rad = coords[2];
            
            Circle ret = new Circle(new Point(cval1, cval2), rad);
            log.debug("[getCircle] " + ret);
            return ret;
        }
        throw new IllegalStateException("array length " + coords.length + " invalid for Circle");
    }

    @Override
    protected MultiPolygon getMultiPolygon(ResultSet rs, int col) throws SQLException {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null) {
            return null;
        }

        MultiPolygon ret = new MultiPolygon();
        for (int i = 0; i < coords.length; i += 3) {
            double cval1 = coords[i];
            double cval2 = coords[i + 1];
            int s = (int) coords[i + 2];
            SegmentType t = SegmentType.toValue(s);
            Vertex v = new Vertex(cval1, cval2, t);
            ret.getVertices().add(v);
        }
        ret.validate();
        return ret;
    }

    @Override
    protected Interval getInterval(ResultSet rs, int col) throws SQLException {
        String s = rs.getString(col);
        if (s == null) {
            return null;
        }
        PgInterval pgi = new PgInterval();
        ca.nrc.cadc.dali.DoubleInterval di = pgi.getInterval(s);
        return new Interval(di.getLower(), di.getUpper());
    }

    @Override
    protected List<SubInterval> getSubIntervalList(ResultSet rs, int col) throws SQLException {
        String s = rs.getString(col);
        if (s == null) {
            return null;
        }
        PgInterval pgi = new PgInterval();
        ca.nrc.cadc.dali.DoubleInterval[] dis = pgi.getIntervalArray(s);
        List<SubInterval> ret = new ArrayList<SubInterval>();
        for (ca.nrc.cadc.dali.DoubleInterval di : dis) {
            ret.add(new SubInterval(di.getLower(), di.getUpper()));
        }
        return ret;
    }
    
    ca.nrc.cadc.dali.Polygon generatePolygonApproximation(Circle val, int numVerts) {
        if (numVerts < 4) {
            throw new IllegalArgumentException("number of vertices in approximation too small (min: 4)");
        }
        
        CartesianTransform trans = CartesianTransform.getTransform(val);
        Point cen = trans.transform(val.getCenter());
        double rad = val.getRadius();

        double phi = 2.0 * Math.PI / ((double) numVerts);
        // compute distance to vertices so that the edges are tangent and circle is
        // inside the polygon
        double vdist = val.getRadius() / Math.cos(phi / 2.0);
        //log.info("phi = " + phi + " vdist=" + vdist);
        
        CartesianTransform inv = trans.getInverseTransform();
        ca.nrc.cadc.dali.Polygon ret = new ca.nrc.cadc.dali.Polygon();        
        for (int i = 0; i < numVerts; i++) {
            double x = cen.cval1 + vdist * Math.cos(i * phi);
            double y = cen.cval2 + vdist * Math.sin(i * phi);
            Point p = new Point(x, y);
            p = inv.transform(p);
            ret.getVertices().add(new ca.nrc.cadc.dali.Point(p.cval1, p.cval2));
        }
        return ret;
    }
}
