/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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

package org.opencadc.caom2.db;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.dali.postgresql.PgInterval;
import ca.nrc.cadc.dali.postgresql.PgSpoint;
import ca.nrc.cadc.dali.postgresql.PgSpoly;
import ca.nrc.cadc.dali.util.MultiShapeFormat;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.wcs.Dimension2D;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class PostgreSQLDialect extends SQLDialect {

    private static final Logger log = Logger.getLogger(PostgreSQLDialect.class);
    
    public PostgreSQLDialect(boolean useIntegerForBoolean) {
        super(useIntegerForBoolean);
    }

    @Override
    public void safeSetArray(StringBuilder sb, PreparedStatement prep, int col, Set<URI> values) throws SQLException {
        
        if (values != null && !values.isEmpty()) {
            String[] array1d = new String[values.size()];
            int i = 0;
            for (URI u : values) {
                array1d[i] = u.toASCIIString();
                i++;
            }
            java.sql.Array arr = prep.getConnection().createArrayOf("text", array1d);
            prep.setObject(col, arr);
            if (sb != null) {
                sb.append("{URIs},");
            }
        } else {
            prep.setNull(col, Types.ARRAY);
            if (sb != null) {
                sb.append("null,");
            }
        }
        
    }
    
    @Override
    public void extractMultiURI(ResultSet rs, int col, Set<URI> out)
            throws SQLException {
        String[] raw  = Util.getTextArray(rs, col);
        stringToURI(raw, out);
    }
    
    private void stringToURI(String[] val, Set<URI> dest) {
        if (val == null) {
            return;
        }
        for (String s : val) {
            try {
                dest.add(new URI(s));
            } catch (URISyntaxException ex) {
                throw new RuntimeException("BUG: found invalid URI " + s, ex);
            }
        }
    }

    @Override
    public void safeSetGroupOptimisation(StringBuilder sb, PreparedStatement ps, int col, Collection<URI> val) throws SQLException {
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
    public String literal(UUID value) {
        // uuid datatype accepts a string with the standard hex string format
        return "'" + value.toString() + "'";
    }

    @Override
    public void safeSetDimension(StringBuilder sb, PreparedStatement ps, int col, Dimension2D val) throws SQLException {
        if (val == null) {
            ps.setNull(col, Types.ARRAY);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        }
        
        Long[] dval = new Long[] { val.naxis1, val.naxis2 };
        java.sql.Array arr = ps.getConnection().createArrayOf("bigint", dval);
        ps.setObject(col, arr);
        if (sb != null) {
            sb.append("[");
            for (Long d : dval) {
                sb.append(d).append(",");
            }
            sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
        }
    }

    @Override
    public Dimension2D getDimension(ResultSet rs, int col) throws SQLException {
        long[] vals = Util.getLongArray(rs, col);
        if (vals == null) {
            return null;
        }
        if (vals.length == 2) {
            return new Dimension2D(vals[0], vals[1]);
        }
        throw new IllegalStateException("array length " + vals.length + " invalid for Dimension2D");
    }
    
    // store as array to make it round trip
    @Override
    public void safeSetPoint(StringBuilder sb, PreparedStatement ps, int col, Point val)
            throws SQLException {
        if (val == null) {
            ps.setNull(col, Types.ARRAY);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        }
        
        Double[] dval = new Double[2];
        dval[0] = val.getLongitude();
        dval[1] = val.getLatitude();
        java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
        ps.setObject(col, arr);
        if (sb != null) {
            sb.append("[");
            for (Double d : dval) {
                sb.append(d).append(",");
            }
            sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
        }
    }

    @Override
    public Point getPoint(ResultSet rs, int col) throws SQLException {
        double[] vals = Util.getDoubleArray(rs, col);
        if (vals == null) {
            return null;
        }
        if (vals.length == 2) {
            return new Point(vals[0], vals[1]);
        }
        throw new IllegalStateException("array length " + vals.length + " invalid for Point");
    }
    
    // store as spoint to make it queryable
    public void safeSetPointOpt(StringBuilder sb, PreparedStatement ps, int col, Point val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetPoint] in: " + val);
            PgSpoint pgs = new PgSpoint();
            PGobject pgo = pgs.generatePoint(new ca.nrc.cadc.dali.Point(val.getLongitude(), val.getLatitude()));
            ps.setObject(col, pgo);
            if (sb != null) {
                sb.append(pgo.getValue());
                sb.append(",");
            }
        }
    }

    // store in array to rounds trip
    @Override
    public void safeSetShape(StringBuilder sb, PreparedStatement ps, int col, Shape val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        } 
        
        log.debug("[safeSetShape] in: " + val);
        Double[] dval = toArray(val);
        
        
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

    @Override
    public Shape getShape(ResultSet rs, int col) throws SQLException {
        double[] dval = Util.getDoubleArray(rs, col);
        if (dval == null) {
            return null;
        }
        if (dval.length == 3) {
            return new Circle(new Point(dval[0], dval[1]), dval[2]);
        }
        Polygon poly = new Polygon();
        for (int i = 0; i < dval.length; i += 2) {
            Point p = new Point(dval[i], dval[i + 1]);
            poly.getVertices().add(p);
        }
        return poly;
    }
    
    private Double[] toArray(Shape val) {
        if (val instanceof Polygon) {
            Polygon poly = (Polygon) val;
            Double[] dval = new Double[2 * poly.getVertices().size()]; // 2 numbers per point
            int i = 0;
            for (Point p : ((Polygon) val).getVertices()) {
                dval[i++] = p.getLongitude();
                dval[i++] = p.getLatitude();
            }
            return dval;
        }
        if (val instanceof Circle) {
            Circle circ = (Circle) val;
            Double[] dval = new Double[3];
            dval[0] = val.getCenter().getLongitude();
            dval[1] = val.getCenter().getLatitude();
            dval[2] = circ.getRadius();
            return dval;
        }
        throw new UnsupportedOperationException("toArray: " + val.getClass().getName());
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
    public void safeSetShapeAsPolygon(StringBuilder sb, PreparedStatement ps, int col, Shape val)
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
            Polygon poly = (Polygon) val;
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
            Polygon poly = Circle.generatePolygonApproximation(cv, 13);
            PgSpoly pgs = new PgSpoly();
            PGobject pgo = pgs.generatePolygon(poly);
            ps.setObject(col, pgo);
            if (sb != null) {
                sb.append(pgo.getValue());
                sb.append(",");
            }
        }
    }

    @Override
    public void safeSetMultiShape(StringBuilder sb, PreparedStatement ps, int col, MultiShape val)
            throws SQLException {
        String sval = null;
        if (val != null) {
            MultiShapeFormat fmt = new MultiShapeFormat();
            sval = fmt.format(val);
        }
        safeSetString(sb,ps,col,sval);
    }

    @Override
    public MultiShape getMultiShape(ResultSet rs, int col) throws SQLException {
        String sval = rs.getString(col);
        if (sval == null) {
            return null;
        }
        MultiShapeFormat fmt = new MultiShapeFormat();
        return fmt.parse(sval);
    }

    @Override
    public void safeSetInterval(StringBuilder sb, PreparedStatement ps, int col, Interval<Double> val) 
            throws SQLException {
        if (val == null) {
            ps.setNull(col, Types.ARRAY);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        }
        
        Double[] dval = new Double[2];
        dval[0] = val.getLower();
        dval[1] = val.getUpper();
        java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
        ps.setObject(col, arr);
        if (sb != null) {
            sb.append("[");
            for (Double d : dval) {
                sb.append(d).append(",");
            }
            sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
        }
    }

    @Override
    public Interval<Double> getInterval(ResultSet rs, int col) throws SQLException {
        double[] vals = Util.getDoubleArray(rs, col);
        if (vals == null) {
            return null;
        }
        if (vals.length == 2) {
            return new Interval<Double>(vals[0], vals[1]);
        }
        throw new IllegalStateException("array length " + vals.length + " invalid for Interval");
    }

    public void safeSetIntervalOptimization(StringBuilder sb, PreparedStatement ps, int col, Interval<Double> val)
            throws SQLException {
        if (val == null) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetInterval] in: " + val);
            PgInterval pgi = new PgInterval();
            PGpolygon poly = pgi.generatePolygon2D(new ca.nrc.cadc.dali.Interval<Double>(val.getLower(), val.getUpper()));
            ps.setObject(col, poly);
            if (sb != null) {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }

    @Override
    public void safeSetIntervalList(StringBuilder sb, PreparedStatement ps, int col, List<Interval<Double>> subs)
            throws SQLException {
        if (subs == null || subs.isEmpty()) {
            ps.setNull(col, Types.ARRAY);
            if (sb != null) {
                sb.append("null,");
            }
            return;
        }
        
        Double[] dval = new Double[2 * subs.size()];
        int i = 0;
        for (Interval<Double> inter : subs) {
            dval[i] = inter.getLower();
            dval[i + 1] = inter.getUpper();
            i += 2;
        }
        
        java.sql.Array arr = ps.getConnection().createArrayOf("float8", dval);
        ps.setObject(col, arr);
        if (sb != null) {
            sb.append("[");
            for (Double d : dval) {
                sb.append(d).append(",");
            }
            sb.setCharAt(sb.length() - 1, ']'); // replace last comma with closing ]
        }
    }

    @Override
    public void extractIntervalList(ResultSet rs, int col, List<Interval<Double>> vals) throws SQLException {
        double[] coords = Util.getDoubleArray(rs, col);
        if (coords == null) {
            return;
        }

        for (int i = 0; i < coords.length; i += 2) {
            double cval1 = coords[i];
            double cval2 = coords[i + 1];

            Interval<Double> di = new Interval<Double>(cval1, cval2);
            vals.add(di);
        }
    }

    public void safeSetIntervalListOptimization(StringBuilder sb, PreparedStatement ps, int col, List<Interval<Double>> subs)
            throws SQLException {
        if (subs == null || subs.isEmpty()) {
            ps.setObject(col, null);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            log.debug("[safeSetSubIntervalList] in: " + subs.size() + " Intervals");
            // cadc-dali-pg still uses concrete DoubleInterval class
            DoubleInterval[] dis = new DoubleInterval[subs.size()];
            int i = 0;
            for (Interval<Double> si : subs) {
                dis[i++] = new DoubleInterval(si.getLower(), si.getUpper());
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
}
