/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.compute.types;

import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.util.HexUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class MultiPolygon {
    private static final Logger log = Logger.getLogger(MultiPolygon.class);

    private final List<Vertex> vertices = new ArrayList<>();

    // lazily computed
    private transient Point center;
    private transient Double area;
    private transient Circle minimumSpanningCircle;
    private transient Boolean ccw;

    public MultiPolygon() {
    }

    public MultiPolygon(List<Vertex> vertices) {
        CaomValidator.assertNotNull(MultiPolygon.class, "vertices", vertices);
        this.vertices.addAll(vertices);
    }

    public final void validate() throws IllegalPolygonException {
        if (vertices.size() < 4) {
            throw new IllegalPolygonException(
                    "invalid MultiPolygon: " + vertices.size() + " vertices");
        }

        Vertex end = vertices.get(0);
        if (!SegmentType.MOVE.equals(end.getType())) {
            throw new IllegalPolygonException(
                    "invalid MultiPolygon: first vertex must be MOVE, found " + end);
        }
        end = vertices.get(vertices.size() - 1);
        if (!SegmentType.CLOSE.equals(end.getType())) {
            throw new IllegalPolygonException(
                    "invalid MultiPolygon: last vertex must be CLOSE, found " + end);
        }

        boolean openLoop = false;
        for (Vertex v : vertices) {
            if (SegmentType.MOVE.equals(v.getType())) {
                if (openLoop) {
                    throw new IllegalPolygonException(
                            "invalid MultiPolygon: found MOVE when loop already open");
                }
                openLoop = true;
            } else if (SegmentType.CLOSE.equals(v.getType())) {
                if (!openLoop) {
                    throw new IllegalPolygonException(
                            "invalid MultiPolygon: found CLOSE without MOVE");
                }
                openLoop = false;
            } else if (!openLoop) {
                throw new IllegalPolygonException(
                        "invalid MultiPolygon: found LINE without MOVE");
            }
        }

        validateSegments(this);
        initProps();
    }

    /**
     * Access the vertices for this polygon. If the vertex list is modified, the
     * caller must call validate in order to enforce correctness and recompute
     * the center, area, and minimum spanning circle (size).
     *
     * @return
     */
    public List<Vertex> getVertices() {
        return vertices;
    }

    public boolean getCCW() {
        if (ccw == null) {
            initProps();
        }
        return ccw;
    }

    public double getArea() {
        if (area == null) {
            initProps();
        }
        return area;
    }

    public Point getCenter() {
        if (center == null) {
            initProps();
        }
        return center;
    }

    public double getSize() {
        if (minimumSpanningCircle == null) {
            initProps();
        }
        return 2.0 * minimumSpanningCircle.getRadius();
    }

    public Circle getMinimumSpanningCircle() {
        if (minimumSpanningCircle == null) {
            initProps();
        }
        return minimumSpanningCircle;
    }

    public boolean isSimple() {
        int num = 0;
        for (Vertex v : vertices) {
            if (SegmentType.MOVE.equals(v.getType())) {
                num++;
            }
        }

        return (num == 1);
    }

    private void initProps() {
        PolygonProperties pp = computePolygonProperties();
        this.area = pp.area;
        this.center = pp.center;
        this.minimumSpanningCircle = pp.minSpanCircle;
        this.ccw = pp.windCounterClockwise;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(MultiPolygon.class.getSimpleName());
        sb.append("[");
        sb.append(isSimple()).append(",");
        sb.append(vertices.size());
        sb.append(",");
        for (int i = 0; i < vertices.size(); i++) {
            sb.append(vertices.get(i));
            sb.append(",");
        }
        // initProps();
        // sb.append("c=").append(getCenter().getLongitude()).append(",").append(getCenter().getLatitude()).append(",");
        // sb.append("a=").append(getArea());
        sb.append("]");
        return sb.toString();
    }

    public static class PolygonProperties implements Serializable {

        private static final long serialVersionUID = 201703221500L;

        boolean windCounterClockwise;
        Double area;
        Point center;
        Circle minSpanCircle;
    }

    // used by Polygon
    private PolygonProperties computePolygonProperties() {
        // log.debug("computePolygonProperties: " + poly);
        // the transform needed for computing things in long/lat using cartesian
        // approximation
        CartesianTransform trans = CartesianTransform.getTransform(this);
        MultiPolygon tpoly = trans.transform(this);

        // algorithm from
        // http://astronomy.swin.edu.au/~pbourke/geometry/polyarea/
        double a = 0.0;
        double cx = 0.0;
        double cy = 0.0;
        int lastMoveTo = 0;
        for (int i = 0; i < tpoly.getVertices().size(); i++) {
            Vertex v2 = (Vertex) tpoly.getVertices().get(i);
            if (SegmentType.MOVE.equals(v2.getType())) {
                lastMoveTo = i;
            } else {
                Vertex v1 = (Vertex) tpoly.getVertices().get(i - 1);
                if (SegmentType.CLOSE.equals(v2.getType())) {
                    // pretend it is a LINE_TO the lastMoveTo vertex
                    v2 = (Vertex) tpoly.getVertices().get(lastMoveTo);
                }

                double tmp = v1.getLongitude() * v2.getLatitude() - v2.getLongitude() * v1.getLatitude();
                a += tmp;
                cx += (v1.getLongitude() + v2.getLongitude()) * tmp;
                cy += (v1.getLatitude() + v2.getLatitude()) * tmp;
                // log.debug("[computePolygonProperties] " + v1 + "," + v2 + ","
                // + tmp + " " + cx + "," + cy);
            }
        }

        a *= 0.5;
        cx = cx / (6.0 * a);
        cy = cy / (6.0 * a);

        // quick and dirty minimum spanning circle computation
        double d = 0.0;
        Vertex e1 = null;
        Vertex e2 = null;
        for (int i = 0; i < tpoly.getVertices().size(); i++) {
            Vertex vi = (Vertex) tpoly.getVertices().get(i);
            if (!SegmentType.CLOSE.equals(vi.getType())) {
                for (int j = i + 1; j < tpoly.getVertices().size(); j++) {
                    Vertex vj = (Vertex) tpoly.getVertices().get(j);
                    if (!SegmentType.CLOSE.equals(vj.getType())) {
                        double d1 = vi.getLongitude() - vj.getLongitude();
                        double d2 = vi.getLatitude() - vj.getLatitude();
                        double dd = Math.sqrt(d1 * d1 + d2 * d2);
                        if (dd > d) {
                            d = dd;
                            e1 = vi;
                            e2 = vj;

                        }
                    }
                }
            }
        }

        PolygonProperties ret = new PolygonProperties();
        ret.windCounterClockwise = (a < 0.0); // RA-DEC increases left-up
        if (a < 0.0) {
            a *= -1.0;
        }
        ret.area = a;

        CartesianTransform inv = trans.getInverseTransform();

        ret.center = inv.transform(new Point(cx, cy));

        // midpoint between vertices
        if (e1 != null && e2 != null && d > 0.0) {
            Point cen = new Point(0.5 * Math.abs(e1.getLongitude() + e2.getLongitude()),
                    0.5 * Math.abs(e1.getLatitude() + e2.getLatitude()));
            Point mscc = inv.transform(cen);
            ret.minSpanCircle = new Circle(mscc, d / 2.0);
        }

        return ret;
    }

    // validation code
    private static class Segment implements Serializable {

        private static final long serialVersionUID = 201207300900L;

        Vertex v1;
        Vertex v2;

        Segment(Vertex v1, Vertex v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        double length() {
            return Math.sqrt(lengthSquared());
        }

        double lengthSquared() {
            return distanceSquared(v1, v2);
        }

        @Override
        public String toString() {
            return "Segment[" + v1.getLongitude() + "," + v1.getLatitude() + ":" + v2.getLongitude() + "," + v2.getLatitude() + "]";
        }
    }

    public static MultiPolygon compose(List<MultiPolygon> parts) {
        MultiPolygon poly = new MultiPolygon();
        for (MultiPolygon p : parts) {
            for (Vertex v : p.getVertices()) {
                poly.getVertices().add(v);
            }
        }
        return poly;
    }

    public static List<MultiPolygon> decompose(MultiPolygon p) {
        if (p == null) {
            return null;
        }
        List<MultiPolygon> polys = new ArrayList<MultiPolygon>();
        MultiPolygon prev = null; // needs to be a stack?
        MultiPolygon cur = null;
        for (Vertex v : p.getVertices()) {
            if (cur == null) { // start new poly
                //log.debug("new MultiPolygon");
                cur = new MultiPolygon();
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            } else if (SegmentType.MOVE.equals(v.getType())) {
                //log.debug("vertex: " + v);
                //cur.getVertices().add(new Vertex(0.0, 0.0,SegmentType.CLOSE));
                //polys.add(cur);
                //log.debug("close MultiPolygon");
                prev = cur; // embedded loop

                //log.debug("new MultiPolygon");
                cur = new MultiPolygon();
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            } else if (SegmentType.CLOSE.equals(v.getType())) {
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
                polys.add(cur);
                cur = prev;
                //log.debug("close MultiPolygon");
            } else {
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
        }
        return polys;
    }

    // used by Polygon
    private static void validateSegments(MultiPolygon poly)
            throws IllegalPolygonException {
        List<MultiPolygon> parts = decompose(poly);
        for (MultiPolygon mp : parts) {
            validatePart(mp);
        }
    }

    private static void validatePart(MultiPolygon poly)
            throws IllegalPolygonException {
        CartesianTransform trans = CartesianTransform.getTransform(poly);
        MultiPolygon tpoly = trans.transform(poly);

        Iterator<Vertex> vi = tpoly.getVertices().iterator();
        List<Segment> tsegs = new ArrayList<Segment>();
        List<Segment> psegs = tsegs;
        Vertex start = vi.next();
        Vertex v1 = start;
        while (vi.hasNext()) {
            Vertex v2 = vi.next();
            if (SegmentType.CLOSE.equals(v2.getType())) {
                log.debug("[validateSegments] tsegs: CLOSE -> " + start);
                v2 = start;
                start = null;
            }
            Segment s = new Segment(v1, v2);
            log.debug("[validateSegments] tsegs: " + s);
            tsegs.add(s);
            if (start == null) {
                if (vi.hasNext()) {
                    start = vi.next(); // another move
                    log.debug("[validateSegments] tsegs: new start " + start);
                }
                v1 = start;
            } else {
                v1 = v2;
            }
        }
        if (poly != tpoly) {
            // make segments with orig coords for reporting
            vi = poly.getVertices().iterator();
            psegs = new ArrayList<Segment>();
            start = (Vertex) vi.next();
            v1 = start;
            while (vi.hasNext()) {
                Vertex v2 = vi.next();
                if (SegmentType.CLOSE.equals(v2.getType())) {
                    log.debug("[validateSegments] psegs: CLOSE -> " + start);
                    v2 = start;
                    start = null;
                }
                Segment s = new Segment(v1, v2);
                log.debug("[validateSegments] psegs: " + s);
                psegs.add(s);
                if (start == null) {
                    if (vi.hasNext()) {
                        start = vi.next(); // another move
                        log.debug("[validateSegments] psegs: new start " + start);
                    }
                    v1 = start;
                } else {
                    v1 = v2;
                }
            }
        }
        intersects(tsegs, psegs);
    }

    private static void intersects(List<Segment> transSegments, List<Segment> origSegments) 
            throws IllegalPolygonException {
        for (int i = 0; i < transSegments.size(); i++) {
            Segment s1 = transSegments.get(i);
            for (int j = 0; j < transSegments.size(); j++) {
                if (i != j) {
                    Segment s2 = transSegments.get(j);
                    if (intersects(s1, s2)) {
                        Segment r1 = origSegments.get(i);
                        Segment r2 = origSegments.get(j);
                        throw new IllegalPolygonException("invalid Polygon: segment intersect " + r1 + " vs " + r2);
                    }
                }
            }
        }
    }

    private static boolean intersects(Segment ab, Segment cd) {
        //log.debug("intersects: " + ab + " vs " + cd);
        // rden = (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)
        double den = (ab.v2.getLongitude() - ab.v1.getLongitude()) * (cd.v2.getLatitude() - cd.v1.getLatitude())
                - (ab.v2.getLatitude() - ab.v1.getLatitude()) * (cd.v2.getLongitude() - cd.v1.getLongitude());
        //log.debug("den = " + den);

        //rnum = (Ay-Cy)(Dx-Cx)-(Ax-Cx)(Dy-Cy)
        double rnum = (ab.v1.getLatitude() - cd.v1.getLatitude()) * (cd.v2.getLongitude() - cd.v1.getLongitude())
                - (ab.v1.getLongitude() - cd.v1.getLongitude()) * (cd.v2.getLatitude() - cd.v1.getLatitude());
        //log.debug("rnum = " + rnum);

        if (Math.abs(den) < 1.0e-12) { //(den == 0.0)
            if (Math.abs(rnum) < 1.0e-12) { //(rnum == 0.0)
                // colinear: check overlap on one axis
                if (ab.v2 == cd.v1 || ab.v1 == cd.v2) {
                    return false; // end-to-end
                }
                double len1 = ab.lengthSquared();
                double len2 = cd.lengthSquared();
                Segment s = ab;
                if (len2 > len1) {
                    s = cd; // the longer one
                }
                double dx = Math.abs(s.v1.getLongitude() - s.v2.getLongitude());
                double dy = Math.abs(s.v1.getLatitude() - s.v2.getLatitude());
                if (dx > dy) { // more horizontal = project to coordX
                    if (ab.v2.getLongitude() < cd.v1.getLongitude()) {
                        return false; // ab left of cd
                    }
                    if (ab.v1.getLongitude() > cd.v2.getLongitude()) {
                        return false; // ab right of cd
                    }
                } else { // more vertical = project to coordY
                    if (ab.v2.getLatitude() < cd.v1.getLatitude()) {
                        return false; // ab below cd
                    }
                    if (ab.v1.getLatitude() > cd.v2.getLatitude()) {
                        return false; // ab above cd
                    }
                }
                return true; // overlapping
            }
            return false; // just parallel
        }

        double r = rnum / den;
        //log.debug("radius = " + radius);
        // no intersect, =0 or 1 means the ends touch, which is normal  but pg_sphere doesn't like it
        //if (radius < 0.0 || radius > 1.0)
        if (r <= 0.0 || r >= 1.0) {
            return false;
        }

        //snum = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
        double snum = (ab.v1.getLatitude() - cd.v1.getLatitude()) * (ab.v2.getLongitude() - ab.v1.getLongitude())
                - (ab.v1.getLongitude() - cd.v1.getLongitude()) * (ab.v2.getLatitude() - ab.v1.getLatitude());
        //log.debug("snum = " + snum);

        double s = snum / den;
        //log.debug("s = " + s);
        //if (s < 0.0 || s > 1.0)
        if (s <= 0.0 || s >= 1.0) {
            return false; // no intersect, =0 or 1 means the ends touch, which is normal
        }

        // radius in [0,1] and s in [0,1] = intersects
        return true;
    }

    private static double distanceSquared(Vertex v1, Vertex v2) {
        return (v1.getLongitude() - v2.getLongitude()) * (v1.getLongitude() - v2.getLongitude())
                + (v1.getLatitude() - v2.getLatitude()) * (v1.getLatitude() - v2.getLatitude());
    }

}
