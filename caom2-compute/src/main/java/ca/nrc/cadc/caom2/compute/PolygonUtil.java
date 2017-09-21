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
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.types.CartesianTransform;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.types.impl.GrahamScan;
import ca.nrc.cadc.caom2.types.impl.SortablePoint2D;
import java.awt.geom.Area;
import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public final class PolygonUtil
{
    private static Logger log = Logger.getLogger(PolygonUtil.class);

    private static final double DEFAULT_SCALE = 0.02;
    private static final double MAX_SCALE = 0.07;
    
    /**
     * Flag to control attempts to use getConcaveHull from getOuterHull.
     * When this is false, the getOuterHull always computes and returns 
     * a convex hull. Currently: hard-coded to false.
     */
    public static final boolean ENABLE_CONCAVE_OUTER = false;

    public static Polygon toPolygon(Shape s)
    {
        if (s == null)
            return null;

        if (s instanceof Polygon)
            return (Polygon) s;

        throw new UnsupportedOperationException(s.getClass().getSimpleName() + " -> Polygon");
    }

    /**
     * Compute a simple outer hull of the specified polygon. This method
     * is simple a wrapper that calls getConcaveHull and getConvexHull and
     * the picks the most suitable value. Suitable is poorly defined.
     *
     * @param poly
     * @return a simple bounding polygon
     */
    public static Polygon getOuterHull(final MultiPolygon poly)
    {
        Polygon convex = getConvexHull(poly);
        double cvxArea = convex.getArea();
        log.debug("[getOuterHull] convex: " + convex + " A = " + cvxArea);
        
        if (ENABLE_CONCAVE_OUTER)
        {
            Polygon concave = getConcaveHull(poly);
            if (concave != null)
            {
                double ccvArea = concave.getArea();
                log.debug("[getOuterHull] concave: " + concave + " A = " + ccvArea);
                
                // if (  concave isBetterThan convex )
                {
                    log.debug("[getOuterHull] pick CONCAVE " + concave);
                    return concave;
                }
            }
            else
                log.debug("[getOuterHull] concave: FAIL");
        }
        
        log.debug("[getOuterHull] pick CONVEX " + convex);
        return convex;
    }

    /**
     * Compute a simple concave outer hull of the specified polygon. This method
     * removes holes and finds a single concave polygon that includes all the area
     * of the original (but usually more). This is not a general purpose concave hull 
     * algorithm and it fails if the disjoint parts are too far apart.
     *
     * @param poly
     * @return concave hull or null if current algorithms fail
     */
    public static Polygon getConcaveHull(final MultiPolygon poly)
    {
        List<MultiPolygon> parts = decompose(poly, true);  // decompose and remove holes
    
        double scale = 0.0;
        MultiPolygon outer = null;
        while (outer == null && scale <= MAX_SCALE)
        {
            log.debug("[getConcaveHull] trying scale=" + scale + " with " + parts.size() + " simple polygons");
            MultiPolygon tmp = transComputeUnion(parts, scale, true, true);
            
            log.debug("[getConcaveHull] union = " + tmp);
            if (tmp.isSimple())
            {
                try 
                { 
                    validateSegments(tmp);
                    outer = tmp;
                }
                catch(IllegalPolygonException skip) 
                { 
                    log.debug("scale: " + scale + " -> " + skip); 
                }
            }
            scale += DEFAULT_SCALE; // 2x, 3x, etc
        }
        if (outer != null)
        {
            log.debug("[getConcaveHull] SUCCESS: " + outer);
            boolean ccw = outer.getCCW();
            List<Point> pts = new ArrayList<Point>();
            for (Vertex v : outer.getVertices())
                if (!SegmentType.CLOSE.equals(v.getType()))
                {
                    Point p = new Point(v.cval1, v.cval2);
                    if (ccw)
                        pts.add(p);
                    else 
                        pts.add(0, p); // add to start aka reverse list
                }
            Polygon ret = new Polygon(pts, poly);
            return ret;
        }

        log.debug("[getConcaveHull] FAILED");
        return null;
    }

    /**
     * Compute a simple convex hull that contains the specified polygon.
     * 
     * @param poly
     * @return
     */
    public static Polygon getConvexHull(final MultiPolygon poly)
    {
        log.debug("[getConvexHull] " + poly);
        if (poly == null)
            return null;
        

        CartesianTransform trans = CartesianTransform.getTransform(poly);
        MultiPolygon tpoly = trans.transform(poly);
        log.debug("[getConvexHull] tpoly " + tpoly);
        
        MultiPolygon tconvex = computeConvexHull(tpoly);
        log.debug("[getConvexHull] tconvex " + tconvex);
        
        smooth(tconvex);
        log.debug("[getConvexHull] tsmooth " + tconvex);
        
        MultiPolygon convex = trans.getInverseTransform().transform(tconvex);
        log.debug("[getConvexHull] convex " + convex );
        
        // TODO: convert
        boolean ccw = convex.getCCW();
        List<Point> pts = new ArrayList<Point>();
        for (Vertex v : convex.getVertices())
            if (!SegmentType.CLOSE.equals(v.getType()))
            {
                Point p = new Point(v.cval1, v.cval2);
                if (ccw)
                    pts.add(p); // add to end
                else
                    pts.add(0, p); // add to start aka revserse order
            }
        Polygon ret = new Polygon(pts, poly);
        return ret;
    }

    public static MultiPolygon intersection(MultiPolygon p1, MultiPolygon p2)
    {
        double[] cube = CartesianTransform.getBoundingCube(p1, null);
        cube = CartesianTransform.getBoundingCube(p2, cube);

        CartesianTransform trans = CartesianTransform.getTransform(cube, false);

        MultiPolygon pt1 = trans.transform(p1);
        MultiPolygon pt2 = trans.transform(p2);

        MultiPolygon inter = doIntersectCAG(pt1, pt2);
        
        if (inter != null)
            inter = trans.getInverseTransform().transform(inter);

        return inter;
    }
  
    // transform, compute, inverse transforms
    public static MultiPolygon transComputeUnion(List<MultiPolygon> polys, double scale, boolean unscale, boolean removeHoles)
    {
        if (polys.size() == 1)
            return polys.get(0);
        
        // transform the input polygons to some other 2D coordinates where cartesian CAG is good enough
        double[] cube = null;
        for (MultiPolygon p : polys)
        {
            cube = CartesianTransform.getBoundingCube(p, cube);
        }
        CartesianTransform trans = CartesianTransform.getTransform(cube, false);
        log.debug("working transform: " + trans);
        
        // transform all the polygons
        List<MultiPolygon> work = new ArrayList<MultiPolygon>(polys.size());
        for (MultiPolygon p : polys)
        {
            work.add(trans.transform(p));
        }

        // compute union
        MultiPolygon union = computeUnion(work, scale, unscale, removeHoles);

        // inverse transform back to longitude,latitude
        union = trans.getInverseTransform().transform(union);

        return union;
    }

     // scale, compute, remove-holes, [unscale], smooth; assumes cartesian approx is safe
    public static MultiPolygon computeUnion(List<MultiPolygon> work, double scale, boolean unscale, boolean removeHoles)
    {
        log.debug("[computeUnion] work=" + work.size() + " scale="+scale 
                + " unscale="+unscale + " removeHoles="+removeHoles);
        
        // scale all the polygons up to cause nearby polygons to intersect
        List<MultiPolygon> scaled = work;
        if (scale > 0.0)
        {
            scaled = new ArrayList<MultiPolygon>(work.size());
            for (MultiPolygon p : work)
            {
                scaled.add(scaleMultiPolygon(p, scale));
            }
        }

        MultiPolygon poly = null;
        for (MultiPolygon p : scaled)
        {
            if (poly == null)
                poly = p;
            else
                poly = doUnionCAG(poly, p);
        }
        log.debug("[computeUnion] raw " + poly);
        
        if (removeHoles)
            poly = removeHoles(poly);
        
        if (unscale && scale > 0.0)
            poly = unscaleMultiPolygon(poly, scaled, scale);

        smooth(poly);

        return poly;
    }

    private static MultiPolygon removeHoles(MultiPolygon poly)
    {
        // impl: no checking, just blindly decompose and reassemble
        List<MultiPolygon> parts = decompose(poly, true);
        return compose(parts);
    }
    
    static MultiPolygon compose(List<MultiPolygon> parts)
    {
        MultiPolygon poly = new MultiPolygon();
        for (MultiPolygon p : parts)
        {
            for (Vertex v : p.getVertices())
            {
                poly.getVertices().add(v);
            }
        }
        return poly;
    }
    
    static List<MultiPolygon> decompose(MultiPolygon p)
    {
        if (p == null)
            return null;
        List<MultiPolygon> polys = new ArrayList<MultiPolygon>();
        MultiPolygon prev = null; // needs to be a stack?
        MultiPolygon cur = null;
        for (Vertex v : p.getVertices())
        {
            if (cur == null) // start new poly
            {
                //log.debug("new MultiPolygon");
                cur = new MultiPolygon();
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
            else if (SegmentType.MOVE.equals(v.getType()))
            {
                //log.debug("vertex: " + v);
                //cur.getVertices().add(new Vertex(0.0, 0.0,SegmentType.CLOSE));
                //polys.add(cur);
                //log.debug("close MultiPolygon");
                prev = cur; // embedded loop

                //log.debug("new MultiPolygon");
                cur = new MultiPolygon();
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
            else if (SegmentType.CLOSE.equals(v.getType()))
            {
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
                polys.add(cur);
                cur = prev;
                //log.debug("close MultiPolygon");
            }
            else
            {
                //log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
        }
        return polys;
    }

    static List<MultiPolygon> decompose(MultiPolygon poly, boolean removeHoles)
    {
        log.debug("[decompose] START: " + poly + " removeHoles="+removeHoles);
        List<MultiPolygon> samples = decompose(poly);
        
        // find samples and holes via sign of the area
        boolean cw = poly.getCCW();
        ListIterator<MultiPolygon> iter = samples.listIterator();
        int num = 0;
        while ( iter.hasNext() )
        {
            MultiPolygon part = iter.next();
            boolean pcw = part.getCCW();
            if (cw != pcw) // opposite sign
            {
                iter.remove();
                num++;
            }
        }
        if (num > 0)
            log.debug("[removeHoles] discarded " + num + " holes");
        
        poly.getVertices().clear();
        for (MultiPolygon p : samples)
        {
            poly.getVertices().addAll(p.getVertices());
        }
        log.debug("[removeHoles] done: " + poly);
        return samples;
    }
    
    // removes all holes with fractional area less than rat
    static boolean removeSmallHoles(MultiPolygon poly, double rat)
    {
        if (poly.getVertices().size() <= 8)
            return false; // cannot have holes with fewer than 8 vertices (two triangles and 2 CLOSE)

        boolean hasHoles = false;
        log.debug("[removeSmallHoles] start: " + poly);
        MultiPolygon tmp = new MultiPolygon();
        double polyArea = poly.getArea();
        boolean go = true;
        while(go)
        {
            go = false;
            tmp.getVertices().clear();
            boolean restart = false;
            Iterator<Vertex> vi = poly.getVertices().iterator();
            while ( !restart && vi.hasNext() )
            {
                Vertex v = vi.next();
                tmp.getVertices().add(v);
                if (SegmentType.CLOSE.equals(v.getType()))
                {
                    if (tmp.getVertices().size() <= 3) // 2+close is a line == 0 area
                    {
                        // this could be a line segment that is in or out: remove both kinds
                        //log.debug("[removeHoles] removing vertex " + start + " " + tmp.getVertices().size() + " times");
                        //for (int j=0; j<tmp.getVertices().size(); j++)
                        //    pVerts.remove(start);
                        log.debug("[removeSmallHoles] remove scrap segment " + tmp + " from " + poly);
                        for (Vertex rv : tmp.getVertices())
                        {
                            boolean ok = poly.getVertices().remove(rv);
                            if (!ok)
                                log.debug("[removeSmallHoles] found hole " + tmp + " but failed to remove " + v + " from " + poly);
                        }
                        go = true;
                        restart = true;
                    }
                    else
                    {
                        // closed loop
                        double tmpArea = tmp.getArea();
                        double da = tmpArea/polyArea;
                        boolean isHole = (tmp.getCCW() != poly.getCCW()); // opposite winding
                        hasHoles = isHole;
                        if (isHole && da < rat) // hole && small
                        {
                            // remove all these verts from list
                            //log.debug("[removeHoles] removing hole: " + tmp + " with da = " + da);
                            //log.debug("removing vertex " + start + " " + tmp.getVertices().size() + " times");
                            //for (int j=0; j<tmp.getVertices().size(); j++)
                            //    pVerts.remove(start);
                            log.debug("[removeSmallHoles] remove hole " + tmp + " from " + poly);
                            for (Vertex rv : tmp.getVertices())
                            {
                                boolean ok = poly.getVertices().remove(rv);
                                if (!ok)
                                    log.debug("[removeSmallHoles] found hole " + tmp + " but failed to remove " + v + " from " + poly);
                            }
                            go = true;
                            restart = true;
                        }
                    }
                    // we kept this loop, so clear and proceed
                    tmp.getVertices().clear();
                }
            }
        }
        log.debug("[removeSmallHoles] done: " + poly);
        return hasHoles;
    }

    private static void smooth(MultiPolygon poly)
    {
        List<MultiPolygon> parts = decompose(poly);
        poly.getVertices().clear();
        for (MultiPolygon p : parts)
        {
            int prev = p.getVertices().size();
            int num = 0;
            boolean improve = true;
            while (improve) // improving
            {
                smoothSimpleColinearSegments(p, 0.05); // ~3 deg ~ 0.05 rad
                smoothSimpleAdjacentVertices(p, 1.0e-2); // 1% of size
                smoothSimpleColinearSegments(p, 0.05); // ~3 deg ~ 0.05 rad
                int cur = p.getVertices().size();
                
                if (cur == prev)
                    improve = false;
                else
                {
                    prev = cur;
                }
            }
            for (Vertex v : p.getVertices())
            {
                poly.getVertices().add(v);
            }
        }
        
        //smoothSimpleSmallAreaChange(poly, 1.0e-4); // relative area change
        
        log.debug("[smooth] after: " + poly);
    }
    
    private static void smoothSimpleAdjacentVertices(MultiPolygon poly, double rsl)
    {
        log.debug("[smooth.adjacent] " + poly);
        if (poly.getVertices().size() <= 5) // 4+close == rectangle
            return;
        
        Circle msc = poly.getMinimumSpanningCircle();
        double tol = msc.getSize()*rsl;
        //double tol = Math.sqrt(pp.area)*rsl; // ~same for squares, smaller for skinny rectangles
        log.debug("[smooth.adjacent] r=" + msc.getRadius()
            + " tol=" + tol);
        
        Iterator<Vertex> vi = poly.getVertices().iterator();
        List<Segment> segs = new ArrayList<Segment>();
        Vertex start = vi.next();
        Vertex v1 = start;
        while ( vi.hasNext() )
        {
            Vertex v2 = vi.next();
            if (SegmentType.CLOSE.equals(v2.getType()))
                v2 = start;
            Segment s = new Segment(v1, v2);
            segs.add(s);
            log.debug("[smooth.adjacent] before: seg.length " + s.length());
            v1 = v2;
        }
        log.debug("[smooth.adjacent] before: " + segs.size() + " segments");
            
        double frac = 1.0e-4; // start with reallty small delta and work up
        boolean changed = true;
        while (frac <= 1.0 || changed)
        {
            changed = false;
            frac *= 10.0; // 1e-3, 1e-2, 1e-1, 1
            for (int i=0; i< segs.size(); i++) // go around twice
            {
                int n2 = i;
                Segment bc = segs.get(n2); // target to test/remove
                
                int n1 = i - 1; // previous
                if (n1 == -1)
                    n1 = segs.size() - 1; // previous == last
                Segment ab = segs.get(n1);
                
                int n3 = i+1; // next
                if (n3 == segs.size())
                    n3 = 0; // next == first
                Segment cd  = segs.get(n3);
                
                double len = bc.length();
                log.debug("[smooth.adjacent] " + bc + " " + len + " triple " + n1 + ","+n2+","+n3);
                if (segs.size() > 4 && len < tol*frac) // realistically: rectangle is the smallest
                {
                    Vertex nv1 = ab.v1;

                    // remove v1 and v2 from poly - replace with average
                    Vertex nv2 = new Vertex((bc.v1.cval1 + bc.v2.cval1)/2.0, (bc.v1.cval2 + bc.v2.cval2)/2.0, SegmentType.LINE);
                    
                    Vertex nv3 = cd.v2;
                    
                    Segment ns1 = new Segment(nv1, nv2);
                    Segment ns2 = new Segment(nv2, nv3);
                    
                    log.debug("[smooth.adjacent] replace segment: " + n1 + " " + ns1);
                    segs.set(n1, ns1);
                    log.debug("[smooth.adjacent] replace segment: " + n3 + " " + ns2);
                    segs.set(n3, ns2);
                    log.debug("[smooth.adjacent] remove segment: " + n2);
                    segs.remove(n2);
                    i--; // back shift so we don't skip
                    //for (Segment s : segs)
                        //log.debug("[smooth.adjacent] sfter replace: seg.length " + s.length());
                    changed = true;
                }
                    
            }
        }
        log.debug("[smooth.adjacent] after: " + segs.size() + " segments");
        poly.getVertices().clear();
        for (int i=0; i<segs.size(); i++)
        {
            Segment s = segs.get(i);
            log.debug("[smooth.adjacent] after: seg.length " + s.length());
            Vertex v = s.v1;
            if ( i == 0 && !v.getType().equals(SegmentType.MOVE))
                v = new Vertex(v.cval1, v.cval2, SegmentType.MOVE);
            poly.getVertices().add(v);
        }
        poly.getVertices().add(Vertex.CLOSE);
        log.debug("[smooth.adjacent] after: " + poly);
    }
    
    // assume simple polygon
    private static void smoothSimpleColinearSegments(MultiPolygon poly, double tol)
    {
        log.debug("[smooth.colinear] " + poly);
        if (poly.getVertices().size() <= 4) // 3+close == triangle
            return;

        Iterator<Vertex> vi = poly.getVertices().iterator();
        List<Segment> segs = new ArrayList<Segment>();
        Vertex start = vi.next();
        Vertex v1 = start;
        while ( vi.hasNext() )
        {
            Vertex v2 = vi.next();
            if (SegmentType.CLOSE.equals(v2.getType()))
                v2 = start;
            Segment s = new Segment(v1, v2);
            segs.add(s);
            v1 = v2;
        }
        log.debug("[smooth.colinear] before: " + segs.size() + " segments");

        boolean changed = true;
        while (segs.size() > 4 && changed)
        {
            changed = false;
            // merge colinear segments
            for (int i=0; i<segs.size(); i++)
            {
                Segment ab = segs.get(i);
                int n = i+1;
                if (n == segs.size())
                    n = 0; // wrap to first segment
                Segment bc = segs.get(n);
                if (segs.size() > 4 && colinear(ab, bc, tol)) // realistically: rectangle is the smallest
                {
                    Vertex nv1 = new Vertex(ab.v1.cval1, ab.v1.cval2, SegmentType.LINE);
                    Vertex nv2 = bc.v2;
                    Segment ns = new Segment(ab.v1, bc.v2);
                    log.debug("[smooth.colinear] " + i + " " + ab + " + " + bc + ": removing " + ab.v2 + " aka " + bc.v1);
                    segs.set(i, ns); // replace before remove so index is correct
                    segs.remove(bc);
                    i--; // back shift so we don't skip
                    
                    changed = true;
                }
                else
                    log.debug("[smooth.colinear] non-colinear: " + i + " " + ab + " + " + bc);
            }
        }
        log.debug("[smooth.colinear] after: " + segs.size() + " segments");
        poly.getVertices().clear();
        for (int i=0; i<segs.size(); i++)
        {
            Segment s = segs.get(i);
            log.debug("[smooth.colinear] after: seg.length " + s.length());
            Vertex v = s.v1;
            if ( i == 0 && !v.getType().equals(SegmentType.MOVE))
                v = new Vertex(v.cval1, v.cval2, SegmentType.MOVE);
            poly.getVertices().add(v);
        }
        poly.getVertices().add(Vertex.CLOSE);
        log.debug("[smooth.colinear] after: " + poly);
    }
    
    // remove vertices when the area change is very small (optional: dA >= 0)
    private static void smoothSimpleSmallAreaChange(MultiPolygon poly, double rat)
    {
        boolean allowNegDA = false;
        
        log.debug("[smooth.area] " + poly + " rat="+rat);
        if (poly.getVertices().size() <= 4) // 3+close == triangle
            return;
        MultiPolygon tmp = new MultiPolygon();
        List<Vertex> verts = tmp.getVertices();
        double polyArea = poly.getArea();
        boolean changed = true;
        while (changed)
        {
            int posI = -1;
            int negI = -1;
            double posDA = Double.MAX_VALUE;
            double negDA = Double.MAX_VALUE;
            Vertex posV = null;
            Vertex negV = null;
            for (int i=0; i<poly.getVertices().size(); i++)
            {
                verts.clear();
                verts.addAll(poly.getVertices());
                Vertex v = tmp.getVertices().get(i);
                log.debug("[smooth.area] (try) " + v);
                boolean move = SegmentType.MOVE.equals(v.getType());
                boolean line = SegmentType.LINE.equals(v.getType());
                if (line || move)
                {
                    verts.remove(i);
                    if (move)
                    {
                        // change the next from  LINE to MOVE
                        Vertex vl = verts.get(i);
                        if (SegmentType.LINE.equals(vl.getType()))
                        {
                            Vertex vm = new Vertex(vl.cval1, vl.cval2, SegmentType.MOVE);
                            log.debug("[smooth.area] (try) change " + vl + " -> " + vm);
                            verts.set(i, vm);
                        }
                        else
                            throw new IllegalStateException("found " + vl + " after " + v);
                    }
                    double tmpArea = tmp.getArea();
                    double da = (tmpArea - polyArea)/polyArea;
                    log.debug("[smooth.area] (try) remove " + v + ", dA = " + da + ", " + polyArea + " -> " + tmpArea);
                    if (da >= 0.0)
                    {
                        if (da < posDA)
                        {
                            posDA = da;
                            posV = v;
                            posI = i;
                        }
                    }
                    else
                    {
                        da *= -1.0; // abs
                        if (da < negDA)
                        {
                            negDA = da;
                            negV = v;
                            negI = i;
                        }
                    }
                }
            }
            changed = false;
            if (posV != null && posDA < rat)
            {
                log.debug("[smooth.area] dA = " + posDA + " remove " + posV);
                poly.getVertices().remove(posV);
                if ( SegmentType.MOVE.equals(posV.getType()) )
                {
                    // change the next from  LINE to MOVE
                    Vertex vl = poly.getVertices().get(posI);
                    Vertex vm = new Vertex(vl.cval1, vl.cval2, SegmentType.MOVE);
                    log.debug("[smooth.area] change " + vl + " -> " + vm);
                    poly.getVertices().set(posI, vm);
                }
                changed = true;
            }
            else if (allowNegDA && negV != null && negDA < rat)
            {
                log.debug("[smooth.area] dA = -" + negDA + " remove " + negV);
                poly.getVertices().remove(negV);
                if ( SegmentType.MOVE.equals(negV.getType()) )
                {
                    // change the next from  LINE to MOVE
                    Vertex vl = poly.getVertices().get(negI);
                    Vertex vm = new Vertex(vl.cval1, vl.cval2, SegmentType.MOVE);
                    log.debug("[smooth.area] change " + vl + " -> " + vm);
                    poly.getVertices().set(negI, vm);
                }
                changed = true;
            }
            else
            {
                if (negV != null) log.debug("[smooth.area] negDA = -" + negDA + " keep " + negV);
                if (posV != null) log.debug("[smooth.area] posDA = " + posDA + " keep " + posV);
            }
        }
    }
  
    private static class ScaledVertex extends Vertex
    {
        private static final long serialVersionUID = 201207271500L;
        Vertex orig;
        ScaledVertex(double c1, double c2, SegmentType t, Vertex orig)
        {
            super(c1, c2, t);
            this.orig = orig;
        }
    }
    
    private static MultiPolygon scaleMultiPolygon(MultiPolygon poly, double scale)
    {
        log.debug("[scaleMultiPolygon] start: " + poly + " BY " + scale);
        MultiPolygon ret = new MultiPolygon();
        Point c = poly.getCenter();
        for (Vertex v : poly.getVertices())
        {
            if (SegmentType.CLOSE.equals(v.getType()))
            {
                Vertex sv = new ScaledVertex(0.0, 0.0, v.getType(), v);
                ret.getVertices().add(sv);
            }
            else
            {
                double dx = (v.cval1 - c.cval1) * scale;
                double dy = (v.cval2 - c.cval2) * scale;
                Vertex sv = new ScaledVertex(v.cval1 + dx, v.cval2 + dy, v.getType(), v);
                ret.getVertices().add(sv);
            }
        }
        log.debug("[scaleMultiPolygon] done: " + ret);
        return ret;
    }
    
    private static MultiPolygon unscaleMultiPolygon(MultiPolygon poly, List<MultiPolygon> scaled, double scale)
    {
        log.debug("[unscaleMultiPolygon] scale: " + scale + " IN: " + poly);
        MultiPolygon ret = new MultiPolygon();
        
        boolean validSeg = false;
        double tol = 1.1 * poly.getSize() * scale;
        log.debug("[unscaleMultiPolygon] tol = " + tol);

        // or each vertex in poly, look for the scaled vertex in the list of input polygons
        // that is nearest and use the original unscaled vertex if it is close enough
        for (Vertex pv : poly.getVertices())
        {
            ret.getVertices().add(pv); // shallow copy
        }
        
        List<Vertex> verts = poly.getVertices();
        for (int i=0; i<verts.size(); i++)
        {
            Vertex pv = verts.get(i);
            if ( !SegmentType.CLOSE.equals(pv.getType()) )
            {
                ScaledVertex sv = (ScaledVertex) findNearest(pv, scaled);
                log.debug("[unscaleMultiPolygon] nearest: " + pv+ " " + sv);
                double d = Math.sqrt(distanceSquared(pv, sv));
                
                if (d < tol)
                {
                    // use orig coords but keep current segtype
                    Vertex v = new Vertex(sv.orig.cval1, sv.orig.cval2, pv.getType());
                    log.debug("[unscaleMultiPolygon] replace: " + pv + " -> " + v + " (d=" + d + ")");
                    ret.getVertices().set(i, v);
                    if (validSeg)
                    {
                        try { validateSegments(ret); }
                        catch(IllegalPolygonException oops)
                        {
                            log.debug("[unscaleMultiPolygon] REVERT: " + v + " -> " + pv);
                            ret.getVertices().set(i, pv); // undo
                        }
                    }
                }
                else
                {
                    log.debug("[unscaleMultiPolygon] KEEP: " + pv + " (d=" + d + ")");
                }
            }
        }
        log.debug("[unscaleMultiPolygon] done: " + ret);
        return ret;
    }

    private static Vertex findNearest(Vertex v, List<MultiPolygon> polys)
    {
        double d = Double.MAX_VALUE;
        Vertex ret = null;
        for (MultiPolygon p : polys)
        {
            for (Vertex pv : p.getVertices())
            {
                if ( !SegmentType.CLOSE.equals(pv.getType()) )
                {
                    double dd = distanceSquared(v, pv);
                    if (dd < d)
                    {
                        d = dd;
                        ret = pv;
                    }
                }
            }
        }
        return ret;
    }
    
    static CartesianTransform getTransform(MultiPolygon p1, MultiPolygon p2)
    {
        double[] cube = CartesianTransform.getBoundingCube(p1, null);
        cube = CartesianTransform.getBoundingCube(p2, cube);
        CartesianTransform trans = CartesianTransform.getTransform(cube, false);
        return trans;
    }

    private static class Segment implements Serializable
    {
        private static final long serialVersionUID = 201207300900L;
        
        Vertex v1;
        Vertex v2;
        Segment(Vertex v1, Vertex v2)
        {
            this.v1 = v1;
            this.v2 = v2;
        }
        
        double length()
        {
            return Math.sqrt(lengthSquared());
        }
        double lengthSquared()
        {
            return distanceSquared(v1, v2);
        }

        @Override
        public String toString()
        {
            return "("+v1+":"+v2+")";
        }
    }

    // validate a simple polygon (single loop) for intersecting segments
    // used by PositionUtil to validate CoordMultiPolygon2D
    static void validateSegments(MultiPolygon poly)
        throws IllegalPolygonException
    {
        CartesianTransform trans = CartesianTransform.getTransform(poly);
        MultiPolygon tpoly = trans.transform(poly);

        Iterator<Vertex> vi = tpoly.getVertices().iterator();
        List<Segment> tsegs = new ArrayList<Segment>();
        List<Segment> psegs = tsegs;
        Vertex start = vi.next();
        Vertex v1 = start;
        while ( vi.hasNext() )
        {
            Vertex v2 = vi.next();
            if (SegmentType.CLOSE.equals(v2.getType()))
                v2 = start;
            Segment s = new Segment(v1, v2);
            log.debug("[validateSegments] tsegs: " + s);
            tsegs.add(s);
            v1 = v2;
        }
        if (poly != tpoly)
        {
            // make segments with orig coords for reporting
            vi = poly.getVertices().iterator();
            psegs = new ArrayList<Segment>();
            start = (Vertex) vi.next();
            v1 = start;
            while ( vi.hasNext() )
            {
                Vertex v2 = vi.next();
                if (SegmentType.CLOSE.equals(v2.getType()))
                    v2 = start;
                Segment s = new Segment(v1, v2);
                //log.debug("[validateSegments] psegs: " + s);
                psegs.add(s);
                v1 = v2;
            }
        }

        for (int i=0; i<tsegs.size(); i++)
        {
            Segment s1 = tsegs.get(i);
            for (int j=0; j<tsegs.size(); j++)
            {
                if (i != j)
                {
                    Segment s2 = tsegs.get(j);
                    if ( intersects(s1, s2, true) )
                    {
                        Segment r1 = psegs.get(i);
                        Segment r2 = psegs.get(j);
                        throw new IllegalPolygonException("invalid polygon: " + r1 + " intersects " + r2);
                    }
                }
            }
        }
    }

    // ab.v2 == bc.v1
    private static boolean colinear(Segment ab, Segment bc, double da)
    {
        // determine dot-product of ba.bc since b is in the middle
        double v1x = (ab.v2.cval1 - ab.v1.cval1);
        double v1y = (ab.v2.cval2 - ab.v1.cval2);
        double v2x = (bc.v2.cval1 - bc.v1.cval1);
        double v2y = (bc.v2.cval2 - bc.v1.cval2);
        
        double ang2 = Math.atan2(v2y, v2x) - Math.atan2(v1y, v1x);
        log.debug("[colinear] ang2="+ang2);
        double ang = Math.abs(ang2);
        
        log.debug("[colinear] ang="+ang);
        if ( ang <= da) // parallel
            return true;
        
        // reduce
        while (ang > 0)
        {
            ang = ang - Math.PI;
            log.debug("[colinear] ang="+ang);
            if ( Math.abs(ang) <= da )
                return true;
        }
        
        return false;
    }
    
    
    private static boolean intersects(Segment ab, Segment cd, boolean isNext)
    {
        log.debug("intersects: " + ab + " vs " + cd);
        // rden = (Bx-Ax)(Dy-Cy)-(By-Ay)(Dx-Cx)
        double den = (ab.v2.cval1 - ab.v1.cval1)*(cd.v2.cval2 - cd.v1.cval2)
                - (ab.v2.cval2 - ab.v1.cval2)*(cd.v2.cval1 - cd.v1.cval1);
        //log.debug("den = " + den);

        //rnum = (Ay-Cy)(Dx-Cx)-(Ax-Cx)(Dy-Cy)
        double rnum = (ab.v1.cval2 - cd.v1.cval2)*(cd.v2.cval1 - cd.v1.cval1)
                - (ab.v1.cval1 - cd.v1.cval1)*(cd.v2.cval2 - cd.v1.cval2);
        //log.debug("rnum = " + rnum);

        if (Math.abs(den) < 1.0e-12) //(den == 0.0)
        {
            if (Math.abs(rnum) < 1.0e-12) //(rnum == 0.0)
            {
                // colinear: check overlap on one axis
                if (ab.v2 == cd.v1 || ab.v1 == cd.v2)
                    return false; // end-to-end
                double len1 = ab.lengthSquared();
                double len2 = cd.lengthSquared();
                Segment s = ab;
                if (len2 > len1)
                    s = cd; // the longer one
                double dx = Math.abs(s.v1.cval1 - s.v2.cval1);
                double dy = Math.abs(s.v1.cval2 - s.v2.cval2);
                if (dx > dy) // more horizontal = project to x
                {
                    if (ab.v2.cval1 < cd.v1.cval1)
                        return false; // ab left of cd
                    if (ab.v1.cval1 > cd.v2.cval1)
                        return false; // ab right of cd
                }
                else // more vertical = project to y
                {
                    if (ab.v2.cval2 < cd.v1.cval2)
                        return false; // ab below cd
                    if (ab.v1.cval2 > cd.v2.cval2)
                        return false; // ab above cd
                }
                return true; // overlapping
            }
            return false; // just parallel
        }

        double r = rnum / den;
        //log.debug("r = " + r);
        // no intersect, =0 or 1 means the ends touch, which is normal  but pg_sphere doesn't like it
        if (r <= 0.0 || r >= 1.0)
        //if (r < 0.0 || r > 1.0)
            return false;

        //snum = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
        double snum = (ab.v1.cval2 - cd.v1.cval2)*(ab.v2.cval1 - ab.v1.cval1)
                - (ab.v1.cval1 - cd.v1.cval1)*(ab.v2.cval2 - ab.v1.cval2);
        //log.debug("snum = " + snum);
        
        double s = snum / den;
        //log.debug("s = " + s);
        if (s <= 0.0 || s >= 1.0)
        //if (s < 0.0 || s > 1.0)
            return false; // no intersect, =0 or 1 means the ends touch, which is normal

        // r in [0,1] and s in [0,1] = intersects
        return true;
    }

    private static double distanceSquared(Vertex v1, Vertex v2)
    {
        return (v1.cval1 - v2.cval1)*(v1.cval1 - v2.cval1)
                + (v1.cval2 - v2.cval2)*(v1.cval2 - v2.cval2);
    }
    
    // from here down: interfaces to "external" libraries with alternate data structures
    
    // use java.awt CAG implementation for 2D cartesian geometry
    public static MultiPolygon doUnionCAG(MultiPolygon p1, MultiPolygon p2)
    {
        Area a1 = toArea(p1);
        Area a2 = toArea(p2);
        a1.add(a2);
        MultiPolygon ret = toMultiPolygon(a1);
        return ret;
    }

    // use java.awt CAG implementation for 2D cartesian geometry
    static MultiPolygon doIntersectCAG(MultiPolygon p1, MultiPolygon p2)
    {
        Area a1 = toArea(p1);
        Area a2 = toArea(p2);
        a1.intersect(a2);
        MultiPolygon ret = toMultiPolygon(a1);
        if (ret.getVertices().isEmpty())
            return null;
        return ret;
    }

    static MultiPolygon toMultiPolygon(Area area)
    {
        MultiPolygon ret = new MultiPolygon();
        List<Vertex> verts = ret.getVertices();

        PathIterator pi = area.getPathIterator(null);

        double[] prev = null;
        int type;
        while (!pi.isDone())
        {
            double[] coords = new double[2];
            type = pi.currentSegment(coords);

            if (type == PathIterator.SEG_MOVETO)
            {
                verts.add(new Vertex(coords[0], coords[1], SegmentType.MOVE));
            }
            else if (type == PathIterator.SEG_LINETO)
            {
                verts.add(new Vertex(coords[0], coords[1], SegmentType.LINE));
            }
            else if (type == PathIterator.SEG_CLOSE)
            {
                verts.add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            }
            else
            {
                throw new IllegalStateException("unexpected PathIterator segment type: " + type);
            }
            prev = coords;
            pi.next();
        }
        return ret;
    }

    // support java.awt rendering
    public static GeneralPath toGeneralPath(MultiPolygon poly)
    {
        GeneralPath gp = new GeneralPath();
        for (Vertex v : poly.getVertices())
        {
            if (SegmentType.MOVE.equals(v.getType()))
                gp.moveTo((float) v.cval1, (float) v.cval2);
            else if (SegmentType.LINE.equals(v.getType()))
                gp.lineTo((float) v.cval1, (float) v.cval2);
            else if (SegmentType.CLOSE.equals(v.getType()))
                gp.closePath();
            else
                throw new IllegalStateException("BUG: unexpected segment type: " + v.getType());
        }
        return gp;
    }
    
    // support java.awt rendering
    public static Area toArea(MultiPolygon poly)
    {
        return new Area(toGeneralPath(poly));
    }
    
    // transform MultiPolygon and call GrahamScan algorithm
    private static MultiPolygon computeConvexHull(MultiPolygon poly)
    {
        // copy and strip out CLOSE
        List<SortablePoint2D> tmp = new ArrayList<SortablePoint2D>();
        for (Vertex v : poly.getVertices())
        {
            if (!SegmentType.CLOSE.equals(v.getType()))
                tmp.add(new SortablePoint2D(v.cval1, v.cval2));
        }
        SortablePoint2D[] points = (SortablePoint2D[]) tmp.toArray(new SortablePoint2D[tmp.size()]);
        
        MultiPolygon ret = new MultiPolygon();
        SegmentType t = SegmentType.MOVE;
        GrahamScan gs = new GrahamScan(points);
        for (SortablePoint2D p : gs.hull())
        {
            ret.getVertices().add(new Vertex(p.x(), p.y(), t));
            t = SegmentType.LINE;
        }
        ret.getVertices().add(Vertex.CLOSE);
        
        return ret;
    }
    
    
}
