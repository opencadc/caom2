
package ca.nrc.cadc.caom2.types;

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
    private static final double MAX_SCALE = 0.12;

    public static Polygon toPolygon(Shape s)
    {
        if (s == null)
            return null;

        if (s instanceof Polygon)
            return (Polygon) s;

        throw new UnsupportedOperationException(s.getClass().getSimpleName() + " -> Polygon");
    }

    /**
     * Compute a simple concave outer hull of the specified polygon.This method
     * is simple a wrapper that calls getConcaveHull and if that fails, it then
     * tries getConvexHull.
     *
     * @param poly
     * @return a simple bounding polygon
     */
    public static Polygon getOuterHull(final Polygon poly)
    {
        log.debug("[getOuterHull] " + poly);
        Polygon ret = getConcaveHull(poly);
        if (ret == null)
            ret = getConvexHull(poly);
        return ret;
    }

    /**
     * Compute a simple concave outer hull of the specified polygon. This method
     * removes holes and finds a single concave polygon that includes all the area
     * of the original (but usually more).
     *
     * @param poly
     * @return concave hull or null of not possible
     */
    public static Polygon getConcaveHull(final Polygon poly)
    {
        log.debug("[getConcaveHull] " + poly);
        if (poly == null)
            return null;
        
        // deep copy
        Polygon outer = new Polygon();
        for (Vertex v : poly.getVertices())
        {
            outer.getVertices().add(new Vertex(v.cval1, v.cval2, v.getType()));
        }

        double scale = DEFAULT_SCALE;
        List<Polygon> parts = removeHoles(outer);
        while ( parts.size() > 1 && scale <= MAX_SCALE)
        {
            log.debug("[getConcaveHull] trying union at scale=" + scale + " with " + parts.size() + " simple polygons");
            outer = union(parts, scale);
            log.debug("[getConcaveHull] union = " + outer);
            scale += DEFAULT_SCALE; // 2x, 3x, etc
            parts = removeHoles(outer);
            log.debug("");
        }
        if (parts.size() == 1)
        {
            outer = parts.get(0);
            log.debug("[getConcaveHull] SUCCESS: " + outer);
            return outer;
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
    public static Polygon getConvexHull(final Polygon poly)
    {
        log.debug("[getConvexHull] " + poly);
        if (poly == null)
            return null;

        // deep copy
        //Polygon outer = new Polygon();
        //for (Vertex v : poly.getVertices())
        //{
        //    outer.getVertices().add(new Vertex(v.cval1, v.cval2, v.getType()));
        //}

        // TODO: triangulate and find union of all triangles
        // technically just need to add triangles that have one vertex in one part and two in
        // the other part (eg that join two parts)
        throw new UnsupportedOperationException("getConvexHull: not implemented");
    }

    public static Polygon union(List<Polygon> polys)
    {
        return union(polys, DEFAULT_SCALE);
    }

    public static Polygon union(List<Polygon> polys, double scale)
    {
        if (polys.size() == 1)
            return polys.get(0);
        
        // transform the input polygons to some other 2D coordinates where cartesian CAG is good enough
        double[] cube = null;
        for (Polygon p : polys)
        {
            cube = CartesianTransform.getBoundingCube(p, cube);
        }
        CartesianTransform trans = CartesianTransform.getTransform(cube, false);
        log.debug("working transform: " + trans);
        
        // transform all the polygons
        List<Polygon> work = new ArrayList<Polygon>(polys.size());
        for (Polygon p : polys)
        {
            work.add(trans.transform(p));
        }

        // compute union without scaling
        Polygon union = computeUnion(work, scale);

        // inverse transform back to longitude,latitude
        union = trans.inverseTransform(union);

        return union;
    }

    public static Polygon intersection(Polygon p1, Polygon p2)
    {
        double[] cube = CartesianTransform.getBoundingCube(p1, null);
        cube = CartesianTransform.getBoundingCube(p2, cube);

        CartesianTransform trans = CartesianTransform.getTransform(cube, false);

        Polygon pt1 = trans.transform(p1);
        Polygon pt2 = trans.transform(p2);

        Polygon inter = doIntersectCAG(pt1, pt2);
        
        if (inter != null)
            inter = trans.inverseTransform(inter);

        return inter;
    }

    // assumes cartesian approx is safe
    private static Polygon computeUnion(List<Polygon> work, double scale)
    {
        log.debug("[computeUnion] work=" + work.size() + ", scale="+scale);
        // scale all the polygons up to cause nearby polygons to intersect
        List<Polygon> scaled = work;
        if (scale > 0.0)
        {
            scaled = new ArrayList<Polygon>(work.size());
            for (Polygon p : work)
            {
                scaled.add(scalePolygon(p, scale));
            }
        }

        Polygon poly = null;
        for (Polygon p : scaled)
        {
            if (poly == null)
                poly = p;
            else
                poly = doUnionCAG(poly, p);
        }

        removeHoles(poly, 1.0);

        if (scale > 0.0)
            poly = unscalePolygon(poly, scaled);

        smooth(poly);
        //smooth(poly, 1.0e-4);

        return poly;
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
    
    private static Polygon scalePolygon(Polygon poly, double factor)
    {
        log.debug("[scalePolygon] start: " + poly + " BY " + factor);
        Polygon ret = new Polygon();
        PolygonProperties pp = computePolygonProperties(poly);
        Point c = pp.center;
        for (Vertex v : poly.getVertices())
        {
            if (SegmentType.CLOSE.equals(v.getType()))
            {
                Vertex sv = new ScaledVertex(0.0, 0.0, v.getType(), v);
                ret.getVertices().add(sv);
            }
            else
            {
                double dx = (v.cval1 - c.cval1) * factor;
                double dy = (v.cval2 - c.cval2) * factor;
                Vertex sv = new ScaledVertex(v.cval1 + dx, v.cval2 + dy, v.getType(), v);
                ret.getVertices().add(sv);
            }
        }
        log.debug("[scalePolygon] done: " + ret);
        return ret;
    }
    private static Polygon unscalePolygon(Polygon poly, List<Polygon> scaled)
    {
        log.debug("[unscalePolygon] start: " + poly);
        Polygon ret = new Polygon();
        
        boolean validSeg = false;
        double tol = 0.5* poly.getSize() * DEFAULT_SCALE;
        log.debug("[unscalePolygon] tol = " + tol);

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
                log.debug("[unscalePolygon] nearest: " + pv+ " " + sv);
                double d = Math.sqrt(distanceSquared(pv, sv));
                
                if (d < tol)
                {
                    // use orig coords but keep current segtype
                    Vertex v = new Vertex(sv.orig.cval1, sv.orig.cval2, pv.getType());
                    log.debug("[unscalePolygon] replace: " + pv + " -> " + v + " (d=" + d + ")");
                    ret.getVertices().set(i, v);
                    if (validSeg)
                    {
                        try { validateSegments(ret); }
                        catch(IllegalPolygonException oops)
                        {
                            log.debug("[unscalePolygon] REVERT: " + v + " -> " + pv);
                            ret.getVertices().set(i, pv); // undo
                        }
                    }
                }
                else
                {
                    log.debug("[unscalePolygon] keep: " + pv + " (d=" + d + ")");
                //    Vertex v = new Vertex(pv.cval1, pv.cval2, pv.getType());
                //    ret.getVertices().add(v);
                }
            }
        }
        // TODO: could track which vertices were replaced and undo it if validateSegments proves
        // we created an invalid polygon
        log.debug("[unscalePolygon] done: " + ret);
        return ret;
    }

    private static Vertex findNearest(Vertex v, List<Polygon> polys)
    {
        double d = Double.MAX_VALUE;
        Vertex ret = null;
        for (Polygon p : polys)
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

    static List<Polygon> removeHoles(Polygon poly)
    {
        log.debug("[removeHoles] start: " + poly);
        List<Polygon> samples = decompose(poly);
        // find samples and holes via sign of the area
        boolean cw = computePolygonProperties(poly).winding;
        ListIterator<Polygon> iter = samples.listIterator();
        int num = 0;
        while ( iter.hasNext() )
        {
            Polygon part = iter.next();
            boolean pcw = computePolygonProperties(part).winding;
            if (cw != pcw) // opposite sign
            {
                iter.remove();
                num++;
            }
        }
        if (num > 0)
            log.debug("[removeHoles] discarded " + num + " holes");
        
        poly.getVertices().clear();
        for (Polygon p : samples)
        {
            poly.getVertices().addAll(p.getVertices());
        }
        log.debug("[removeHoles] done: " + poly);
        return samples;
    }
    
    // removes all holes with fractional area less than rat
    static boolean removeHoles(Polygon poly, double rat)
    {
        if (poly.getVertices().size() <= 8)
            return false; // cannot have holes with fewer than 8 vertices (two triangles and 2 CLOSE)

        boolean hasHoles = false;
        log.debug("[removeHoles] start: " + poly);
        PolygonProperties pProp = computePolygonProperties(poly);
        Polygon tmp = new Polygon();
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
                        log.debug("[removeHoles] remove scrap segment " + tmp + " from " + poly);
                        for (Vertex rv : tmp.getVertices())
                        {
                            boolean ok = poly.getVertices().remove(rv);
                            if (!ok)
                                log.warn("[removeHoles] found hole " + tmp + " but failed to remove " + v + " from " + poly);
                        }
                        go = true;
                        restart = true;
                    }
                    else
                    {
                        // closed loop
                        PolygonProperties tProp = computePolygonProperties(tmp);
                        double da = tProp.area/pProp.area;
                        boolean isHole = (tProp.winding != pProp.winding); // opposite winding
                        hasHoles = isHole;
                        if (isHole && da < rat) // hole && small
                        {
                            // remove all these verts from list
                            //log.debug("[removeHoles] removing hole: " + tmp + " with da = " + da);
                            //log.debug("removing vertex " + start + " " + tmp.getVertices().size() + " times");
                            //for (int j=0; j<tmp.getVertices().size(); j++)
                            //    pVerts.remove(start);
                            log.debug("[removeHoles] remove hole " + tmp + " from " + poly);
                            for (Vertex rv : tmp.getVertices())
                            {
                                boolean ok = poly.getVertices().remove(rv);
                                if (!ok)
                                    log.warn("[removeHoles] found hole " + tmp + " but failed to remove " + v + " from " + poly);
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
        log.debug("[removeHoles] done: " + poly);
        return hasHoles;
    }

    private static void smooth(Polygon poly)
    {
        List<Polygon> parts = decompose(poly);
        poly.getVertices().clear();
        for (Polygon p : parts)
        {
            smoothSimpleAdjacentVertices(p);
            smoothSimpleColinearSegments(p);
            for (Vertex v : p.getVertices())
            {
                poly.getVertices().add(v);
            }
        }
    }
    
    // currently unused by computeUnion
    private static void smooth(Polygon poly, double rat)
    {
        List<Polygon> parts = decompose(poly);
        poly.getVertices().clear();
        for (Polygon p : parts)
        {
            for (Vertex v : p.getVertices())
            {
                poly.getVertices().add(v);
            }
        }
    }
    
    private static void smoothSimpleAdjacentVertices(Polygon poly)
    {
        log.debug("[smooth.adjacent] " + poly);
        if (poly.getVertices().size() <= 4) // 3+close == triangle
            return;
        PolygonProperties pp = computePolygonProperties(poly);
        
        double tol = pp.minSpanCircle.getSize()*1.0e-6;
        
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
        
        // use length of segments to identify vertices we can remove
        for (int i=0; i<segs.size(); i++)
        {
            Segment ab = segs.get(i);
            double dx = Math.abs(ab.v1.cval1 - ab.v2.cval1);
            double dy = Math.abs(ab.v1.cval2 - ab.v2.cval2);
            log.debug("[smooth.adjacent] " + ab + " " + dx + " " + dy);
            if ( dx < tol && dy < tol) // 
            {
                final Vertex v = ab.v1;
                log.debug("[smooth.adjacent] removing " + v);
                // remove v from poly
                poly.getVertices().remove(v); // Vertex uses Object.equals aka ==
                if (SegmentType.MOVE.equals(v.getType()))
                {
                    // change v+1 to MOVE
                    final Vertex nv = new Vertex(ab.v2.cval1, ab.v2.cval2, SegmentType.MOVE);
                    // ugly List.replace(old, new): find index and set
                    int curi = 0;
                    while (ab.v2 != poly.getVertices().get(curi))
                        curi++;
                    poly.getVertices().set(curi, nv);
                    /*
                    poly.getVertices().replaceAll(new UnaryOperator<Vertex>()
                    {

                        public Vertex apply(Vertex t)
                        {
                            Vertex ret = t;
                            if (vp1 == t) // yes: really ==
                                return nv;
                            return ret;
                        }
                    });
                    */
                }
            }
        }
    }
    
    private static void smoothSimpleColinearSegments(Polygon poly)
    {
        log.debug("[smooth.colinear] " + poly);
        if (poly.getVertices().size() <= 4) // 3+close == triangle
            return;
        PolygonProperties pp = computePolygonProperties(poly);
        
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
        
        // remove colinear segments
        for (int i=0; i<segs.size() - 1; i++)
        {
            Segment ab = segs.get(i);
            Segment cd = segs.get(i+1);
            if ( colinear(ab, cd) )
            {
                // vertex b aka c is on a line between a and d: remove it
                final Vertex v = ab.v2; // == cd.v1
                log.debug("[smooth.colinear] " + ab + " + " + cd + ": removing " + v);
                
                // remove v from poly
                poly.getVertices().remove(v);
                
                if (SegmentType.MOVE.equals(v.getType()))
                {
                    // change v+1 to MOVE
                    final Vertex nv = new Vertex(cd.v2.cval1, cd.v2.cval2, SegmentType.MOVE);
                    // ugly List.replace(old, new): find index and set
                    int curi = 0;
                    while (cd.v2 != poly.getVertices().get(curi))
                        curi++;
                    poly.getVertices().set(curi, nv);
                    /*
                    poly.getVertices().replaceAll(new UnaryOperator<Vertex>()
                    {

                        public Vertex apply(Vertex t)
                        {
                            Vertex ret = t;
                            if (vp1 == t) // yes: really ==
                                return nv;
                            return ret;
                        }
                    });
                    */
                }
            }
            else
                log.debug("[smooth.colinear] non-colinear: " + ab + " + " + cd);
        }
        
    }
    
    private static void smoothSimpleSmallAreaChange(Polygon poly, double rat)
    {
        log.debug("[smooth] " + poly + " rat="+rat);
        if (poly.getVertices().size() <= 4) // 3+close == triangle
            return;
        PolygonProperties pp = computePolygonProperties(poly);
        Polygon tmp = new Polygon();
        List<Vertex> verts = tmp.getVertices();
        boolean changed = true;
        while (changed)
        {
            int posI = -1;
            int negI = -1;
            double posDA = Double.MAX_VALUE;
            double negDA = Double.MAX_VALUE;
            Vertex posV = null;
            Vertex negV = null;
            log.debug("starting loop over vertices...");
            for (int i=0; i<poly.getVertices().size(); i++)
            {
                verts.clear();
                verts.addAll(poly.getVertices());
                Vertex v = tmp.getVertices().get(i);
                log.debug("[smooth] (try) " + v);
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
                            log.debug("[smooth] (try) change " + vl + " -> " + vm);
                            verts.set(i, vm);
                        }
                        else
                            throw new IllegalStateException("found " + vl + " after " + v);
                    }
                    PolygonProperties tp = computePolygonProperties(tmp);
                    double da = (tp.area - pp.area)/pp.area;
                    log.debug("[smooth] (try) remove " + v + ", dA = " + da + ", " + pp.area + " -> " + tp.area);
                    if (da >= 0.0)
                    {
                        if (da < posDA)
                        {
                            posDA = da;
                            posV = v;
                            posI = i;
                        }
                    }
                    else // da < 0.0
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
            if (posV != null && posDA <= negDA && posDA < rat)
            {
                log.debug("[smooth] dA = " + posDA + " remove " + posV);
                poly.getVertices().remove(posV);
                if ( SegmentType.MOVE.equals(posV.getType()) )
                {
                    // change the next from  LINE to MOVE
                    Vertex vl = poly.getVertices().get(posI);
                    Vertex vm = new Vertex(vl.cval1, vl.cval2, SegmentType.MOVE);
                    log.debug("[smooth] change " + vl + " -> " + vm);
                    poly.getVertices().set(posI, vm);
                }
                changed = true;
            }
            else if (negV != null && negDA < rat)
            {
                log.debug("[smooth] dA = -" + negDA + " remove " + negV);
                poly.getVertices().remove(negV);
                if ( SegmentType.MOVE.equals(negV.getType()) )
                {
                    // change the next from  LINE to MOVE
                    Vertex vl = poly.getVertices().get(negI);
                    Vertex vm = new Vertex(vl.cval1, vl.cval2, SegmentType.MOVE);
                    log.debug("[smooth] change " + vl + " -> " + vm);
                    poly.getVertices().set(negI, vm);
                }
                changed = true;
            }
            else
            {
                if (negV != null) log.debug("[smooth] negDA = -" + negDA + " keep " + negV);
                if (posV != null) log.debug("[smooth] posDA = " + posDA + " keep " + posV);
            }
        }
    }
    
    /**
     * Decompose a polygon into one or more simple polygons. Note that disjoint
     * sections and holes are both included in the output list.
     * 
     * @param p
     * @return
     */
    public static List<Polygon> decompose(Polygon p)
    {
        List<Polygon> polys = new ArrayList<Polygon>();
        Polygon prev = null; // needs to be a stack?
        Polygon cur = null;
        for (Vertex v : p.getVertices())
        {
            if (cur == null) // start new poly
            {
                log.debug("new Polygon");
                cur = new Polygon();
                log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
            else if (SegmentType.MOVE.equals(v.getType()))
            {
                //log.debug("vertex: " + v);
                //cur.getVertices().add(new Vertex(0.0, 0.0,SegmentType.CLOSE));
                //polys.add(cur);
                //log.debug("close Polygon");
                if (cur != null)
                    prev = cur; // embedded loop

                log.debug("new Polygon");
                cur = new Polygon();
                log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
            else if (SegmentType.CLOSE.equals(v.getType()))
            {
                log.debug("vertex: " + v);
                cur.getVertices().add(v);
                polys.add(cur);
                cur = prev;
                log.debug("close Polygon");
            }
            else
            {
                log.debug("vertex: " + v);
                cur.getVertices().add(v);
            }
        }
        return polys;
    }

    static class PolygonProperties implements Serializable
    {
        private static final long serialVersionUID = 201207300900L;
        
        boolean winding;
        Double area;
        Point center;
        Circle minSpanCircle;
    }
    
    // lazy computation of center, area, and size
    // returns the signed area
    static PolygonProperties computePolygonProperties(Polygon poly)
    {
        // the transform needed for computing things in long/lat using cartesian approximation
        CartesianTransform trans = CartesianTransform.getTransform(poly);
        Polygon tpoly = trans.transform(poly);
       
        // algorithm from
        // http://astronomy.swin.edu.au/~pbourke/geometry/polyarea/
        double a = 0.0;
        double cx = 0.0;
        double cy = 0.0;
        int lastMoveTo = 0;
        for (int i = 0; i < tpoly.getVertices().size(); i++)
        {
            Vertex v2 = (Vertex) tpoly.getVertices().get(i);
            if (SegmentType.MOVE.equals(v2.getType()))
                lastMoveTo = i;
            else
            {
                Vertex v1 = (Vertex) tpoly.getVertices().get(i-1);
                if (SegmentType.CLOSE.equals(v2.getType()))
                    // pretend it is a LINE_TO the lastMoveTo vertex
                    v2 = (Vertex) tpoly.getVertices().get(lastMoveTo);

                double tmp = v1.cval1 * v2.cval2 - v2.cval1 * v1.cval2;
                a += tmp;
                cx += (v1.cval1 + v2.cval1) * tmp;
                cy += (v1.cval2 + v2.cval2) * tmp;
                //log.debug("[computePolygonProperties] " + v1 + "," + v2 + "," + tmp + " " + cx + "," + cy);
            }
        }

        a *= 0.5;
        cx = cx / (6.0 * a);
        cy = cy / (6.0 * a);

        // quick and dirty minimum spanning circle computation
        double d = 0.0;
        Vertex e1 = null;
        Vertex e2 = null;
        for (int i = 0; i < tpoly.getVertices().size(); i++)
        {
            Vertex vi = (Vertex) tpoly.getVertices().get(i);
            if (!SegmentType.CLOSE.equals(vi.getType()))
            {
                for (int j = i+1; j < tpoly.getVertices().size(); j++)
                {
                    Vertex vj = (Vertex) tpoly.getVertices().get(j);
                    if (!SegmentType.CLOSE.equals(vj.getType()))
                    {
                        double d1 = vi.cval1 - vj.cval1;
                        double d2 = vi.cval2 - vj.cval2;
                        double dd = Math.sqrt(d1*d1 + d2*d2);
                        if (dd > d)
                        {
                            d = dd;
                            e1 = vi;
                            e2 = vj;
                            
                        }
                    }
                }
            }
        }
        
        PolygonProperties ret = new PolygonProperties();
        ret.winding = (a < 0.0); // arbitrary
        if (a < 0.0) a *= -1.0;
        ret.area = new Double(a);
        ret.center = trans.inverseTransform(new Point(cx, cy));
        
        // midpoint between vertices
        if (e1 != null && e2 != null && d > 0.0)
        {
            log.warn("MSC raw: " + e1 + " -- " + e2);
            log.warn("MSC inv: " + trans.inverseTransform(e1) + " -- " + trans.inverseTransform(e2));
            Point cen = new Point(0.5*Math.abs(e1.cval1 + e2.cval1), 0.5*Math.abs(e1.cval2 + e2.cval2));
            Point mscc = trans.inverseTransform(cen);
            log.warn("MSC center raw: " +  cen);
            log.warn("MSC center inv:" + mscc);
            ret.minSpanCircle = new Circle(mscc, d/2.0);
        }
        
        
        return ret;
    }

    
    static CartesianTransform getTransform(Polygon p1, Polygon p2)
    {
        CartesianTransform trans1 = CartesianTransform.getTransform(p1);
        CartesianTransform trans = CartesianTransform.getTransform(p2);
        if (trans.isNull())
            trans = trans1;
        else if (!trans1.isNull() && !trans.isNull())
        {
            if (trans1.axis.equals(trans.axis))
                trans.a = (trans1.a + trans.a) / 2.0; // mean rotation
            else if (trans1.axis.equals(CartesianTransform.Y)) // prefer Y over Z
                trans = trans1;
        }
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

        @Override
        public String toString()
        {
            return "("+v1+":"+v2+")";
        }
    }

    // validate a simple polygon (single loop) for intersecting segments
    static void validateSegments(Polygon poly)
        throws IllegalPolygonException
    {
        CartesianTransform trans = CartesianTransform.getTransform(poly);
        Polygon tpoly = trans.transform(poly);

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
            log.debug("validateSegments: " + s);
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
                log.debug("validateSegments: " + s);
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

    private static boolean colinear(Segment ab, Segment cd)
    {
        // determine dot-product 
        double v1x = (ab.v2.cval1 - ab.v1.cval1);
        double v1y = (ab.v2.cval2 - ab.v1.cval2);
        double v2x = (cd.v2.cval1 - cd.v1.cval1);
        double v2y = (cd.v2.cval2 - cd.v1.cval2);
        
        double dp = v1x*v2x + v1y*v2y;
        double magAB = Math.sqrt(v1x*v1x + v1y*v1y);
        double magCD = Math.sqrt(v2x*v2x + v2y*v2y);
        double ang = Math.acos(dp/(magAB*magCD));
        
        log.debug("colinear: dot.product = " + ang);
        if ( Math.abs(ang) < 0.05
                || Math.abs(Math.PI - ang) < 0.05 )
            return true;
        
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
                double len1 = lengthSquared(ab);
                double len2 = lengthSquared(cd);
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
    private static double lengthSquared(Segment s)
    {
        return distanceSquared(s.v1, s.v2);
    }

    // use java.awt CAG implementation for 2D cartesian geometry
    static Polygon doUnionCAG(Polygon p1, Polygon p2)
    {
        Area a1 = toArea(p1);
        Area a2 = toArea(p2);
        a1.add(a2);
        Polygon ret = toPolygon(a1);
        return ret;
    }

    // use java.awt CAG implementation for 2D cartesian geometry
    static Polygon doIntersectCAG(Polygon p1, Polygon p2)
    {
        Area a1 = toArea(p1);
        Area a2 = toArea(p2);
        a1.intersect(a2);
        Polygon ret = toPolygon(a1);
        if (ret.getVertices().isEmpty())
            return null;
        return ret;
    }

    static Polygon toPolygon(Area area)
    {
        Polygon ret = new Polygon();
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

    static GeneralPath toGeneralPath(Polygon poly)
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
    static Area toArea(Polygon poly)
    {
        return new Area(toGeneralPath(poly));
    }
}
