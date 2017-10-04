/**
 * Origin: http://algs4.cs.princeton.edu/99hull/Point2D.java
 * Copyright: not specified
 * License: not specified
 */

package ca.nrc.cadc.caom2.compute.convex;

import ca.nrc.cadc.caom2.types.impl.SortablePoint2D;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Stack;

/**
 * The <tt>GrahamScan</tt> data type provides methods for computing the convex hull of a set of <em>N</em> points in the plane.
 * 
 * <p>The implementation uses the Graham-Scan convex hull algorithm. It runs in O(<em>N</em> log <em>N</em>) time in the worst case and uses O(<em>N</em>) extra
 * memory.
 * 
 * <p>For additional documentation, see <a href="http://algs4.cs.princeton.edu/99scientific">Section 9.9</a> of <i>Algorithms, 4th Edition</i> by Robert Sedgewick
 * and Kevin Wayne.
 *
 * @author Robert Sedgewick
 * @author Kevin Wayne
 */
public class GrahamScan implements Serializable {
    private static final long serialVersionUID = 201603031530L;
    private Stack<SortablePoint2D> hull = new Stack<SortablePoint2D>();

    /**
     * Computes the convex hull of the specified array of points.
     *
     * @param pts
     *            the array of points
     * @throws NullPointerException
     *             if <tt>points</tt> is <tt>null</tt> or if any entry in <tt>points[]</tt> is <tt>null</tt>
     */
    public GrahamScan(SortablePoint2D[] pts) {

        // defensive copy
        int ptsLength = pts.length;
        SortablePoint2D[] points = new SortablePoint2D[ptsLength];
        for (int i = 0; i < ptsLength; i++) {
            points[i] = pts[i];
        }

        // preprocess so that points[0] has lowest coordY-coordinate; break ties by
        // coordX-coordinate
        // points[0] is an extreme point of the convex hull
        // (alternatively, could do easily in linear time)
        Arrays.sort(points);

        // sort by polar angle with respect to base point points[0],
        // breaking ties by distance to points[0]
        Arrays.sort(points, 1, ptsLength, points[0].polarOrder());

        hull.push(points[0]); // p[0] is first extreme point

        // find index k1 of first point not equal to points[0]
        int k1;
        for (k1 = 1; k1 < ptsLength; k1++) {
            if (!points[0].equals(points[k1])) {
                break;
            }
        }
        if (k1 == ptsLength) {
            return; // all points equal
        }

        // find index k2 of first point not collinear with points[0] and
        // points[k1]
        int k2;
        for (k2 = k1 + 1; k2 < ptsLength; k2++) {
            if (SortablePoint2D.ccw(points[0], points[k1], points[k2]) != 0) {
                break;
            }
        }
        hull.push(points[k2 - 1]); // points[k2-1] is second extreme point

        // Graham scan; note that points[N-1] is extreme point different from
        // points[0]
        for (int i = k2; i < ptsLength; i++) {
            SortablePoint2D top = hull.pop();
            while (SortablePoint2D.ccw(hull.peek(), top, points[i]) <= 0) {
                top = hull.pop();
            }
            hull.push(top);
            hull.push(points[i]);
        }

        assert isConvex();
    }

    /**
     * Returns the extreme points on the convex hull in counterclockwise order.
     *
     * @return the extreme points on the convex hull in counterclockwise order
     */
    public Iterable<SortablePoint2D> hull() {
        Stack<SortablePoint2D> s = new Stack<SortablePoint2D>();
        for (SortablePoint2D p : hull) {
            s.push(p);
        }
        return s;
    }

    // check that boundary of hull is strictly convex
    private boolean isConvex() {
        int hullSize = hull.size();
        if (hullSize <= 2) {
            return true;
        }

        SortablePoint2D[] points = new SortablePoint2D[hullSize];
        int n = 0;
        for (SortablePoint2D p : hull()) {
            points[n++] = p;
        }

        for (int i = 0; i < hullSize; i++) {
            if (SortablePoint2D.ccw(points[i], points[(i + 1) % hullSize], points[(i + 2) % hullSize]) <= 0) {
                return false;
            }
        }
        return true;
    }
}
