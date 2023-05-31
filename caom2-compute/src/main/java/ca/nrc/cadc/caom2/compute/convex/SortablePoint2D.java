/**
 * Origin: http://algs4.cs.princeton.edu/99hull/Point2D.java
 * Copyright: not specified
 * License: not specified
 */

package ca.nrc.cadc.caom2.compute.convex;

import java.io.Serializable;
import java.util.Comparator;


/**
 * The SortablePoint2D class is an immutable data type to encapsulate a
 * two-dimensional point with real-value coordinates.
 * <p>
 * Note: in order to deal with the difference behavior of double and
 * Double with respect to -0.0 and +0.0, the SortablePoint2D constructor converts
 * any coordinates that are -0.0 to +0.0.</p>
 * <p>
 * For additional documentation,
 * see <a href="http://algs4.cs.princeton.edu/12oop">Section 1.2</a> of
 * <i>Algorithms, 4th Edition</i> by Robert Sedgewick and Kevin Wayne.</p>
 *
 * @author Robert Sedgewick
 * @author Kevin Wayne
 */
public final class SortablePoint2D implements Comparable<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
    /**
     * Compares two points by coordX-coordinate.
     */
    public static final Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> X_ORDER = new XOrder();
    /**
     * Compares two points by coordY-coordinate.
     */
    public static final Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> Y_ORDER = new YOrder();
    /**
     * Compares two points by polar radius.
     */
    public static final Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> R_ORDER = new ROrder();
    private static final long serialVersionUID = 201603031530L;
    private final double coordX;
    private final double coordY;

    /**
     * Initializes a new point (coordX, coordY).
     *
     * @param x the coordX-coordinate
     * @param y the coordY-coordinate
     * @throws IllegalArgumentException if either coordX or coordY
     *                                  is Double.NaN, Double.POSITIVE_INFINITY or
     *                                  Double.NEGATIVE_INFINITY
     */
    public SortablePoint2D(double x, double y) {
        if (Double.isInfinite(x) || Double.isInfinite(y)) {
            throw new IllegalArgumentException("Coordinates must be finite");
        }
        if (Double.isNaN(x) || Double.isNaN(y)) {
            throw new IllegalArgumentException("Coordinates cannot be NaN");
        }
        if (x == 0.0) {
            this.coordX = 0.0;  // convert -0.0 to +0.0
        } else {
            this.coordX = x;
        }

        if (y == 0.0) {
            this.coordY = 0.0;  // convert -0.0 to +0.0
        } else {
            this.coordY = y;
        }
    }

    /**
     * Returns true if a-&gt;b-&gt;c is a counterclockwise turn.
     *
     * @param a first point
     * @param b second point
     * @param c third point
     * @return { -1, 0, +1 } if a-&gt;b-&gt;c is a { clockwise, collinear; counterclocwise } turn.
     */
    public static int ccw(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D a,
                          ca.nrc.cadc.caom2.compute.convex.SortablePoint2D b,
                          ca.nrc.cadc.caom2.compute.convex.SortablePoint2D c) {
        double area2 = (b.coordX - a.coordX) * (c.coordY - a.coordY) - (b.coordY - a.coordY) * (c.coordX - a.coordX);
        if (area2 < 0) {
            return -1;
        } else if (area2 > 0) {
            return +1;
        } else {
            return 0;
        }
    }

    /**
     * Returns twice the signed area of the triangle a-b-c.
     *
     * @param a first point
     * @param b second point
     * @param c third point
     * @return twice the signed area of the triangle a-b-c
     */
    public static double area2(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D a,
                               ca.nrc.cadc.caom2.compute.convex.SortablePoint2D b,
                               ca.nrc.cadc.caom2.compute.convex.SortablePoint2D c) {
        return (b.coordX - a.coordX) * (c.coordY - a.coordY) - (b.coordY - a.coordY) * (c.coordX - a.coordX);
    }

    /**
     * Returns the x-coordinate.
     *
     * @return the x-coordinate
     */
    public double getX() {
        return coordX;
    }

    /**
     * Returns the coordY-coordinate.
     *
     * @return the coordY-coordinate
     */
    public double getY() {
        return coordY;
    }

    /**
     * Returns the polar radius of this point.
     *
     * @return the polar radius of this point in polar coordiantes: sqrt(coordX*coordX + coordY*coordY)
     */
    public double radius() {
        return Math.sqrt(coordX * coordX + coordY * coordY);
    }

    /**
     * Returns the angle of this point in polar coordinates.
     *
     * @return the angle (in radians) of this point in polar coordiantes (between -pi/2 and pi/2)
     */
    public double theta() {
        return Math.atan2(coordY, coordX);
    }

    /**
     * Returns the angle between this point and that point.
     *
     * @return the angle in radians (between -pi and pi) between this point and that point (0 if equal)
     */
    private double angleTo(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D that) {
        double dx = that.coordX - this.coordX;
        double dy = that.coordY - this.coordY;
        return Math.atan2(dy, dx);
    }

    /**
     * Returns the Euclidean distance between this point and that point.
     *
     * @param that the other point
     * @return the Euclidean distance between this point and that point
     */
    public double distanceTo(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D that) {
        double dx = this.coordX - that.coordX;
        double dy = this.coordY - that.coordY;
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Returns the square of the Euclidean distance between this point and that point.
     *
     * @param that the other point
     * @return the square of the Euclidean distance between this point and that point
     */
    public double distanceSquaredTo(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D that) {
        double dx = this.coordX - that.coordX;
        double dy = this.coordY - that.coordY;
        return dx * dx + dy * dy;
    }

    /**
     * Compares two points by coordY-coordinate, breaking ties by coordX-coordinate.
     * Formally, the invoking point (x0, y0) is less than the argument point (x1, y1)
     * if and only if either y0 &lt; y1 or if y0 = y1 and x0 &lt; x1.
     *
     * @param that the other point
     * @return the value 0 if this string is equal to the argument
     *     string (precisely when equals() returns true);
     *     a negative integer if this point is less than the argument
     *     point; and a positive integer if this point is greater than the
     *     argument point
     */
    public int compareTo(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D that) {
        if (this.coordY < that.coordY) {
            return -1;
        }
        if (this.coordY > that.coordY) {
            return +1;
        }
        if (this.coordX < that.coordX) {
            return -1;
        }
        if (this.coordX > that.coordX) {
            return +1;
        }
        return 0;
    }

    /**
     * Compares two points by polar angle (between 0 and 2pi) with respect to this point.
     *
     * @return the comparator
     */
    public Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> polarOrder() {
        return new PolarOrder();
    }

    /**
     * Compares two points by atan2() angle (between -pi and pi) with respect to this point.
     *
     * @return the comparator
     */
    public Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> atan2Order() {
        return new Atan2Order();
    }

    /**
     * Compares two points by distance to this point.
     *
     * @return the comparator
     */
    public Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D> distanceToOrder() {
        return new DistanceToOrder();
    }

    /**
     * Compares this point to the specified point.
     *
     * @param other the other point
     * @return true if this point equals other;
     *     false otherwise
     */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (other.getClass() != this.getClass()) {
            return false;
        }
        ca.nrc.cadc.caom2.compute.convex.SortablePoint2D that = (ca.nrc.cadc.caom2.compute.convex.SortablePoint2D) other;
        return this.coordX == that.coordX && this.coordY == that.coordY;
    }

    /**
     * Return a string representation of this point.
     *
     * @return a string representation of this point in the format (coordX, coordY)
     */
    @Override
    public String toString() {
        return "(" + coordX + ", " + coordY + ")";
    }

    /**
     * Returns an integer hash code for this point.
     *
     * @return an integer hash code for this point
     */
    @Override
    public int hashCode() {
        int hashX = ((Double) coordX).hashCode();
        int hashY = ((Double) coordY).hashCode();
        return 31 * hashX + hashY;
    }

    // compare points according to their coordX-coordinate
    private static class XOrder implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D p, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q) {
            if (p.coordX < q.coordX) {
                return -1;
            }
            if (p.coordX > q.coordX) {
                return +1;
            }
            return 0;
        }
    }

    // compare points according to their coordY-coordinate
    private static class YOrder implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D p, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q) {
            if (p.coordY < q.coordY) {
                return -1;
            }
            if (p.coordY > q.coordY) {
                return +1;
            }
            return 0;
        }
    }

    // compare points according to their polar radius
    private static class ROrder implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D p, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q) {
            double delta = (p.coordX * p.coordX + p.coordY * p.coordY) - (q.coordX * q.coordX + q.coordY * q.coordY);
            if (delta < 0) {
                return -1;
            }
            if (delta > 0) {
                return +1;
            }
            return 0;
        }
    }

    // compare other points relative to atan2 angle (bewteen -pi/2 and pi/2) they make with this Point
    private class Atan2Order implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q1, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q2) {
            double angle1 = angleTo(q1);
            double angle2 = angleTo(q2);
            if (angle1 < angle2) {
                return -1;
            } else if (angle1 > angle2) {
                return +1;
            } else {
                return 0;
            }
        }
    }

    // compare other points relative to polar angle (between 0 and 2pi) they make with this Point
    private class PolarOrder implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q1, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q2) {
            double dx1 = q1.coordX - coordX;
            double dy1 = q1.coordY - coordY;
            double dx2 = q2.coordX - coordX;
            double dy2 = q2.coordY - coordY;

            if (dy1 >= 0 && dy2 < 0) {
                return -1;    // q1 above; q2 below
            } else if (dy2 >= 0 && dy1 < 0) {
                return +1;    // q1 below; q2 above
            } else if (dy1 == 0 && dy2 == 0) {            // 3-collinear and horizontal
                if (dx1 >= 0 && dx2 < 0) {
                    return -1;
                } else if (dx2 >= 0 && dx1 < 0) {
                    return +1;
                } else {
                    return 0;
                }
            } else {
                return -ccw(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D.this, q1, q2);     // both above or below
            }

            // Note: ccw() recomputes dx1, dy1, dx2, and dy2
        }
    }

    // compare points according to their distance to this point
    private class DistanceToOrder implements Comparator<ca.nrc.cadc.caom2.compute.convex.SortablePoint2D>, Serializable {
        private static final long serialVersionUID = 201603031530L;

        public int compare(ca.nrc.cadc.caom2.compute.convex.SortablePoint2D p, ca.nrc.cadc.caom2.compute.convex.SortablePoint2D q) {
            double dist1 = distanceSquaredTo(p);
            double dist2 = distanceSquaredTo(q);
            if (dist1 < dist2) {
                return -1;
            } else if (dist1 > dist2) {
                return +1;
            } else {
                return 0;
            }
        }
    }
}
