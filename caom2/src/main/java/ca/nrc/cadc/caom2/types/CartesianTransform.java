// Created on 20-Jun-07

package ca.nrc.cadc.caom2.types;

import java.io.Serializable;
import org.apache.log4j.Logger;

public class CartesianTransform implements Serializable {
    private static final Logger log = Logger
            .getLogger(CartesianTransform.class);

    private static final long serialVersionUID = 201207300900L;

    // the negative X axis
    static final double[] NEG_X_AXIS = new double[] { -1.0, 0.0, 0.0 };

    // the positive X axis
    static final double[] POS_X_AXIS = new double[] { 1.0, 0.0, 0.0 };

    public static final String X = "x";
    public static final String Y = "y";
    public static final String Z = "z";

    // some tests mess with these
    double angle;
    String axis;

    CartesianTransform() {
    }

    public static CartesianTransform getTransform(MultiPolygon poly) {
        return getTransform(poly, false);
    }

    public static CartesianTransform getTransform(MultiPolygon poly, boolean force) {
        double[] cube = getBoundingCube(poly, null);
        return getTransform(cube, force);
    }

    public static CartesianTransform getTransform(Circle c) {
        return getTransform(c, false);
    }
    
    public static CartesianTransform getTransform(Circle c, boolean force) {
        double[] xyz = CartesianTransform.toUnitSphere(c.getCenter().cval1, c.getCenter().cval2);
        double[] cube = new double[] {
            xyz[0], xyz[0],
            xyz[1], xyz[1],
            xyz[2], xyz[2]
        };
        return getTransform(cube, force);
    }
    
    public static CartesianTransform getTransform(double[] cube,
            boolean force) {
        double x1 = cube[0];
        double x2 = cube[1];
        double y1 = cube[2];
        double y2 = cube[3];
        double z1 = cube[4];
        double z2 = cube[5];
        // log.debug("getTransform: bounding cube = " + x1+ ":" + x2 + " " + y1
        // + ":" + y2 + " " + z1 + ":" + z2);

        double cx = 0.5 * (x1 + x2);
        double cz = 0.5 * (z1 + z2);
        // log.debug("getTransform: bounding cube center = " + cx + " " + cy + "
        // " + cz);

        if (Math.abs(cz) > 0.02 || force) {
            CartesianTransform trans = new CartesianTransform();
            trans.axis = CartesianTransform.Y;
            // project vector in X-Z plane and normalise
            double mv1 = Math.sqrt(cx * cx + cz * cz);
            double[] v1 = { cx / mv1, 0, cz / mv1 };
            // acos is only valid for 0 to PI
            if (cz > 0.0) { // north
                // angle to +X axis, then add 180 degrees
                double[] v2 = { 1, 0, 0 };
                double cosa = dotProduct(v1, v2);
                trans.angle = Math.acos(cosa) + Math.PI;
            } else { // south
                // directly get angle to -X axis
                double[] v2 = { -1, 0, 0 };
                double cosa = dotProduct(v1, v2);
                trans.angle = Math.acos(cosa);
            }
            log.debug("off equator: " + trans);
            return trans;
        }
        if (y1 <= 0.0 && y2 >= 0.0 && x1 > 0.0) {
            CartesianTransform trans = new CartesianTransform();
            trans.angle = Math.PI;
            trans.axis = CartesianTransform.Z;
            log.debug("straddling meridan at equator: " + trans);
            return trans;
        }
        return new CartesianTransform();
    }

    public CartesianTransform getInverseTransform() {
        if (isNull()) {
            return this;
        }
        CartesianTransform ret = new CartesianTransform();
        ret.axis = axis;
        ret.angle = -1.0 * angle;
        return ret;
    }

    @Override
    public String toString() {
        return "CartesianTransform[" + axis + "," + angle + "]";
    }

    public boolean isNull() {
        return axis == null;
    }

    /**
     * Convert long,lat coordinates to cartesian coordinates on the unit sphere.
     * NOTE: this uses a right-handed unit sphere but looking from the outside,
     * which is opposite to the normal astro convention of looking from the
     * center (which means theta goes in the other direction in astronomy), but
     * as long as the opposite conversion is done with toLongLat that is just
     * fine.
     * 
     * @param longitude
     * @param latitude
     * @return a double[3]
     */
    public static double[] toUnitSphere(double longitude, double latitude) {
        double[] ret = new double[3];
        double theta = Math.toRadians(longitude);
        double phi = Math.toRadians(90.0 - latitude);
        ret[0] = Math.cos(theta) * Math.sin(phi);
        ret[1] = Math.sin(theta) * Math.sin(phi);
        ret[2] = Math.cos(phi);
        return ret;
    }

    /**
     * Convert cartesian points on the unit sphere to long,lat.
     * 
     * @param x
     * @param y
     * @param z
     * @return a double[2]
     */
    public static double[] toLongLat(double x, double y, double z) {
        double[] ret = new double[2];
        if ((x == 0.0 && y == 0.0) || z == 1.0 || z == -1.0) {
            ret[0] = 0.0;
        } else {
            ret[0] = Math.toDegrees(Math.atan2(y, x));
        }
        if (ret[0] < 0.0) {
            ret[0] += 360.0;
        }
        if (z > 1.0) {
            z = 1.0;
        } else if (z < -1.0) {
            z = -1.0;
        }
        ret[1] = 90.0 - Math.toDegrees(Math.acos(z));
        return ret;
    }

    public Point transform(Point p) {
        if (isNull()) {
            return p;
        }

        double[] p2 = transformPoint(p);
        return new Point(p2[0], p2[1]);
    }

    public Vertex transform(Vertex v) {
        if (isNull()) {
            return v;
        }
        double[] p2 = { 0.0, 0.0 };
        if (!SegmentType.CLOSE.equals(v.getType())) {
            p2 = transformPoint(v);
        }
        return new Vertex(p2[0], p2[1], v.getType());
    }

    public MultiPolygon transform(MultiPolygon p) {
        if (isNull()) {
            return p;
        }

        MultiPolygon ret = new MultiPolygon();
        for (Vertex v : p.getVertices()) {
            ret.getVertices().add(transform(v));
        }
        return ret;
    }

    // impl for above methods
    private double[] transformPoint(Point p) {
        double[] xyz = CartesianTransform.toUnitSphere(p.cval1, p.cval2);
        double[] dp = rotate(xyz);
        return CartesianTransform.toLongLat(dp[0], dp[1], dp[2]);
    }

    public static double[] getBoundingCube(MultiPolygon poly, double[] cube) {
        // x1, x2, y1, y2, z1, z2
        if (cube == null) {
            cube = new double[6];
            cube[0] = Double.POSITIVE_INFINITY;
            cube[1] = Double.NEGATIVE_INFINITY;
            cube[2] = Double.POSITIVE_INFINITY;
            cube[3] = Double.NEGATIVE_INFINITY;
            cube[4] = Double.POSITIVE_INFINITY;
            cube[5] = Double.NEGATIVE_INFINITY;
        }
        for (Vertex v : poly.getVertices()) {
            if (!SegmentType.CLOSE.equals(v.getType())) {
                double[] xyz = CartesianTransform.toUnitSphere(v.cval1,
                        v.cval2);
                cube[0] = Math.min(cube[0], xyz[0]);
                cube[1] = Math.max(cube[1], xyz[0]);

                cube[2] = Math.min(cube[2], xyz[1]);
                cube[3] = Math.max(cube[3], xyz[1]);

                cube[4] = Math.min(cube[4], xyz[2]);
                cube[5] = Math.max(cube[5], xyz[2]);
            }
        }
        // fix X bounds assuming smallish polygons and cubes
        if (cube[2] < 0.0 && 0.0 < cube[3] && cube[4] < 0.0 && 0.0 < cube[5]) {
            if (cube[0] > 0.0) {
                cube[1] = 1.01; // slightly outside sphere
            } else if (cube[1] < 0.0) {
                cube[0] = -1.01; // slightly outside sphere
            }
        }
        // fix Y bounds assuming smallish polygons and cubes
        if (cube[0] < 0.0 && 0.0 < cube[1] && cube[4] < 0.0 && 0.0 < cube[5]) {
            if (cube[2] > 0.0) {
                cube[3] = 1.01; // slightly outside sphere
            } else if (cube[3] < 0.0) {
                cube[2] = -1.01; // slightly outside sphere
            }
        }
        // fix Z bounds assuming smallish polygons and cubes
        if (cube[0] < 0.0 && 0.0 < cube[1] && cube[2] < 0.0 && 0.0 < cube[3]) {
            if (cube[4] > 0.0) {
                cube[5] = 1.01; // slightly outside sphere
            } else if (cube[5] < 0.0) {
                cube[4] = -1.01; // slightly outside sphere
            }
        }
        return cube;
    }

    private static double dotProduct(double[] v1, double[] v2) {
        return (v1[0] * v2[0] + v1[1] * v2[1] + v1[2] * v2[2]);
    }

    public double[] rotate(double[] p) {
        if (Y.equals(axis)) {
            return yrotate(p);
        }

        if (Z.equals(axis)) {
            return zrotate(p);
        }

        throw new IllegalStateException("unknown axis: " + axis);
    }

    // rotation about the y-axis
    private double[] yrotate(double[] p) {
        double[] ret = new double[3];
        double cr = Math.cos(angle);
        double sr = Math.sin(angle);
        ret[0] = cr * p[0] + sr * p[2];
        ret[1] = p[1];
        ret[2] = -1.0 * sr * p[0] + cr * p[2];
        return ret;
    }

    // rotation about the z-axis
    private double[] zrotate(double[] p) {
        double[] ret = new double[3];
        double cr = Math.cos(angle);
        double sr = Math.sin(angle);
        ret[0] = cr * p[0] + -1 * sr * p[1];
        ret[1] = sr * p[0] + cr * p[1];
        ret[2] = p[2];
        return ret;
    }
}
