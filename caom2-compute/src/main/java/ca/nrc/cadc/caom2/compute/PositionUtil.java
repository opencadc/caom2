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
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.compute.types.CartesianTransform;
import ca.nrc.cadc.caom2.compute.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.compute.types.MultiPolygon;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.InvalidPolygonException;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import jsky.coords.wcscon;
import org.apache.log4j.Logger;

/**
 * @author pdowler
 */
public final class PositionUtil {
    public static final double MAX_SANE_AREA = 250.0; // square degrees, CGPS has 235
    private static final Logger log = Logger.getLogger(PositionUtil.class);

    private PositionUtil() {
    }

    static class ComputedBounds {
        Shape bounds;
        MultiShape samples;
    }

    /**
     * Compute all possible positional metadata for the specified artifacts.
     *
     * @param artifacts
     * @return
     * @throws NoSuchKeywordException
     * @throws WCSLibRuntimeException
     */
    public static Position compute(Set<Artifact> artifacts)
        throws NoSuchKeywordException, WCSLibRuntimeException, InvalidPolygonException {
        DataLinkSemantics productType = DataLinkSemantics.THIS;
        log.debug("compute: " + productType);

        ComputedBounds cb = computeBounds(artifacts, productType);
        if (cb != null) {
            Position p = new Position(cb.bounds, cb.samples);
            p.dimension = computeDimensionsFromRange(artifacts, productType);
            if (p.dimension == null) {
                p.dimension = computeDimensionsFromWCS(p.getBounds(), artifacts, productType);
            }
            p.resolution = computeResolution(artifacts, productType);
            p.sampleSize = computeSampleSize(artifacts, productType);
            
            return p;
        }

        return null;
    }

    static ComputedBounds computeBounds(Set<Artifact> artifacts, DataLinkSemantics productType) 
            throws NoSuchKeywordException, InvalidPolygonException {
        List<Shape> samples = generateShapes(artifacts, productType);
        if (samples == null || samples.isEmpty()) {
            return null;
        }

        Shape bounds = combineShapes(samples);
        ComputedBounds ret = new ComputedBounds();
        ret.bounds = bounds;
        ret.samples = new MultiShape();
        ret.samples.getShapes().addAll(samples);

        return ret;
    }

    static List<Shape> generateShapes(Set<Artifact> artifacts, DataLinkSemantics productType) throws InvalidPolygonException {
        int num = 0;
        List<Shape> ret = new ArrayList<>();
        Chunk chunk = null;
        for (Artifact a : artifacts) {
            log.debug("generateShapes: " + a.getURI());
            for (Part p : a.getParts()) {
                log.debug("generateShapes: " + a.getURI() + " " + p.getName());
                for (Chunk c : p.getChunks()) {
                    log.debug("generateShapes: " + a.getURI() + " " + p.getName() + " " + c.getID());
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        log.debug("generateShapes: " + a.getURI() + " "
                            + a.getProductType() + " " + p.productType + " " + c.productType);
                        if (c.position != null) {
                            Shape s = generateShape(c.position);
                            if (s != null) {
                                ret.add(s);
                            }
                        }
                    } else {
                        log.debug("generateShapes SKIP: " + a.getURI() + " "
                            + a.getProductType() + " " + p.productType + " " + c.productType);
                    }
                }
            }
        }
        
        return ret;
    }
    
    static Shape generateShape(SpatialWCS wcs) throws InvalidPolygonException {
        return toShapeICRS(wcs);
    }

    static Shape combineShapes(List<Shape> shapes) {
        if (shapes.size() == 1) {
            return shapes.get(0);
        }
        List<Polygon> polys = new ArrayList<>(shapes.size());
        // convert to polygon
        for (Shape s : shapes) {
            if (s instanceof Circle) {
                Circle c = (Circle) s;
                Polygon p = toPolygon(c);
                polys.add(p);
            } else if (s instanceof Polygon) {
                Polygon p = (Polygon) s;
                polys.add(p);
            } else {
                throw new UnsupportedOperationException("unexpected shape: " + s.getClass().getName());
            }
        }
        // compute union
        List<MultiPolygon> mps = PolygonUtil.convert(polys);
        MultiPolygon combined = MultiPolygon.compose(mps);
        Polygon outer = PolygonUtil.getOuterHull(combined);
        return outer;
    }

    // this works for mosaic camera data: multiple parts with ~single scale wcs functions
    static Dimension2D computeDimensionsFromWCS(Shape bounds, Set<Artifact> artifacts, DataLinkSemantics productType)
        throws NoSuchKeywordException {
        log.debug("[computeDimensionsFromWCS] " + bounds + " " + artifacts.size());
        if (bounds == null) {
            return null;
        }

        // pick the WCS with the largest pixel size
        SpatialWCS sw = null;
        boolean foundCoordBounds = false;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.position != null && c.position.getAxis().function != null) {
                            num++;
                            double ss = Util.getPixelScale(c.position.getAxis().function);
                            if (ss >= scale) {
                                scale = ss;
                                sw = c.position;
                            }
                            foundCoordBounds = foundCoordBounds || c.position.getAxis().bounds != null;
                        }
                    }
                }
            }
        }
        if (sw == null) {
            return null;
        }

        if (num == 1) { // single WCS solution, ASSUME CoordBounds2D mask is whole frame
            // deep copy
            long ix = sw.getAxis().function.getDimension().naxis1;
            long iy = sw.getAxis().function.getDimension().naxis2;
            return new Dimension2D(ix, iy);
        }
        
        List<Point> points = null;
        if (bounds instanceof Polygon) {
            Polygon poly = (Polygon) bounds;
            points = poly.getVertices();
        } else if (bounds instanceof Circle) {
            throw new UnsupportedOperationException();
        }

        WCSWrapper map = new WCSWrapper(sw, 1, 2);
        Transform transform = new Transform(map);

        double x1 = Double.MAX_VALUE;
        double x2 = -1 * x1;
        double y1 = x1;
        double y2 = -1 * y1;
        CoordSys csys = inferCoordSys(sw);
        List<long[]> pixv = new ArrayList<long[]>();
        for (Point pt : points) {
            double[] coords = new double[] {pt.getLongitude(), pt.getLatitude()};
            if (csys.swappedAxes) {
                double tmp = coords[0];
                coords[0] = coords[1];
                coords[1] = tmp;
            }
            // convert vertices -> wcs sky coordinates -> pixel coords
            if (csys.name.equals("GAL")) {
                Point2D p = wcscon.fk52gal(new Point2D.Double(coords[0], coords[1]));
                coords[0] = p.getX();
                coords[1] = p.getY();
            } else if (csys.name.equals("FK4")) {
                Point2D p = wcscon.fk524(new Point2D.Double(coords[0], coords[1]));
                coords[0] = p.getX();
                coords[1] = p.getY();
            }
            Transform.Result tr = transform.sky2pix(coords);
            long[] pv = new long[] {(long) tr.coordinates[0], (long) tr.coordinates[1]};
            log.debug("[computeDimensionsFromWCS] " + pt.getLongitude() + "," + pt.getLatitude() 
                    + " -> " + pv[0] + "," + pv[1]);
            pixv.add(pv);
        }
        // find lengths of edges
        long ix = 0;
        long iy = 0;
        long[] pp = pixv.get(0);
        long px = pp[0];
        long py = pp[1];
        for (long[] p : pixv) {
            // find minimum px and py
            if (p[0] < px) {
                px = p[0];
            }
            if (p[1] < py) {
                py = p[1];
            }
        }
        for (long[] p : pixv) {
            // find max distance
            long dix = p[0] - px;
            long diy = p[1] - py;
            if (dix < 0) {
                dix *= -1;
            }
            if (diy < 0) {
                diy *= -1;
            }
            if (dix > ix) {
                ix = dix;
            }
            if (diy > iy) {
                iy = diy;
            }
            log.debug("[computeDimensionsFromWCS] " + dix + "," + diy + " ::: " + ix + "," + iy);
        }
        log.debug("[computeDimensionsFromWCS] " + ix + "," + iy);
        return new Dimension2D(ix, iy);
    }

    static Dimension2D computeDimensionsFromRange(Set<Artifact> artifacts, DataLinkSemantics productType) {
        // assume all the WCS come from a single data array, find min/max pixel values
        double x1 = Double.MAX_VALUE;
        double x2 = -1 * x1;
        double y1 = x1;
        double y2 = -1 * y1;
        boolean found = false;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.position != null) {
                            CoordRange2D range = c.position.getAxis().range;
                            CoordBounds2D bounds = c.position.getAxis().bounds;
                            if (range != null) {
                                x1 = Math.min(x1, range.getStart().getCoord1().pix);
                                x1 = Math.min(x1, range.getEnd().getCoord1().pix);
                                x2 = Math.max(x2, range.getStart().getCoord1().pix);
                                x2 = Math.max(x2, range.getEnd().getCoord1().pix);

                                y1 = Math.min(y1, range.getStart().getCoord2().pix);
                                y1 = Math.min(y1, range.getEnd().getCoord2().pix);
                                y2 = Math.max(y2, range.getStart().getCoord2().pix);
                                y2 = Math.max(y2, range.getEnd().getCoord2().pix);
                                found = true;
                            }
                        }
                    }
                }
            }
        }
        if (!found) {
            return null;
        }
        double dx = Math.abs(x2 - x1);
        double dy = Math.abs(y2 - y1);
        return new Dimension2D((long) dx, (long) dy);
    }

    static Double computeSampleSize(Set<Artifact> artifacts, DataLinkSemantics productType) {
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.position != null) {
                            CoordAxis2D axis = c.position.getAxis();
                            double num = Util.getNumPixels(axis);
                            double scale = Util.getPixelScale(axis);
                            totSampleSize += scale * num;
                            numPixels += num;
                            log.debug("[computeSampleSize] num=" + num + " scale=" + scale);
                        }
                    }
                }
            }
        }
        if (totSampleSize > 0.0 && numPixels > 0.0) {
            return new Double(3600.0 * totSampleSize / numPixels); // convert degrees to arcsec
        }
        return null;
    }

    // resolution of SCIENCE data, mean is weighted by number of pixels
    static Double computeResolution(Set<Artifact> artifacts, DataLinkSemantics productType) {
        double totResolution = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.position != null && c.position.resolution != null) {
                            double num = Util.getNumPixels(c.position.getAxis());
                            totResolution += c.position.resolution * num;
                            numPixels += num;
                        }
                    }
                }
            }
        }
        if (totResolution > 0.0 && numPixels > 0.0) {
            return new Double(totResolution / numPixels);
        }
        return null;
    }


    static Shape toShape(SpatialWCS wcs, boolean swappedAxes) throws InvalidPolygonException {

        CoordRange2D range = wcs.getAxis().range;
        CoordBounds2D bounds = wcs.getAxis().bounds;
        CoordFunction2D function = wcs.getAxis().function;

        Shape ret = null;
        if (bounds != null && bounds instanceof CoordCircle2D) {
            CoordCircle2D cc = (CoordCircle2D) bounds;
            if (swappedAxes) {
                ret = new Circle(new Point(cc.getCenter().coord2, cc.getCenter().coord1), cc.getRadius());
            } else {
                ret = new Circle(new Point(cc.getCenter().coord1, cc.getCenter().coord2), cc.getRadius());
            }
            return ret;
        }

        try {
            Polygon poly = null;
            if (bounds != null && bounds instanceof CoordPolygon2D) {
                CoordPolygon2D cp = (CoordPolygon2D) bounds;
                poly = toPolygon(wcs, cp);
            } else if (bounds != null) {
                throw new UnsupportedOperationException(bounds.getClass().getName() + " -> Polygon");
            } else if (function != null) {
                poly = toPolygon(wcs, function);
            } else if (range != null) {
                poly = toPolygon(wcs, range);
            }
            if (poly != null) {
                log.debug("[wcs.toPolygon] native " + poly);
                Polygon tmp = new Polygon();
                for (Point v : poly.getVertices()) {
                    Point sv = v;
                    if (swappedAxes) {
                        sv = new Point(v.getLatitude(), v.getLongitude());
                    }
                    rangeReduce(v);
                    tmp.getVertices().add(sv);
                }
                tmp.validate();
                ret = tmp;
            }

            log.debug("[toPolygon] normalised " + poly);
            return ret;
        } catch (NoSuchKeywordException ex) {
            log.debug("failed to generate shape from WCS", ex);
        }
        return ret;
    }
    
    static Polygon toPolygon(SpatialWCS wcs, CoordRange2D cr)
        throws NoSuchKeywordException {
        
        double x1 = cr.getStart().getCoord1().val;
        double x2 = cr.getEnd().getCoord1().val;
        double y1 = cr.getStart().getCoord2().val;
        double y2 = cr.getEnd().getCoord2().val;
        Polygon ret = new Polygon();
        ret.getVertices().add(new Point(x1, y1));
        ret.getVertices().add(new Point(x2, y1));
        ret.getVertices().add(new Point(x2, y2));
        ret.getVertices().add(new Point(x1, y2));
        if (!ret.getCounterClockwise()) {
            ret = Polygon.flip(ret);
        }
        return ret;
    }
    
    static Polygon toPolygon(SpatialWCS wcs, CoordPolygon2D cp)
        throws NoSuchKeywordException {
        Polygon ret = new Polygon();
        Iterator<ValueCoord2D> i = cp.getVertices().iterator();
        while (i.hasNext()) {
            ValueCoord2D coord = i.next();
            ret.getVertices().add(new Point(coord.coord1, coord.coord2));
        }
        if (!ret.getCounterClockwise()) {
            ret = Polygon.flip(ret);
        }
        return ret;
    }
    
    static Circle toCircle(SpatialWCS wcs, CoordCircle2D cc)
        throws NoSuchKeywordException {
        Circle circ = new Circle(new Point(cc.getCenter().coord1, cc.getCenter().coord2), cc.getRadius());
        return circ;
    }
    
    static Polygon toPolygon(Circle circ) {
        CartesianTransform trans = CartesianTransform.getTransform(circ);
        
        Polygon tmp = new Polygon();
        Point tcen = trans.transform(circ.getCenter());
        double x = tcen.getLongitude();
        double y = tcen.getLatitude();
        double dy = circ.getRadius();
        double dx = Math.abs(dy / Math.cos(Math.toRadians(y)));
        tmp.getVertices().add(rangeReduce(new Point(x - dx, y - dy)));
        tmp.getVertices().add(rangeReduce(new Point(x + dx, y - dy)));
        tmp.getVertices().add(rangeReduce(new Point(x + dx, y + dy)));
        tmp.getVertices().add(rangeReduce(new Point(x - dx, y + dy)));
        
        
        CartesianTransform inv = trans.getInverseTransform();
        return inv.transform(tmp);
    }
    
    static Polygon toPolygon(SpatialWCS wcs, CoordFunction2D function)
        throws NoSuchKeywordException {
        double x1 = 0.5;
        double x2 = function.getDimension().naxis1 + 0.5;
        double y1 = 0.5;
        double y2 = function.getDimension().naxis2 + 0.5;
        List<Point> pixCoords = new ArrayList<>();
        pixCoords.add(new Point(x1, y1));
        pixCoords.add(new Point(x2, y1));
        pixCoords.add(new Point(x2, y2));
        pixCoords.add(new Point(x1, y2));
        List<Point> skyCoords = getVerticesWcsLib(wcs, pixCoords);
        
        Polygon ret = new Polygon();
        ret.getVertices().addAll(skyCoords);
        if (!ret.getCounterClockwise()) {
            ret = Polygon.flip(ret);
        }
        return ret;
    }

    static Shape toShapeICRS(SpatialWCS wcs) throws InvalidPolygonException {
        CoordSys coordsys = inferCoordSys(wcs);
        if (!coordsys.supported) {
            return null;
        }

        Shape shape = toShape(wcs, coordsys.swappedAxes);

        if (shape != null) {
            if (shape instanceof Circle) {
                Circle circ = (Circle) shape;
                Point icrsCenter = toICRS(coordsys, circ.getCenter());
                shape = new Circle(icrsCenter, circ.getRadius());
            }
            if (shape instanceof Polygon) {
                Polygon poly = (Polygon) shape;
                toICRS(coordsys, poly.getVertices()); // in-place transform

                Point c = poly.getCenter();
                if (c == null || Double.isNaN(c.getLongitude()) || Double.isNaN(c.getLatitude())) {
                    throw new IllegalPolygonException("computed polygon has invalid center: " + c);
                }

                if (wcs.getAxis().function != null && wcs.getAxis().bounds == null) {
                    // toPolygon used the wcs function to compute polygon: enforce MAX_SANE_AREA
                    if (poly.getArea() > MAX_SANE_AREA) {
                        throw new IllegalPolygonException("area too large: " + poly.getArea() + " sq. deg. -- assuming invalid WCS");
                    }
                }
            }
        }

        log.debug("[toShapeICRS] " + shape);
        return shape;
    }

    static Point rangeReduce(Point v) {
        double longitude = v.getLongitude();
        double latitude = v.getLatitude();
        if (latitude > 90.0) {
            longitude += 180.0;
            latitude = 180.0 - latitude;
        }
        if (latitude < -90.0) {
            longitude += 180.0;
            latitude = -180.0 - latitude;
        }
        if (longitude < 0) {
            longitude += 360.0;
        }
        if (longitude > 360.0) {
            longitude -= 360.0;
        }
        return new Point(longitude, latitude);
    }

    public static CoordSys inferCoordSys(SpatialWCS wcs) {
        CoordSys ret = new CoordSys();
        ret.name = wcs.coordsys;
        ret.supported = false;
        String ctype1 = wcs.getAxis().getAxis1().getCtype();
        String ctype2 = wcs.getAxis().getAxis2().getCtype();

        if (CoordSys.GAPPT.equals(ret.name)) {
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        } else if ((ctype1.startsWith("ELON") && ctype2.startsWith("ELAT"))
            || (ctype1.startsWith("ELAT") && ctype2.startsWith("ELON"))) {
            // ecliptic
            ret.name = CoordSys.ECL;
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        } else if ((ctype1.startsWith("HLON") && ctype2.startsWith("HLAT"))
            || (ctype1.startsWith("HLAT") && ctype2.startsWith("HLON"))) {
            // helio-ecliptic
            ret.name = CoordSys.HECL;
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        } else if ((ctype1.startsWith("GLON") && ctype2.startsWith("GLAT"))
            || (ctype1.startsWith("GLAT") && ctype2.startsWith("GLON"))) {
            if (CoordSys.GAL.equals(ret.name)) {
                log.debug("found coordsys=" + ret.name + " with GLON,GLAT - OK");
            } else if (ret.name != null) {
                log.debug("found coordsys=" + ret.name + " with GLON,GLAT - ignoring and assuming GAL");
                ret.name = null;
            }
            if (ret.name == null) {
                ret.name = CoordSys.GAL;
            }
            if (ctype1.startsWith("GLAT")) {
                ret.swappedAxes = true;
            }
            ret.supported = true;
        } else if ((ctype1.startsWith("RA") && ctype2.startsWith("DEC"))
            || (ctype1.startsWith("DEC") && ctype2.startsWith("RA"))) {
            if (ret.name == null) {
                if (wcs.equinox == null) {
                    ret.name = CoordSys.ICRS;
                } else if (Math.abs(wcs.equinox.doubleValue() - 1950.0) < 1.0) {
                    ret.name = CoordSys.FK4;
                } else if (Math.abs(wcs.equinox.doubleValue() - 2000.0) < 1.0) {
                    ret.name = CoordSys.FK5;
                } else {
                    log.debug("cannot infer coordinate system from RA,DEC and equinox = " + wcs.equinox);
                }
            }

            if (ctype1.startsWith("DEC")) {
                ret.swappedAxes = true;
            }
            if (ret.name != null) {
                ret.supported = true;
            }
        }

        return ret;
    }

    private static List<Point> getVerticesWcsLib(SpatialWCS wcs, List<Point> vertices)
        throws NoSuchKeywordException {
        double[] coords = new double[2];

        List<Point> ret = new ArrayList<>(vertices.size());
        WCSWrapper map = new WCSWrapper(wcs, 1, 2);
        Transform transform = new Transform(map);
        Iterator<Point> i = vertices.iterator();
        while (i.hasNext()) {
            Point v = i.next();
            coords[0] = v.getLongitude();
            coords[1] = v.getLatitude();
            //log.debug("transform: " + v);
            Transform.Result tr = transform.pix2sky(coords);
            //log.debug("wcslib: " + coords[0] + "," + coords[1] + " -> " + tr.coordinates[0] + "," + tr.coordinates[1]);
            Point p = new Point(tr.coordinates[0], tr.coordinates[1]);
            ret.add(p);
        }
        return ret;
    }

    public static Point toICRS(CoordSys cs, Point v) {
        if (CoordSys.ICRS.equals(cs.name) || CoordSys.FK5.equals(cs.name)) {
            return v;
        }

        Point2D p;
        if (CoordSys.GAL.equals(cs.name)) {
            p = wcscon.gal2fk5(new Point2D.Double(v.getLongitude(), v.getLatitude()));
        } else if (CoordSys.FK4.equals(cs.name)) {
            p = wcscon.fk425(new Point2D.Double(v.getLongitude(), v.getLatitude()));
        } else {
            throw new IllegalArgumentException("unexpected coordsys: " + cs.name);
        }
        return new Point(p.getX(), p.getY());
    }

    public static void toICRS(CoordSys cs, List<Point> vertices) {
        if (CoordSys.ICRS.equals(cs.name) || CoordSys.FK5.equals(cs.name)) {
            return;
        }

        Point2D p;
        if (CoordSys.GAL.equals(cs.name)) {
            for (int i = 0; i < vertices.size(); i++) {
                Point v = vertices.get(i);
                p = wcscon.gal2fk5(new Point2D.Double(v.getLongitude(), v.getLatitude()));
                Point r = new Point(p.getX(), p.getY());
                vertices.set(i, r);
            }
        } else if (CoordSys.FK4.equals(cs.name)) {
            for (int i = 0; i < vertices.size(); i++) {
                Point v = vertices.get(i);
                p = wcscon.fk425(new Point2D.Double(v.getLongitude(), v.getLatitude()));
                Point r = new Point(p.getX(), p.getY());
                vertices.set(i, r);
            }
        } else {
            throw new IllegalArgumentException("unexpected coordsys: " + cs.name);
        }
    }

    public static class CoordSys implements Serializable {
        private static final long serialVersionUID = 201207300900L;

        public static String ICRS = "ICRS";
        public static String GAL = "GAL";
        public static String FK4 = "FK4";
        public static String FK5 = "FK5";

        public static String ECL = "ECL";
        public static String HECL = "HELIOECLIPTIC";
        public static String GAPPT = "GAPPT";

        String name;
        Boolean timeDependent;
        boolean supported;
        boolean swappedAxes = false;

        public String getName() {
            return name;
        }

        public Boolean getTimeDependent() {
            return timeDependent;
        }

        public boolean isSupported() {
            return supported;
        }

        public boolean isSwappedAxes() {
            return swappedAxes;
        }

    }
}
