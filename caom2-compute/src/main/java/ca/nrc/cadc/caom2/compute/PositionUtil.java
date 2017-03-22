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

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.types.Box;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.Location;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.caom2.wcs.WCSWrapper;
import ca.nrc.cadc.util.HexUtil;
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
 *
 * @author pdowler
 */
public final class PositionUtil
{
    private static final Logger log = Logger.getLogger(PositionUtil.class);
    
    public static final double MAX_SANE_AREA = 250.0; // square degrees, CGPS has 235
    
    private PositionUtil() { }

    /**
     * Compute all possible positional metadata for the specified artifacts.
     * 
     * @param artifacts
     * @return
     * @throws NoSuchKeywordException
     * @throws WCSLibRuntimeException
     */
    public static Position compute(Set<Artifact> artifacts)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        ProductType productType = Util.choseProductType(artifacts);
        log.debug("compute: " + productType);
                
        Position p = new Position();
        if (productType != null)
        {
            Polygon poly = computeBounds(artifacts, productType);
            p.bounds = poly;
            p.dimension = computeDimensionsFromRange(artifacts, productType);
            if (p.dimension == null)
                p.dimension = computeDimensionsFromWCS(poly, artifacts, productType);
            p.resolution = computeResolution(artifacts, productType);
            p.sampleSize = computeSampleSize(artifacts, productType);
            p.timeDependent = computeTimeDependent(artifacts, productType);
        }

        return p;
    }

    

    /**
     * Encode the specified Shape as a byte array. This method figures out which type of shape
     * has been specified and calls the encode method on the implementation class.
     *
     * @param s the shape to encode
     * @return byte array storing all state for the shape
     * @see Polygon#encode(Polygon)
     * @see Circle#encode(Circle)
     * @see Location#encode(Location)
     */
    public static byte[] encode(Shape s)
    {
        if (s instanceof Polygon)
            return Polygon.encode((Polygon) s);
        if (s instanceof Circle)
            return Circle.encode((Circle) s);
        if (s instanceof Box)
            return Box.encode((Box) s);
        throw new IllegalArgumentException("unknown shape type: " + s.getClass().getName());
    }

    /**
     * Decode the specified byte array and create a Shape. This method figures out
     * which type of shape has been encoded and calls the decode method on the
     * implementation class.
     *
     * @param encoded a previously encoded shape
     * @return a Shape or null if type cannot be determined
     */
    public static Shape decode(byte[] encoded)
    {
        int magic = HexUtil.toInt(encoded, 0);
        switch(magic)
        {
            case Shape.MAGIC_CIRCLE: return Circle.decode(encoded);
            case Shape.MAGIC_POLYGON: return Polygon.decode(encoded);
            case Shape.MAGIC_BOX: return Box.decode(encoded);
        }
        throw new IllegalArgumentException("unknown shape type, found magic number: " + magic);
    }
   
    public static List<Polygon> generatePolygons(Set<Artifact> artifacts, ProductType productType)
        throws NoSuchKeywordException
    {
        List<Polygon> polys = new ArrayList<Polygon>();
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        log.debug("generatePolygons: " + a.getURI() + " " 
                                + a.getProductType() + " " + p.productType + " " + c.productType);
                        if (c.position != null)
                        {
                            Polygon poly = toPolygon(c.position);
                            log.debug("[generatePolygons] wcs: " + poly);
                            if (poly != null && poly.getArea() > MAX_SANE_AREA)
                                throw new IllegalPolygonException("area too large, assuming invalid WCS: " 
                                    + a.getURI() + "[" + p.getName() + "] " + poly.getArea());
                            if (poly != null)
                                polys.add(poly);
                        }
                    }
                }
            }
        }
        return polys;
    }
    public static Polygon computeBounds(Set<Artifact> artifacts, ProductType productType)
        throws NoSuchKeywordException
    {
        // since we compute the union, just blindly use all the polygons
        // derived from spatial wcs
        List<Polygon> polys = generatePolygons(artifacts, productType);
        if (polys.isEmpty())
            return null;
        log.debug("[computeBounds] components: " + polys.size());
        Polygon poly = PolygonUtil.union(polys);
        log.debug("[computeBounds] done: " + poly);
        if (poly.getArea() > MAX_SANE_AREA)
            throw new IllegalPolygonException("area too large, assuming invalid WCS: " + poly.getArea());
        return poly;
    }

    // this works for mosaic camera data: multiple parts with ~single scale wcs functions
    static Dimension2D computeDimensionsFromWCS(Polygon poly, Set<Artifact> artifacts, ProductType productType)
        throws NoSuchKeywordException
    {
        log.debug("[computeDimensionsFromWCS] " + poly + " " + artifacts.size());
        if (poly == null)
            return null;

        // pick the WCS with the largest pixel size
        SpatialWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.position != null && c.position.getAxis().function != null)
                        {
                            num++;
                            double ss = Util.getPixelScale(c.position.getAxis().function);
                            if (ss >= scale)
                            {
                                scale = ss;
                                sw = c.position;
                            }
                        }
                    }
                }
            }
        }
        if (sw == null)
            return null;
        
        if (num == 1) // single WCS solution
        {
            // deep copy
            long ix = sw.getAxis().function.getDimension().naxis1;
            long iy = sw.getAxis().function.getDimension().naxis2;
            return new Dimension2D(ix, iy);
        }
        
        WCSWrapper map = new WCSWrapper(sw, 1, 2);
        Transform transform = new Transform(map);

        double x1 = Double.MAX_VALUE;
        double x2 = -1 * x1;
        double y1 = x1;
        double y2 = -1 * y1;
        CoordSys csys = inferCoordSys(sw);
        List<long[]> pixv = new ArrayList<long[]>();
        for (Vertex v : poly.getVertices())
        {
            if ( !SegmentType.CLOSE.equals(v.getType()) )
            {
                double[] coords = new double[] { v.cval1, v.cval2 };
                if (csys.swappedAxes)
                {
                    double tmp = coords[0];
                    coords[0] = coords[1];
                    coords[1] = tmp;
                }
                // convert vertices -> wcs sky coordinates -> pixel coords
                if (csys.name.equals("GAL"))
                {
                    Point2D p = wcscon.fk52gal(new Point2D.Double(coords[0], coords[1]));
                    coords[0] = p.getX();
                    coords[1] = p.getY();
                }
                else if (csys.name.equals("FK4"))
                {
                    Point2D p = wcscon.fk524(new Point2D.Double(coords[0], coords[1]));
                    coords[0] = p.getX();
                    coords[1] = p.getY();
                }
                Transform.Result tr = transform.sky2pix(coords);
                long[] pv = new long[] { (long) tr.coordinates[0], (long) tr.coordinates[1] };
                log.debug("[computeDimensionsFromWCS] " + v.cval1 + "," + v.cval2 + " -> " + pv[0] + "," + pv[1]);
                pixv.add(pv);
            }
        }
        // find lengths of edges
        long ix = 0;
        long iy = 0;
        long[] pp = pixv.get(0);
        long px = pp[0];
        long py = pp[1];
        for (long[] p : pixv)
        {
            // find minimum px and py
            if (p[0] < px)
                px = p[0];
            if (p[1] < py)
                py = p[1];
        }
        for (long[] p : pixv)
        {
            // find max distance
            long dix = p[0] - px;
            long diy = p[1] - py;
            if (dix < 0) dix *= -1;
            if (diy < 0) diy *= -1;
            if (dix > ix) ix = dix;
            if (diy > iy) iy = diy;
            log.debug("[computeDimensionsFromWCS] " + dix + "," + diy + " ::: " + ix + "," + iy);
        }
        log.debug("[computeDimensionsFromWCS] " + ix + "," + iy);
        return new Dimension2D(ix, iy);
    }

    static Dimension2D computeDimensionsFromRange(Set<Artifact> artifacts, ProductType productType)
    {
        // assume all the WCS come from a single data array, find min/max pixel values
        double x1 = Double.MAX_VALUE;
        double x2 = -1 * x1;
        double y1 = x1;
        double y2 = -1 * y1;
        boolean found = false;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.position != null)
                        {
                            CoordRange2D range = c.position.getAxis().range;
                            CoordBounds2D bounds = c.position.getAxis().bounds;
                            if (range != null)
                            {
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
        if (!found)
            return null;
        double dx = Math.abs(x2 - x1);
        double dy = Math.abs(y2 - y1);
        return new Dimension2D((long) dx, (long) dy);
    }

    static Double computeSampleSize(Set<Artifact> artifacts, ProductType productType)
    {
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.position != null)
                        {
                            CoordAxis2D axis = c.position.getAxis();
                            double num = Util.getNumPixels(axis);
                            double scale = Util.getPixelScale(axis);
                            totSampleSize += scale * num;
                            numPixels += num;
                            log.debug("[computeSampleSize] num=" + num + " scale="+scale);
                        }
                    }
                }
            }
        }
        if (totSampleSize > 0.0 && numPixels > 0.0)
            return new Double(3600.0 * totSampleSize / numPixels); // convert degrees to arcsec
        return null;
    }

    // resolution of SCIENCE data, mean is weighted by number of pixels
    static Double computeResolution(Set<Artifact> artifacts, ProductType productType)
    {
        double totResolution = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.position != null && c.position.resolution != null)
                        {
                            double num = Util.getNumPixels(c.position.getAxis());
                            totResolution += c.position.resolution * num;
                            numPixels += num;
                        }
                    }
                }
            }
        }
        if (totResolution > 0.0 && numPixels > 0.0)
            return new Double(totResolution / numPixels);
        return null;
    }

    static Boolean computeTimeDependent(Set<Artifact> artifacts, ProductType productType)
    {
        boolean foundTD = false;
        boolean foundTI = false;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.position != null)
                        {
                            CoordSys csys = inferCoordSys(c.position);
                            if (csys.timeDependent != null)
                            {
                                foundTD = foundTD || csys.timeDependent;
                                foundTI = foundTI || !csys.timeDependent;
                            }
                        }
                    }
                }
            }
        }
        if (foundTD && !foundTI)
            return Boolean.TRUE;
        if (!foundTD && foundTI)
            return Boolean.FALSE;
        return null;
    }
    
    public static Polygon toPolygon(SpatialWCS wcs)
        throws NoSuchKeywordException
    {
        CoordSys coordsys = inferCoordSys(wcs);
        if (!coordsys.supported)
            return null;

        CoordRange2D range = wcs.getAxis().range;
        CoordBounds2D bounds = wcs.getAxis().bounds;
        CoordFunction2D function = wcs.getAxis().function;
        
        Polygon poly = new Polygon();
        if(bounds != null)
        {
            if (bounds instanceof CoordCircle2D)
            {
                CoordCircle2D cc = (CoordCircle2D) bounds;
                if (Math.abs(cc.getCenter().coord2) + cc.getRadius() > 80.0) // near a pole
                    throw new UnsupportedOperationException("cannot convert CoordCircle2D -> Polygon near the pole ("+cc+")");
                double x = cc.getCenter().coord1;
                double y = cc.getCenter().coord2;
                double dy = cc.getRadius();
                double dx = Math.abs(dy / Math.cos(Math.toRadians(y)));
                poly.getVertices().add(rangeReduce(new Vertex(x-dx, y-dy, SegmentType.MOVE)));
                poly.getVertices().add(rangeReduce(new Vertex(x+dx, y-dy, SegmentType.LINE)));
                poly.getVertices().add(rangeReduce(new Vertex(x+dx, y+dy, SegmentType.LINE)));
                poly.getVertices().add(rangeReduce(new Vertex(x-dx, y+dy, SegmentType.LINE)));
                poly.getVertices().add(rangeReduce(new Vertex(0.0, 0.0, SegmentType.CLOSE)));
            }
            else if (bounds instanceof CoordPolygon2D)
            {
                CoordPolygon2D cp = (CoordPolygon2D) bounds;
                Iterator<ValueCoord2D> i = cp.getVertices().iterator();
                while ( i.hasNext() )
                {
                    ValueCoord2D coord = i.next();
                    if (poly.getVertices().isEmpty())
                        poly.getVertices().add(new Vertex(coord.coord1, coord.coord2, SegmentType.MOVE));
                    else
                        poly.getVertices().add(new Vertex(coord.coord1, coord.coord2, SegmentType.LINE));
                }
                poly.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
                PolygonUtil.validateSegments(poly);
            }
            else
                throw new UnsupportedOperationException(bounds.getClass().getName() + " -> Polygon");
        }
        else if (function != null)
        {
            double x1 = 0.5;
            double x2 = function.getDimension().naxis1 + 0.5;
            double y1 = 0.5;
            double y2 = function.getDimension().naxis2 + 0.5;
            List<Vertex> pixCoords = new ArrayList<Vertex>();
            pixCoords.add(new Vertex(x1, y1, SegmentType.MOVE));
            pixCoords.add(new Vertex(x2, y1, SegmentType.LINE));
            pixCoords.add(new Vertex(x2, y2, SegmentType.LINE));
            pixCoords.add(new Vertex(x1, y2, SegmentType.LINE));
            pixCoords.add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            List<Vertex> skyCoords = getVerticesWcsLib(wcs, pixCoords);
            poly.getVertices().addAll(skyCoords);
        }
        else if (range != null)
        {
            double x1 = range.getStart().getCoord1().val;
            double x2 = range.getEnd().getCoord1().val;
            double y1 = range.getStart().getCoord2().val;
            double y2 = range.getEnd().getCoord2().val;
            poly.getVertices().add(new Vertex(x1, y1, SegmentType.MOVE));
            poly.getVertices().add(new Vertex(x2, y1, SegmentType.LINE));
            poly.getVertices().add(new Vertex(x2, y2, SegmentType.LINE));
            poly.getVertices().add(new Vertex(x1, y2, SegmentType.LINE));
            poly.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
        }

        log.debug("[wcs.toPolygon] native " + poly);
        toICRS(coordsys, poly.getVertices());
        
        Point c = poly.getCenter();
        if (c == null || Double.isNaN(c.cval1) || Double.isNaN(c.cval2))
            throw new IllegalPolygonException("computed polygon has invalid center: " + c);
        
        log.debug("[wcs.toPolygon] icrs " + poly);
        return poly;
    }
    
    private static Vertex rangeReduce(Vertex v)
    {
        if (v.cval2 > 90.0)
        {
            v.cval1 += 180.0;
            v.cval2 = 180.0 - v.cval2;
        }
        if (v.cval2 <  -90.0)
        {
            v.cval1 += 180.0;
            v.cval2 = -180.0 - v.cval2;
        }
        if (v.cval1 < 0)
            v.cval1 += 360.0;
        if (v.cval1 > 360.0)
            v.cval1 -= 360.0;
        return v;
    }

    static class CoordSys implements Serializable
    {
        private static final long serialVersionUID = 201207300900L;

        private static String ICRS = "ICRS";
        private static String GAL = "GAL";
        private static String FK4 = "FK4";
        private static String FK5 = "FK5";

        private static String ECL = "ECL";
        private static String HECL = "HELIOECLIPTIC";
        private static String GAPPT = "GAPPT";
        
        String name;
        Boolean timeDependent;
        boolean supported;
        boolean swappedAxes = false;

    }

    static CoordSys inferCoordSys(SpatialWCS wcs)
    {
        CoordSys ret = new CoordSys();
        ret.name = wcs.coordsys;
        ret.supported = false;
        String ctype1 = wcs.getAxis().getAxis1().getCtype();
        String ctype2 = wcs.getAxis().getAxis2().getCtype();

        if ( CoordSys.GAPPT.equals(ret.name) )
        {
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        }
        else if((ctype1.startsWith("ELON") && ctype2.startsWith("ELAT"))
                || (ctype1.startsWith("ELAT") && ctype2.startsWith("ELON")) )
        {
            // ecliptic
            ret.name = CoordSys.ECL;
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        }
        else if ( (ctype1.startsWith("HLON") && ctype2.startsWith("HLAT"))
                || (ctype1.startsWith("HLAT") && ctype2.startsWith("HLON")) )
        {
            // helio-ecliptic
            ret.name = CoordSys.HECL;
            ret.timeDependent = Boolean.TRUE;
            ret.supported = false;
        }
        else if((ctype1.startsWith("GLON") && ctype2.startsWith("GLAT"))
                || (ctype1.startsWith("GLAT") && ctype2.startsWith("GLON")) )
        {
            if (CoordSys.GAL.equals(ret.name))
                log.debug("found coordsys=" + ret.name + " with GLON,GLAT - OK");
            else if (ret.name != null)
            {
                log.debug("found coordsys=" + ret.name + " with GLON,GLAT - ignoring and assuming GAL");
                ret.name = null;
            }
            if (ret.name == null)
                ret.name = CoordSys.GAL;
            if (ctype1.startsWith("GLAT"))
                ret.swappedAxes = true;
            ret.supported = true;
        }
        else if ( (ctype1.startsWith("RA") && ctype2.startsWith("DEC"))
                    || (ctype1.startsWith("DEC") && ctype2.startsWith("RA")) )
        {
            if (ret.name == null)
            {
                if (wcs.equinox == null)
                    ret.name = CoordSys.ICRS;
                else if (Math.abs(wcs.equinox.doubleValue() - 1950.0) < 1.0)
                    ret.name = CoordSys.FK4;
                else if (Math.abs(wcs.equinox.doubleValue() - 2000.0) < 1.0)
                    ret.name = CoordSys.FK5;
                else
                    log.debug("cannot infer coordinate system from RA,DEC and equinox = " + wcs.equinox);
            }

            if ( ctype1.startsWith("DEC") )
                ret.swappedAxes = true;
            if (ret.name != null)
                ret.supported = true;
        }

        return ret;
    }

    static List<Vertex> getVerticesWcsLib(SpatialWCS wcs, List<Vertex> vertices)
        throws NoSuchKeywordException
    {
        double[] coords = new double[2];

        WCSWrapper map = new WCSWrapper(wcs, 1, 2);
        Transform transform = new Transform(map);
        Iterator<Vertex> i = vertices.iterator();
        while ( i.hasNext() )
        {
            Vertex v = i.next();
            if ( !SegmentType.CLOSE.equals(v.getType()) )
            {
                coords[0] = v.cval1;
                coords[1] = v.cval2;
                Transform.Result tr = transform.pix2sky(coords);
                //log.debug("wcslib: " + coords[0] + "," + coords[1]
                //        + " -> " + tr.coordinates[0] + "," + tr.coordinates[1]);
                v.cval1 = tr.coordinates[0];
                v.cval2 = tr.coordinates[1];
                //truncatePrecision(v);
            }
        }
        return vertices;
    }

    static void toICRS(CoordSys cs, List<Vertex> vertices)
    {
        Point2D p;
        if (CoordSys.GAL.equals(cs.name))
        {
            for (int i=0; i<vertices.size(); i++)
            {
                Vertex v = (Vertex) vertices.get(i);
                if ( !SegmentType.CLOSE.equals(v.getType()) )
                {
                    p = wcscon.gal2fk5(new Point2D.Double(v.cval1, v.cval2));
                    v.cval1 = p.getX();
                    v.cval2 = p.getY();
                }
            }
        }
        else if (CoordSys.FK4.equals(cs.name))
        {
            for (int i=0; i<vertices.size(); i++)
            {
                Vertex v = (Vertex) vertices.get(i);
                if ( !SegmentType.CLOSE.equals(v.getType()) )
                {
                    p = wcscon.fk425(new Point2D.Double(v.cval1, v.cval2));
                    v.cval1 = p.getX();
                    v.cval2 = p.getY();
                }
            }
        }
        else if ( !CoordSys.ICRS.equals(cs.name) && !CoordSys.FK5.equals(cs.name))
            throw new IllegalArgumentException("unexpected coordsys: " + cs.name);

        if (cs.swappedAxes)
        {
            for (int i=0; i<vertices.size(); i++)
            {
                Vertex v = (Vertex) vertices.get(i);
                double tmp = v.cval1;
                v.cval1 = v.cval2;
                v.cval2 = tmp;
            }
        }
    }
    
    public static long[] getBounds(SpatialWCS wcs, Shape s)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        if (s == null)
            return null;
        if (s instanceof Circle)
            return getBounds(wcs, (Circle) s);
        if (s instanceof Polygon)
            return getBounds(wcs, (Polygon) s);
        throw new IllegalArgumentException("unsupported cutout shape: " + s.getClass().getSimpleName());
    }
    
    /**
     * Find the pixel bounds that enclose the specified circle.
     *
     * @param wcs a spatial wcs solution
     * @param c circle with center in ICRS coordinates
     * @return int[4] holding [x1, x2, y1, y2], int[0] if all pixels are included,
     *         or null if the circle does not intersect the WCS
     */
    public static long[] getBounds(SpatialWCS wcs, Circle c)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        // convert the Circle -> Box ~ Polygon
        // TODO: a WCS-aligned polygon would be better than an axis-aligned Box
        double x = c.getCenter().cval1;
        double y = c.getCenter().cval2;
        double dy = c.getRadius();
        double dx = Math.abs(dy / Math.cos(Math.toRadians(y)));
        Polygon poly = new Polygon();
        poly.getVertices().add(new Vertex(x-dx, y-dy, SegmentType.MOVE));
        poly.getVertices().add(new Vertex(x+dx, y-dy, SegmentType.LINE));
        poly.getVertices().add(new Vertex(x+dx, y+dy, SegmentType.LINE));
        poly.getVertices().add(new Vertex(x-dx, y+dy, SegmentType.LINE));
        poly.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
        return getBounds(wcs, poly);
    }

    /**
     * Compute the range of pixel indices that correspond to the supplied
     * polygon. This method computes the cutout ranges within the image. The
     * returned value is null if there is no intersection, an int[4] for a
     * cutout, and an int[0] when the image is wholly contained within the
     * polygon (and no cutout is necessary).
     *
     * @param wcs a spatial wcs solution
     * @param poly the shape to cutout
     * @return int[4] holding [x1, x2, y1, y2], int[0] if all pixels are included,
     *         or null if the circle does not intersect the WCS
     */
    public static long[] getBounds(SpatialWCS wcs, Polygon poly)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        CoordSys coordsys = inferCoordSys(wcs);

        // detect necessary conversion of target coords to native WCS coordsys
        boolean gal = false;
        boolean fk4 = false;
        if (CoordSys.GAL.equals(coordsys.name))
            gal = true;
        else if (CoordSys.FK4.equals(coordsys.name))
            fk4 = true;
        else if (!CoordSys.ICRS.equals(coordsys.name) && !CoordSys.FK5.equals(coordsys.name))
            throw new UnsupportedOperationException("unexpected coordsys: " + coordsys.name);

        WCSWrapper map = new WCSWrapper(wcs, 1, 2);
        Transform transform = new Transform(map);

        // convert wcs/footprint to sky coords
        log.debug("computing poly INTERSECT footprint");
        Polygon foot = toPolygon(wcs);
        log.debug("input poly: " + poly);
        log.debug("wcs poly: " + foot);
        
        Polygon npoly = PolygonUtil.intersection(poly, foot);
        if (npoly == null)
        {
            log.debug("poly INTERSECT footprint == null");
            return null;
        }
        //log.debug("intersection: " + npoly);
        

        // npoly is in ICRS
        if (gal || fk4)
        {
            // convert npoly to native coordsys, in place since we created a new npoly above
            log.debug("converting poly to " + coordsys);
            for (Vertex v : npoly.getVertices())
            {
                if ( !SegmentType.CLOSE.equals(v.getType()) )
                {
                    Point2D.Double pp = new Point2D.Double(v.cval1, v.cval2);

                    // convert poly coords to native WCS coordsys
                    if (gal)
                        pp = wcscon.fk52gal(pp);
                    else if (fk4)
                        pp = wcscon.fk524(pp);
                    v.cval1 = pp.x;
                    v.cval2 = pp.y;
                }
            }
        }

        // convert npoly to pixel coordinates and find min/max
        long x1 = Integer.MAX_VALUE;
        long x2 = -1 * x1;
        long y1 = x1;
        long y2 = -1 * y1;
        log.debug("converting npoly to pixel coordinates");
        double[] coords = new double[2];
        for (Vertex v : npoly.getVertices())
        {
            if ( !SegmentType.CLOSE.equals(v.getType()) )
            {
                coords[0] = v.cval1;
                coords[1] = v.cval2;
                if (coordsys.swappedAxes)
                {
                    coords[0] = v.cval2;
                    coords[1] = v.cval1;
                }
                Transform.Result tr = transform.sky2pix(coords);
                // if p2 is null, it was a long way away from the WCS and does not
                // impose a limit/cutout - so we can safely skip it
                if (tr != null)
                {
                    log.debug(v + " -> " + tr.coordinates[0] + "," + tr.coordinates[1]);
                    x1 = (long) Math.min(x1, tr.coordinates[0]);
                    x2 = (long) Math.max(x2, tr.coordinates[0]);
                    y1 = (long) Math.min(y1, tr.coordinates[1]);
                    y2 = (long) Math.max(y2, tr.coordinates[1]);
                }
                //else
                //    System.out.println("[GeomUtil] failed to convert " + v + ": skipping");
            }
        }

        // clipping
        long naxis1 = wcs.getAxis().function.getDimension().naxis1;
        long naxis2 = wcs.getAxis().function.getDimension().naxis2;
        return doClipCheck(naxis1, naxis2, x1, x2, y1, y2);
    }

    private static long[] doClipCheck(long w, long h, long x1, long x2, long y1, long y2)
    {
        if (x1 < 1)
            x1 = 1;
        if (x2 > w)
            x2 = w;
        if (y1 < 1)
            y1 = 1;
        if (y2 > h)
            y2 = h;

        // validity check
        if (x1 >= w || x2 <= 1 || y1 >= h || y2 <= 1) // no pixels included
        {
            return null;
        }
        if (x1 == 1 && y1 == 1 && x2 == w && y2 == h) // all pixels includes
        {
            return new long[0];
        }
        return new long[] { x1, x2, y1, y2 }; // an actual cutout
    }


}
