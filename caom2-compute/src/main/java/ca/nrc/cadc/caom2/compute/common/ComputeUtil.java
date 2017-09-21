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

package ca.nrc.cadc.caom2.compute.common;


import ca.nrc.cadc.caom2.*;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.wcs.*;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import jsky.coords.wcscon;
import org.apache.log4j.Logger;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class to assign values to fields marked with the computed stereotype
 * in the data model.
 * 
 * @author pdowler
 */
public class ComputeUtil 
{
    private static final Logger log = Logger.getLogger(ComputeUtil.class);
    public static final double MAX_SANE_AREA = 250.0; // square degrees, CGPS has 235

    private ComputeUtil() { }
    
    /**
     * Clear computed plane metadata.
     * @deprecated 
     */
    public static void clearTransientState(Plane p)
    {
        p.position = null;
        p.energy = null;
        p.time = null;
        p.polarization = null;
    }
    
    /**
     * Compute plane metadata from WCS.
     * @deprecated 
     */
    public static void computeTransientState(Observation o, Plane p)
    {
        computePosition(p);
        computeEnergy(p);
        computeTime(p);
        computePolarization(p);
    }


    private static void computePosition(Plane pl)
    {
        try
        {
//            p.position = PositionUtil.compute(p.getArtifacts());

//            public static Position compute(Set<Artifact> artifacts)
//        throws NoSuchKeywordException, WCSLibRuntimeException
//            {
            Set<Artifact> artifacts = pl.getArtifacts();
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

//                return p;
//            }
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
    }

    private static void computeEnergy(Plane p)
    {
        try
        {
            p.energy = EnergyUtil.compute(p.getArtifacts());
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
    }

    private static void computeTime(Plane p)
    {
        p.time = TimeUtil.compute(p.getArtifacts());
    }

    private static void computePolarization(Plane p)
    {
        p.polarization = PolarizationUtil.compute(p.getArtifacts());
    }



    public static Dimension2D computeDimensionsFromRange(Set<Artifact> artifacts, ProductType productType)
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

    public static Double computeSampleSize(Set<Artifact> artifacts, ProductType productType)
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
    public static Double computeResolution(Set<Artifact> artifacts, ProductType productType)
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

    public static Boolean computeTimeDependent(Set<Artifact> artifacts, ProductType productType)
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
                            PositionUtil.CoordSys csys = PositionUtil.inferCoordSys(c.position);
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


    public static List<MultiPolygon> generatePolygons(Set<Artifact> artifacts, ProductType productType)
            throws NoSuchKeywordException
    {
        List<MultiPolygon> polys = new ArrayList<MultiPolygon>();
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
                            MultiPolygon poly = PositionUtil.toPolygon(c.position);
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
        List<MultiPolygon> polys = generatePolygons(artifacts, productType);
        if (polys.isEmpty())
            return null;
        log.debug("[computeBounds] components: " + polys.size());
        MultiPolygon mp = PolygonUtil.compose(polys);
        Polygon poly = PolygonUtil.getOuterHull(mp);
        log.debug("[computeBounds] done: " + poly);
        if (poly.getArea() > MAX_SANE_AREA)
            throw new IllegalPolygonException("area too large, assuming invalid WCS: " + poly.getArea());
        return poly;
    }

    // this works for mosaic camera data: multiple parts with ~single scale wcs functions
    public static Dimension2D computeDimensionsFromWCS(Polygon poly, Set<Artifact> artifacts, ProductType productType)
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
        PositionUtil.CoordSys csys = PositionUtil.inferCoordSys(sw);
        List<long[]> pixv = new ArrayList<long[]>();
        for (Point pt : poly.getPoints())
        {
            double[] coords = new double[] { pt.cval1, pt.cval2 };
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
            log.debug("[computeDimensionsFromWCS] " + pt.cval1 + "," + pt.cval2 + " -> " + pv[0] + "," + pv[1]);
            pixv.add(pv);
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
}
