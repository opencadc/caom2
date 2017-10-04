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
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Utility class for 1-d Time calculations.
 *
 * @author pdowler
 */
public final class TimeUtil {
    private static final Logger log = Logger.getLogger(TimeUtil.class);
    // sort of a hack: we assume absolute MJD values in the TimeWCS which
    // is the default if MJDREF, JDREF, and DATEREF are all absent = 0.0 in FITS
    private static final String TARGET_TIMESYS = "UTC";
    private static final String TARGET_CTYPE = "TIME";
    private static final String TARGET_CUNIT = "d";
    public static double DEFAULT_UNION_SCALE = 0.10;

    private TimeUtil() 
    {
    }

    public static Time compute(Set<Artifact> artifacts) {
        ProductType productType = Util.choseProductType(artifacts);
        log.debug("compute: " + productType);
        Time t = new Time();
        if (productType != null) {
            t.bounds = computeBounds(artifacts, productType);
            t.dimension = computeDimensionFromRangeBounds(artifacts, productType);
            if (t.dimension == null) {
                t.dimension = computeDimensionFromWCS(t.bounds, artifacts, productType);
            }
            t.resolution = computeResolution(artifacts, productType);
            t.sampleSize = computeSampleSize(artifacts, productType);
            t.exposure = computeExposureTime(artifacts, productType);
        }

        return t;
    }

    /**
     * Computes the union.
     */
    static Interval computeBounds(Set<Artifact> artifacts, ProductType productType) {
        double unionScale = 0.02;
        List<SubInterval> subs = new ArrayList<SubInterval>();

        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null) {
                            CoordRange1D range = c.time.getAxis().range;
                            CoordBounds1D bounds = c.time.getAxis().bounds;
                            CoordFunction1D function = c.time.getAxis().function;
                            if (range != null) {
                                SubInterval s = toInterval(c.time, range);
                                log.debug("[computeBounds] range -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    SubInterval s = toInterval(c.time, cr);
                                    log.debug("[computeBounds] bounds -> sub: " + s);
                                    Util.mergeIntoList(s, subs, unionScale);
                                }
                            } else if (function != null) {
                                SubInterval s = toInterval(c.time, function);
                                log.debug("[computeBounds] function -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            }
                        }
                    }
                }
            }
        }
        if (subs.isEmpty()) {
            return null;
        }

        // compute the outer bounds of the sub-intervals
        double lb = Double.MAX_VALUE;
        double ub = Double.MIN_VALUE;
        for (SubInterval sub : subs) {
            lb = Math.min(lb, sub.getLower());
            ub = Math.max(ub, sub.getUpper());
        }
        return new Interval(lb, ub, subs);
    }


    /**
     * Compute mean sample size (pixel scale).
     *
     * @param wcs
     * @return a new Polygon computed with the default union scale
     */
    static Double computeSampleSize(Set<Artifact> artifacts, ProductType productType) {
        // assumption: all pixels are distinct so we can just compute a weighted average
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null) {
                            CoordRange1D range = c.time.getAxis().range;
                            CoordBounds1D bounds = c.time.getAxis().bounds;
                            CoordFunction1D function = c.time.getAxis().function;

                            numPixels += Util.getNumPixels(c.time.getAxis());
                            if (range != null) {
                                SubInterval si = toInterval(c.time, range);
                                totSampleSize += si.getUpper() - si.getLower();
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    SubInterval si = toInterval(c.time, cr);
                                    totSampleSize += si.getUpper() - si.getLower();
                                }
                            } else if (function != null) {
                                SubInterval si = toInterval(c.time, function);
                                totSampleSize += si.getUpper() - si.getLower();
                            }
                        }
                    }
                }
            }
        }

        if (totSampleSize > 0.0 && numPixels > 0.0) {
            return totSampleSize / numPixels;
        }
        return null;
    }

    /**
     * Compute dimensionality (number of pixels) from WCS function. This method assumes
     * that the energy axis is roughly continuous (e.g. it ignores gaps).
     *
     * @param wcsArray
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromWCS(Interval bounds, Set<Artifact> artifacts, ProductType productType) {
        log.debug("computeDimensionFromWCS: " + bounds);
        if (bounds == null) {
            return null;
        }

        // pick the WCS with the largest pixel size
        TemporalWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null && c.time.getAxis().function != null) {
                            num++;
                            double ss = Math.abs(c.time.getAxis().function.getDelta());
                            if (ss >= scale) {
                                scale = ss;
                                sw = c.time;
                            }
                        }
                    }
                }
            }
        }
        if (sw == null) {
            return null;
        }

        if (sw.getAxis().function == null) {
            return null;
        }

        if (num == 1) {
            return sw.getAxis().function.getNaxis();
        }

        double x1 = val2pix(sw, sw.getAxis().function, bounds.getLower());
        double x2 = val2pix(sw, sw.getAxis().function, bounds.getUpper());

        log.debug("computeDimensionFromWCS: " + x1 + "," + x2);
        return new Long(Math.round(Math.abs(x2 - x1)));
    }

    /**
     * Compute dimensionality (number of pixels).
     *
     * @param wcsArray
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, ProductType productType) {
        // assumption: all   pixels are distinct so just add up the number of pixels
        double numPixels = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null) {
                            double n = Util.getNumPixels(c.time.getAxis(), false);
                            numPixels += n;
                        }
                    }
                }
            }
        }

        if (numPixels > 0.0) {
            log.debug("computeDimensionFromRangeBounds: " + numPixels);
            return new Long((long) numPixels);
        }
        log.debug("computeDimensionFromRangeBounds: null");
        return null;
    }

    /**
     * Compute the mean exposure time per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param wcs
     * @return exposure time in seconds
     */
    static Double computeExposureTime(Set<Artifact> artifacts, ProductType productType) {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply compute the mean values time weighted by number of pixels in
        // the chunk
        double totExposureTime = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null && c.time.exposure != null) {
                            double num = Util.getNumPixels(c.time.getAxis());
                            totExposureTime += c.time.exposure * num;
                            numPixels += num;
                        }
                    }
                }
            }
        }

        if (totExposureTime > 0.0 && numPixels > 0.0) {
            return new Double(totExposureTime / numPixels);
        }
        return null;
    }

    /**
     * Compute the mean resolution per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param wcs
     * @return exposure time in seconds
     */
    static Double computeResolution(Set<Artifact> artifacts, ProductType productType) {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply compute the mean values time weighted by number of pixels in
        // the chunk
        double totResolution = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null && c.time.resolution != null) {
                            double num = Util.getNumPixels(c.time.getAxis());
                            totResolution += c.time.resolution * num;
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

    static SubInterval toInterval(TemporalWCS wcs, CoordRange1D r) {
        double a = r.getStart().val;
        double b = r.getEnd().val;

        String ctype = wcs.getAxis().getAxis().getCtype();
        if (!(TARGET_TIMESYS.equals(wcs.timesys) && TARGET_CTYPE.equals(ctype))
            && !(wcs.timesys == null && TARGET_TIMESYS.equals(ctype))) {
            throw new UnsupportedOperationException("unexpected TIMESYS, CTYPE: " + wcs.timesys + "," + ctype);
        }

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        String cunit = wcs.getAxis().getAxis().getCunit();
        if (!TARGET_CUNIT.equals(cunit)) {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null) {
            a += wcs.mjdref.doubleValue();
            b += wcs.mjdref.doubleValue();
        }

        return new SubInterval(Math.min(a, b), Math.max(a, b));
    }

    static SubInterval toInterval(TemporalWCS wcs, CoordFunction1D func) {
        double p1 = 0.5;
        double p2 = func.getNaxis().doubleValue() + 0.5;
        double a = Util.pix2val(func, p1);
        double b = Util.pix2val(func, p2);

        String ctype = wcs.getAxis().getAxis().getCtype();
        if (!(TARGET_TIMESYS.equals(wcs.timesys) && TARGET_CTYPE.equals(ctype))
            && !(wcs.timesys == null && TARGET_TIMESYS.equals(ctype))) {
            throw new UnsupportedOperationException("unexpected TIMESYS, CTYPE: " + wcs.timesys + "," + ctype);
        }

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        String cunit = wcs.getAxis().getAxis().getCunit();
        if (!TARGET_CUNIT.equals(cunit)) {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null) {
            a += wcs.mjdref.doubleValue();
            b += wcs.mjdref.doubleValue();
        }

        return new SubInterval(Math.min(a, b), Math.max(a, b));
    }

    private static double pix2val(TemporalWCS wcs, CoordFunction1D func, double pix) {
        double ret = Util.pix2val(func, pix);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        String cunit = wcs.getAxis().getAxis().getCunit();
        if (!TARGET_CUNIT.equals(cunit)) {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null) {
            ret += wcs.mjdref.doubleValue();
        }

        return ret;
    }

    private static double val2pix(TemporalWCS wcs, CoordFunction1D func, double val) {
        if (wcs.mjdref != null) {
            val -= wcs.mjdref.doubleValue();
        }

        double ret = Util.val2pix(func, val);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        String cunit = wcs.getAxis().getAxis().getCunit();
        if (!TARGET_CUNIT.equals(cunit)) {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        return ret;
    }
}
