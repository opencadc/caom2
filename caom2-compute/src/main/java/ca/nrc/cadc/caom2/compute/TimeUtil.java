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

import ca.nrc.cadc.dali.Interval;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Time;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.TemporalWCS;
import org.opencadc.erfa.DubiousYearException;
import org.opencadc.erfa.ERFALib;
import org.opencadc.erfa.ERFALibException;
import org.opencadc.erfa.UnacceptableDateException;

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
    private static final String COMPAT_CTYPE = "TIME";
    private static final String TARGET_CUNIT = "d";
    public static double DEFAULT_UNION_SCALE = 0.10;
    private static final String TAI_TIMESYS = "TAI";
    private static final String TT_TIMESYS = "TT";
    private static final List<String> SUPPORTED_TIMESYS =
        Arrays.asList(TAI_TIMESYS, TT_TIMESYS, TARGET_TIMESYS);
    public static double MJD2JD_OFFSET = 2400000.5D;

    private TimeUtil() {
    }

    static class ComputedBounds {
        Interval<Double> bounds;
        List<Interval<Double>> samples;
    }
    
    public static Time compute(Set<Artifact> artifacts) {
        DataLinkSemantics productType = DataLinkSemantics.THIS;
        log.debug("compute: " + productType);
        
        ComputedBounds cb = computeBounds(artifacts, productType);
        if (cb != null) {
            Time ret = new Time(cb.bounds);
            ret.getSamples().addAll(cb.samples);
            ret.dimension = computeDimensionFromRangeBounds(artifacts, productType);
            if (ret.dimension == null) {
                ret.dimension = computeDimensionFromWCS(ret.getBounds(), artifacts, productType);
            }
            ret.resolution = computeResolution(artifacts, productType);
            ret.sampleSize = computeSampleSize(artifacts, productType);
            ret.exposure = computeExposureTime(artifacts, productType);
            
            return ret;
        }
        
        return null;
    }

    /**
     * Computes the union.
     */
    static ComputedBounds computeBounds(Set<Artifact> artifacts, DataLinkSemantics productType) {
        double unionScale = 0.02;
        List<Interval<Double>> subs = new ArrayList<>();

        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.time != null) {
                            CoordRange1D range = c.time.getAxis().range;
                            CoordBounds1D bounds = c.time.getAxis().bounds;
                            CoordFunction1D function = c.time.getAxis().function;
                            if (range != null) {
                                Interval<Double> s = toInterval(c.time, range);
                                log.debug("[computeBounds] range -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    Interval<Double> s = toInterval(c.time, cr);
                                    log.debug("[computeBounds] bounds -> sub: " + s);
                                    Util.mergeIntoList(s, subs, unionScale);
                                }
                            } else if (function != null) {
                                Interval<Double> s = TimeUtil.toInterval(c.time, function);
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
        for (Interval<Double> sub : subs) {
            lb = Math.min(lb, sub.getLower());
            ub = Math.max(ub, sub.getUpper());
        }

        ComputedBounds ret = new ComputedBounds();
        ret.bounds = new Interval<Double>(lb, ub);
        ret.samples = subs;
        return ret;
    }

    /**
     * Compute mean sample size (pixel scale).
     *
     * @param artifacts the set of Artifact's
     * @param productType the artifact DataLinkSemantics
     * @return a new Polygon computed with the default union scale
     */
    static Double computeSampleSize(Set<Artifact> artifacts, DataLinkSemantics productType) {
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
                                Interval<Double> si = toInterval(c.time, range);
                                totSampleSize += si.getUpper() - si.getLower();
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    Interval<Double> si = toInterval(c.time, cr);
                                    totSampleSize += si.getUpper() - si.getLower();
                                }
                            } else if (function != null) {
                                Interval<Double> si = TimeUtil.toInterval(c.time, function);
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
     * Compute dimensionality (number of pixels) from WCS function. This method
     * assumes
     * that the energy axis is roughly continuous (e.g. it ignores gaps).
     *
     * @param bounds the interval bounds
     * @param artifacts the set of Artifact's
     * @param productType the artifact DataLinkSemantics
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromWCS(Interval<Double> bounds, Set<Artifact> artifacts, DataLinkSemantics productType) {
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
        return Math.round(Math.abs(x2 - x1));
    }

    /**
     * Compute dimensionality (number of pixels).
     *
     * @param artifacts the set of Artifact's
     * @param productType the artifact DataLinkSemantics
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, DataLinkSemantics productType) {
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
            return (long) numPixels;
        }
        log.debug("computeDimensionFromRangeBounds: null");
        return null;
    }

    /**
     * Compute the mean exposure time per chunk, weighted by the number of
     * pixels.
     * in the chunk.
     *
     * @param artifacts the set of Artifact's
     * @param productType the artifact DataLinkSemantics
     * @return exposure time in seconds
     */
    static Double computeExposureTime(Set<Artifact> artifacts, DataLinkSemantics productType) {
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
            return totExposureTime / numPixels;
        }
        return null;
    }

    /**
     * Compute the mean resolution per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param artifacts the set of Artifact's
     * @param productType the artifact DataLinkSemantics
     * @return exposure time in seconds
     */
    static Double computeResolution(Set<Artifact> artifacts, DataLinkSemantics productType) {
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
            return totResolution / numPixels;
        }
        return null;
    }

    static Interval<Double> toInterval(TemporalWCS wcs, CoordRange1D r) {
        validateWCS(wcs);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        double np = Math.abs(r.getStart().pix - r.getEnd().pix);
        double a = r.getStart().val;
        double b = r.getEnd().val;
        double delta = Math.abs(b - a);
        if (delta == 0.0 && np > 1.0) {
            throw new IllegalArgumentException("invalid CoordRange1D: found " + np + " + pixels and delta = 0.0 in [" + a + "," + b + "]");
        }

        // for TIMESYS other than UTC do the time scale transform to UTC
        double[] ta = transform(wcs, a);
        double[] tb = transform(wcs, b);
        double lower = ta[0] + ta[1];
        double upper = tb[0] + tb[1];
        return new Interval<Double>(Math.min(lower, upper), Math.max(lower, upper));
    }

    static Interval<Double> toInterval(TemporalWCS wcs, CoordFunction1D func) {
        validateWCS(wcs);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
            throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
        }
        
        double p1 = 0.5;
        double p2 = func.getNaxis().doubleValue() + 0.5;
        double a = Util.pix2val(func, p1);
        double b = Util.pix2val(func, p2);

        // for TIMESYS other than UTC do the time scale transform to UTC
        double[] ta = transform(wcs, a);
        double[] tb = transform(wcs, b);
        double lower = ta[0] + ta[1];
        double upper = tb[0] + tb[1];
        return new Interval<Double>(Math.min(lower, upper), Math.max(lower, upper));
    }

    // check that the provided wcs includes supported combination of CTYPE, CUNIT, and TIMESYS
    private static void validateWCS(TemporalWCS wcs) {
        StringBuilder sb = new StringBuilder();
        String ctype = wcs.getAxis().getAxis().getCtype();
        if (wcs.timesys != null && SUPPORTED_TIMESYS.contains(wcs.timesys)
                && (ctype.equals(wcs.timesys) || ctype.equals(COMPAT_CTYPE))) {
            // OK
        } else if (wcs.timesys == null && SUPPORTED_TIMESYS.contains(ctype)) {
            // OK
        } else {
            sb.append("unexpected TIMESYS, CTYPE: ").append(wcs.timesys).append(",").append(ctype);
        }
        
        // TODO: with wcs.mjdref != null, CUNIT could be in alt units (eg sec)
        String cunit = wcs.getAxis().getAxis().cunit;
        if (!TARGET_CUNIT.equals(cunit)) {
            sb.append("unexpected CUNIT: ").append(cunit);
        }
        
        if (sb.length() > 0) {
            throw new UnsupportedOperationException(sb.toString());
        }
    }
    
    private static double pix2val(TemporalWCS wcs, CoordFunction1D func, double pix) {
        validateWCS(wcs);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        double ret = Util.pix2val(func, pix);
        if (wcs.mjdref != null) {
            ret += wcs.mjdref;
        }
        return ret;
    }

    public static double val2pix(TemporalWCS wcs, CoordFunction1D func, double val) {
        validateWCS(wcs);

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref

        if (wcs.mjdref != null) {
            val -= wcs.mjdref;
        }
        return Util.val2pix(func, val);
    }

    private static double[] transform(TemporalWCS wcs, double mjd) {
        String timeScale;
        if (wcs.timesys != null) {
            timeScale = wcs.timesys;
        } else {
            timeScale = wcs.getAxis().getAxis().getCtype();
        }

        double mjdref = wcs.mjdref == null ? 0.0 : wcs.mjdref;
        switch (timeScale) {
            case TAI_TIMESYS:
                return tai2utc(mjdref, mjd);
            case TT_TIMESYS:
                return tt2utc(mjdref, mjd);
            default:
                return new double[] {mjdref ,mjd};
        }
    }

    /**
     * Transform the time scale of a Modified Julian Date from TAI to UTC.
     *
     * @param mjdref the Modified Julian Date reference value.
     * @param mjd the Modified Julian Date.
     * @return mjd in the UTC time scale.
     */
    public static double[] tai2utc(double mjdref, double mjd) {
        // ERFALib expects Julian Date, convert MJD reference value to JD
        double jdref = mjdref + MJD2JD_OFFSET;
        double[] utc;
        try {
            utc = ERFALib.tai2utc(jdref, mjd);
        } catch (ERFALibException e) {
            String msg = String.format("error transforming %f from TAI to UTC: %s",
                                       mjd, e.getMessage());
            throw new IllegalStateException(msg, e);
        } catch (DubiousYearException | UnacceptableDateException e) {
            String msg = String.format("unable to transform %f from TAI to UTC: %s",
                                       mjd, e.getMessage());
            throw new IllegalArgumentException(msg, e);
        }
        // convert JD reference value back to MJD
        return new double[]{utc[0] - MJD2JD_OFFSET, utc[1]};
    }

    /**
     * Transform the time scale of a Modified Julian Date from TT to UTC.
     *
     * @param mjdref the Modified Julian Date reference value.
     * @param mjd the Modified Julian Date.
     * @return mjd in the UTC time scale.
     */
    public static double[] tt2utc(double mjdref, double mjd) {
        // ERFALib expects Julian Date, convert MJD reference value to JD
        double jdref = mjdref + MJD2JD_OFFSET;
        double[] utc;
        try {
            utc = ERFALib.tt2utc(jdref, mjd);
        } catch (ERFALibException e) {
            String msg = String.format("error transforming %f from TT to UTC: %s",
                                       mjd, e.getMessage());
            throw new IllegalStateException(msg, e);
        } catch (DubiousYearException | UnacceptableDateException e) {
            String msg = String.format("unable to transform %f from TT to UTC: %s",
                                       mjd, e.getMessage());
            throw new IllegalArgumentException(msg, e);
        }
        // convert JD reference value back to MJD
        return new double[]{utc[0] - MJD2JD_OFFSET, utc[1]};
    }

}
