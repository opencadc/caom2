
package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.Time;
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
public final class TimeUtil
{
    private static final Logger log = Logger.getLogger(TimeUtil.class);
    
    public static double DEFAULT_UNION_SCALE = 0.10;

    // sort of a hack: we assume absolute MJD values in the TimeWCS which
    // is the default if MJDREF, JDREF, and DATEREF are all absent = 0.0 in FITS
    private static final String TARGET_TIMESYS = "UTC";
    private static final String TARGET_CTYPE = "TIME";
    private static final String TARGET_CUNIT = "d";

    private TimeUtil() { }
    
    public static Time compute(Set<Artifact> artifacts)
    {
        ProductType productType = Util.choseProductType(artifacts);
        log.debug("compute: " + productType);
        Time t = new Time();
        if (productType != null)
        {
            t.bounds = computeBounds(artifacts, productType);
            t.dimension = computeDimensionFromRangeBounds(artifacts, productType);
            if (t.dimension == null)
                t.dimension = computeDimensionFromWCS(t.bounds, artifacts, productType);
            t.resolution = computeResolution(artifacts, productType);
            t.sampleSize = computeSampleSize(artifacts, productType);
            t.exposure = computeExposureTime(artifacts, productType);
        }
        
        return t;
    }

    /**
     * Computes the union.
     */
    static Interval computeBounds(Set<Artifact> artifacts, ProductType productType)
    {
        double unionScale = 0.02;
        List<SubInterval> subs = new ArrayList<SubInterval>();
        
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        if (c.time != null)
                        {
                            CoordRange1D range = c.time.getAxis().range;
                            CoordBounds1D bounds = c.time.getAxis().bounds;
                            CoordFunction1D function = c.time.getAxis().function;
                            if (range != null)
                            {
                                SubInterval s = toInterval(c.time, range);
                                log.debug("[computeBounds] range -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            }
                            else if (bounds != null)
                            {
                                for (CoordRange1D cr : bounds.getSamples())
                                {
                                    SubInterval s = toInterval(c.time, cr);
                                    log.debug("[computeBounds] bounds -> sub: " + s);
                                    Util.mergeIntoList(s, subs, unionScale);
                                }
                            }
                            else if (function != null)
                            {
                                SubInterval s = toInterval(c.time, function);
                                log.debug("[computeBounds] function -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            }
                        }
                    }
                }
            }
        }
        if (subs.isEmpty())
            return null;
        
        // compute the outer bounds of the sub-intervals
        double lb = Double.MAX_VALUE;
        double ub = Double.MIN_VALUE;
        for (SubInterval sub : subs)
        {
            lb = Math.min(lb, sub.getLower());
            ub = Math.max(ub, sub.getUpper());
        }
        if (subs.size() == 1)
            return new Interval(lb, ub);
        return new Interval(lb, ub, subs);
    }

    

    /**
     * Compute mean sample size (pixel scale).
     * 
     * @param wcs
     * @return a new Polygon computed with the default union scale
     */
    static Double computeSampleSize(Set<Artifact> artifacts, ProductType productType)
    {
        // assumption: all pixels are distinct so we can just compute a weighted average
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        if (c.time != null)
                        {
                            CoordRange1D range = c.time.getAxis().range;
                            CoordBounds1D bounds = c.time.getAxis().bounds;
                            CoordFunction1D function = c.time.getAxis().function;

                            numPixels += Util.getNumPixels(c.time.getAxis());
                            if (range != null)
                            {
                                SubInterval si = toInterval(c.time, range);
                                totSampleSize += si.getUpper() - si.getLower();
                            }
                            else if (bounds != null)
                            {
                                for (CoordRange1D cr : bounds.getSamples())
                                {
                                    SubInterval si = toInterval(c.time, cr);
                                    totSampleSize += si.getUpper() - si.getLower();
                                }
                            }
                            else if (function != null)
                            {
                                SubInterval si = toInterval(c.time, function);
                                totSampleSize += si.getUpper() - si.getLower();
                            }
                        }
                    }
                }
            }
        }

        if (totSampleSize > 0.0 && numPixels > 0.0)
            return totSampleSize / numPixels;
        return null;
    }
    
    /**
     * Compute dimensionality (number of pixels) from WCS function. This method assumes
     * that the energy axis is roughly continuous (e.g. it ignores gaps).
     * 
     * @param wcsArray
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromWCS(Interval bounds, Set<Artifact> artifacts, ProductType productType)
    {
        log.debug("computeDimensionFromWCS: " + bounds);
        if (bounds == null)
            return null;
        
        // pick the WCS with the largest pixel size
        TemporalWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        if (c.time != null && c.time.getAxis().function != null)
                        {
                            num++;
                            double ss = Math.abs(c.time.getAxis().function.getDelta());
                            if (ss >= scale)
                            {
                                scale = ss;
                                sw = c.time;
                            }
                        }
                    }
                }
            }
        }
        if (sw == null)
            return null;
        
        if (sw.getAxis().function == null)
            return null;
        
        if (num == 1)
            return sw.getAxis().function.getNaxis();
        
        double x1 = val2pix(sw, sw.getAxis().function, bounds.getLower());
        double x2 = val2pix(sw, sw.getAxis().function, bounds.getUpper());
        
        log.debug("computeDimensionFromWCS: " + x1 + "," + x2);
        return new Long (Math.round(Math.abs(x2 - x1)));
    }
    
    /**
     * Compute dimensionality (number of pixels).
     * 
     * @param wcsArray
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, ProductType productType)
    {
        // assumption: all   pixels are distinct so just add up the number of pixels
        double numPixels = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        double n = Util.getNumPixels(c.time.getAxis(), false);
                        numPixels += n;
                    }
                }
            }
        }
        
        if (numPixels > 0.0)
        {
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
    static Double computeExposureTime(Set<Artifact> artifacts, ProductType productType)
    {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply compute the mean values time weighted by number of pixels in
        // the chunk
        double totExposureTime = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        if (c.time != null && c.time.exposure != null)
                        {
                            double num = Util.getNumPixels(c.time.getAxis());
                            totExposureTime += c.time.exposure * num;
                            numPixels += num;
                        }
                    }
                }
            }
        }

        if (totExposureTime > 0.0 && numPixels > 0.0)
            return new Double(totExposureTime / numPixels);
        return null;
    }

    /**
     * Compute the mean resolution per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param wcs
     * @return exposure time in seconds
     */
    static Double computeResolution(Set<Artifact> artifacts, ProductType productType)
    {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply compute the mean values time weighted by number of pixels in
        // the chunk
        double totResolution = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                //for (Chunk c : p.getChunks())
                if (p.chunk != null)
                {   
                    Chunk c = p.chunk;
                    if ( Util.usePart(a.getProductType(), p.productType, productType) )
                    {
                        if (c.time != null && c.time.resolution != null)
                        {
                            double num = Util.getNumPixels(c.time.getAxis());
                            totResolution += c.time.resolution * num;
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

    private static SubInterval toInterval(TemporalWCS wcs, CoordRange1D r)
    {
        double a = r.getStart().val;
        double b = r.getEnd().val;

        String ctype = wcs.getAxis().getAxis().getCtype();
        if ( !( TARGET_TIMESYS.equals(wcs.timesys) && TARGET_CTYPE.equals(ctype) )
                && !( wcs.timesys == null && TARGET_TIMESYS.equals(ctype) ) )
        {
            throw new UnsupportedOperationException("unexpected TIMESYS, CTYPE: " + wcs.timesys + "," + ctype);
        }

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref
        
        String cunit = wcs.getAxis().getAxis().getCunit();
        if ( ! TARGET_CUNIT.equals(cunit) )
        {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null)
        {
            a += wcs.mjdref.doubleValue();
            b += wcs.mjdref.doubleValue();
        }
        
        return new SubInterval(Math.min(a,b), Math.max(a,b));
    }

    private static SubInterval toInterval(TemporalWCS wcs, CoordFunction1D func)
    {
        double p1 = 0.5;
        double p2 = func.getNaxis().doubleValue() + 0.5;
        double a = Util.pix2val(func, p1);
        double b = Util.pix2val(func, p2);

        String ctype = wcs.getAxis().getAxis().getCtype();
        if ( !( TARGET_TIMESYS.equals(wcs.timesys) && TARGET_CTYPE.equals(ctype) )
                && !( wcs.timesys == null && TARGET_TIMESYS.equals(ctype) ) )
        {
            throw new UnsupportedOperationException("unexpected TIMESYS, CTYPE: " + wcs.timesys + "," + ctype);
        }

        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref
        
        String cunit = wcs.getAxis().getAxis().getCunit();
        if ( ! TARGET_CUNIT.equals(cunit) )
        {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null)
        {
            a += wcs.mjdref.doubleValue();
            b += wcs.mjdref.doubleValue();
        }
        
        return new SubInterval(Math.min(a,b), Math.max(a,b));
    }
    
    private static double pix2val(TemporalWCS wcs, CoordFunction1D func, double pix)
    {
        double ret = Util.pix2val(func, pix);
        
        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref
        
        String cunit = wcs.getAxis().getAxis().getCunit();
        if ( ! TARGET_CUNIT.equals(cunit) )
        {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }

        if (wcs.mjdref != null)
        {
            ret += wcs.mjdref.doubleValue();
        }
        
        return ret;
    }
    
    private static double val2pix(TemporalWCS wcs, CoordFunction1D func, double val)
    {
        if (wcs.mjdref != null)
            val -= wcs.mjdref.doubleValue();
        
        double ret = Util.val2pix(func, val);
        
        // TODO: if mjdref has a value then the units of axis values could be any time
        // units, like days, hours, minutes, seconds, and smaller since they are offsets
        // from mjdref
        
        String cunit = wcs.getAxis().getAxis().getCunit();
        if ( ! TARGET_CUNIT.equals(cunit) )
        {
            throw new UnsupportedOperationException("unexpected CUNIT: " + cunit);
        }
        
        return ret;
    }
}
