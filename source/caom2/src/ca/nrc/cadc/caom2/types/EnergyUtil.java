
package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyBand;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.util.EnergyConverter;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.WCSWrapper;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.WCSKeywords;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Utility class for 1-d Time calculations.
 *
 * @author pdowler
 */
public final class EnergyUtil
{
    private static final Logger log = Logger.getLogger(EnergyUtil.class);
    
    private static final EnergyConverter conv = new EnergyConverter();

    private EnergyUtil() { }

    /**
     * Compute all possible energy metadata from the specified artifacts.
     * 
     * @param artifacts
     * @return
     * @throws NoSuchKeywordException
     * @throws WCSLibRuntimeException
     */
    public static Energy compute(Set<Artifact> artifacts)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        ProductType productType = Util.choseProductType(artifacts);
        log.debug("compute: " + productType);

        Energy e = new Energy();
        if (productType != null)
        {
            e.bounds = computeBounds(artifacts, productType);
            e.dimension = computeDimensionFromRangeBounds(artifacts, productType);
            if (e.dimension == null)
                e.dimension = computeDimensionFromWCS(e.bounds, artifacts, productType);
            e.sampleSize = computeSampleSize(artifacts, productType);
            e.resolvingPower = computeResolution(artifacts, productType);
            e.bandpassName = computeBandpassName(artifacts, productType);
            e.transition = computeTransition(artifacts, productType);
            e.emBand = EnergyBand.getEnergyBand(e.bounds);
            e.restwav = computeRestWav(artifacts, productType);
        }

        return e;
    }

    /**
     * Computes the union.
     */
    static Interval computeBounds(Set<Artifact> artifacts, ProductType productType)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        double smooth = 0.02;
        List<SubInterval> subs = new ArrayList<SubInterval>();
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null)
                        {
                            CoordRange1D range = c.energy.getAxis().range;
                            CoordBounds1D bounds = c.energy.getAxis().bounds;
                            CoordFunction1D function = c.energy.getAxis().function;
                            if (range != null)
                            {
                                SubInterval s = toInterval(c.energy, range);
                                Util.mergeIntoList(s, subs, smooth);
                            }
                            else if (bounds != null)
                            {
                                for (CoordRange1D sr : bounds.getSamples())
                                {
                                    SubInterval s = toInterval(c.energy, sr);
                                    Util.mergeIntoList(s, subs, smooth);
                                }
                            }
                            else if (function != null)
                            {
                                SubInterval s = toInterval(c.energy, function);
                                Util.mergeIntoList(s, subs, smooth);
                            }
                        }
                    }
                }
            }
        }

        // compute the outer bounds of the sub-intervals
        double lb = Double.MAX_VALUE;
        double ub = Double.MIN_VALUE;
        for (SubInterval sub : subs)
        {
            lb = Math.min(lb, sub.getLower());
            ub = Math.max(ub, sub.getUpper());
        }

        if (subs.isEmpty())
            return null;
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
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null)
                        {
                            double num = Util.getNumPixels(c.energy.getAxis());
                            double tot = 0.0;

                            CoordRange1D range = c.energy.getAxis().range;
                            CoordBounds1D bounds = c.energy.getAxis().bounds;
                            CoordFunction1D function = c.energy.getAxis().function;
                            if (range != null)
                            {
                                SubInterval si = toInterval(c.energy, range);
                                tot = si.getUpper() - si.getLower();
                            }
                            else if (bounds != null)
                            {
                                for (CoordRange1D cr : bounds.getSamples())
                                {
                                    SubInterval si = toInterval(c.energy, cr);
                                    tot += si.getUpper() - si.getLower();
                                }
                            }
                            else if (function != null)
                            {
                                SubInterval si = toInterval(c.energy, function);
                                tot = si.getUpper() - si.getLower();
                            }
                            totSampleSize += tot;
                            numPixels += num;
                        }
                    }
                }
            }
        }

        if (totSampleSize > 0.0 && numPixels > 0.0)
        {
            double ret = totSampleSize / numPixels;
            log.debug("computeSampleSize: " + totSampleSize + "/" + numPixels + " = " + ret);
            return ret;
        }
        log.debug("computeSampleSize: " + totSampleSize + "/" + numPixels + " = null");
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
        throws NoSuchKeywordException
    {
        if (bounds == null)
            return null;
        
        SpectralWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null && c.energy.getAxis().function != null)
                        {
                            num++;
                            double ss = c.energy.getAxis().function.getDelta();
                            if (ss >= scale)
                            {
                                scale = ss;
                                sw = c.energy;
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
        
        WCSKeywords kw = new WCSWrapper(sw,1);
        Transform trans = new Transform(kw);
        
        String ctype = sw.getAxis().getAxis().getCtype();
        if ( !ctype.startsWith(EnergyConverter.CORE_CTYPE) )
        {
            log.debug("toInterval: transform from " + ctype + " to " + EnergyConverter.CORE_CTYPE + "-???");
            kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
            trans = new Transform(kw);
        }
        
        // NOTE: this works correctly because wcslib assumes arg is in SI units (m)
        Transform.Result pix;
        pix = trans.sky2pix( new double[] { bounds.getLower() });
        double x1 = pix.coordinates[0];
        pix = trans.sky2pix( new double[] { bounds.getUpper() });
        double x2 = pix.coordinates[0];
        
        return new Long((long) Math.abs(x2 - x1));
    }
    
    /**
     * Compute dimensionality (number of pixels).
     * 
     * @param wcsArray
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, ProductType productType)
    {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply add up the pixels from each chunk
        double numPixels = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null)
                        {
                            double num = Util.getNumPixels(c.energy.getAxis(), false);
                            log.debug("[computeDimension] num=" + num + ", numPixels="+numPixels);
                            numPixels += num;
                        }
                    }
                }
            }
        }
        
        if (numPixels > 0.0)
            return new Long((long) numPixels);
        return null;
    }
    
    /**
     * Compute the mean resolving power per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param wcs
     * @return resolving power
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
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null && c.energy.resolvingPower != null)
                        {
                            double num = Util.getNumPixels(c.energy.getAxis());
                            totResolution += c.energy.resolvingPower * num;
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

    static EnergyTransition computeTransition(Set<Artifact> artifacts, ProductType productType)
    {
        EnergyTransition ret = null;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null)
                        {
                            if (ret == null)
                                ret = c.energy.transition;
                            // check for conflict and return null immediately
                            if (ret != null && c.energy.transition != null)
                            {
                                if ( !ret.getSpecies().equals(c.energy.transition.getSpecies())
                                        || !ret.getTransition().equals(c.energy.transition.getTransition()) )
                                    return null;
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }
    
    static String computeBandpassName(Set<Artifact> artifacts, ProductType productType)
    {
        String ret = null;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        if (c.energy != null)
                        {
                            if (ret == null)
                                ret = c.energy.bandpassName;
                            // check for conflict and return null immediately
                            if (ret != null && c.energy.bandpassName != null)
                            {
                                if ( !ret.equals(c.energy.bandpassName) )
                                    return null;
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }
    
    /**
     * Compute the mean rest wavelength per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param wcs
     * @return rest wavelength in meters
     */
    static Double computeRestWav(Set<Artifact> artifacts, ProductType productType)
    {
        double minW = Double.MAX_VALUE;
        double maxW = 0.0;
        boolean found = false;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if ( Util.useChunk(a.productType, p.productType, c.productType, productType) )
                    {
                        Double rw = getRestWav(c.energy);
                        if (rw != null)
                        {
                            minW = Math.min(minW, rw);
                            maxW = Math.max(maxW, rw);
                            found = true;
                        }
                    }
                }
            }
        }
        if (found)
        {
            double delta = Math.abs(maxW - minW);
            Double ret = minW + delta/2.0;
            if (delta/ret < 0.01)
                return ret;
        }
        return null;
    }
    
    private static Double getRestWav(SpectralWCS w)
    {
        if (w == null)
            return null;
        if (w.restwav != null)
            return w.restwav;
        if (w.restfrq != null)
            return conv.toMeters(w.restfrq, EnergyConverter.BASE_UNIT_FREQ);
        return null;
    }

    private static SubInterval toInterval(SpectralWCS wcs, CoordRange1D r)
    {
        double a = r.getStart().val;
        double b = r.getEnd().val;

        String specsys = wcs.getSpecsys();
        if ( !EnergyConverter.CORE_SPECSYS.equals(specsys) )
        {
            a = conv.convertSpecsys(a, specsys);
            b = conv.convertSpecsys(b, specsys);
        }

        String ctype = wcs.getAxis().getAxis().getCtype();
        String cunit = wcs.getAxis().getAxis().getCunit();
        if ( !ctype.startsWith(EnergyConverter.CORE_CTYPE)
                || !EnergyConverter.CORE_UNIT.equals(cunit) )
        {
            log.debug("toInterval: converting " + a + cunit);
            a = conv.convert(a, EnergyConverter.CORE_CTYPE, cunit);
            log.debug("toInterval: converting " + b + cunit);
            b = conv.convert(b, EnergyConverter.CORE_CTYPE, cunit);
        }
        return new SubInterval(Math.min(a,b), Math.max(a,b));
    }

    private static SubInterval toInterval(SpectralWCS wcs, CoordFunction1D f)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        // convert to TARGET_CTYPE
        WCSKeywords kw = new WCSWrapper(wcs,1);
        Transform trans = new Transform(kw);
        String ctype = wcs.getAxis().getAxis().getCtype();
        if ( !ctype.startsWith(EnergyConverter.CORE_CTYPE) )
        {
            log.debug("toInterval: transform from " + ctype + " to " + EnergyConverter.CORE_CTYPE + "-???");
            kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
            trans = new Transform(kw);
        }
        double naxis = kw.getDoubleValue("NAXIS1"); // axis set to 1 above
        double p1 = 0.5;
        double p2 = naxis + 0.5;
        Transform.Result start = trans.pix2sky( new double[] { p1 });
        Transform.Result end = trans.pix2sky( new double[] { p2 });

        double a = start.coordinates[0];
        double b = end.coordinates[0];
        log.debug("toInterval: wcslib returned " + a + start.units[0] + "," + b + end.units[0]);

        String specsys = wcs.getSpecsys();
        if ( !EnergyConverter.CORE_SPECSYS.equals(specsys) )
        {
            a = conv.convertSpecsys(a, specsys);
            b = conv.convertSpecsys(b, specsys);
        }

        // wcslib convert to WAVE-??? but units might be a multiple of EnergyConverter.CORE_UNIT
        String cunit = start.units[0]; // assume same as end.units[0]
        if ( !EnergyConverter.CORE_UNIT.equals(cunit) )
        {
            log.debug("toInterval: converting " + a + " " + cunit);
            a = conv.convert(a, EnergyConverter.CORE_CTYPE, cunit);
            log.debug("toInterval: converting " + b + " " + cunit);
            b = conv.convert(b, EnergyConverter.CORE_CTYPE, cunit);
        }

        return new SubInterval(Math.min(a,b), Math.max(a,b));
    }

    /**
     * Compute a pixel cutout for the specified bounds. The bounds are assumed to be
     * barycentric wavelength in meters.
     *
     * @param wcs
     * @param bounds
     * @return int[2] with the pixel bounds, int[0] if all pixels are included, or
     *         null if no pixels are included
     */
    public static long[] getBounds(SpectralWCS wcs, Interval bounds)
        throws NoSuchKeywordException, WCSLibRuntimeException
    {
        if (wcs.getAxis().function != null)
        {
            // convert wcs to energy axis interval
            SubInterval si =  toInterval(wcs, wcs.getAxis().function);
            Interval wbounds = new Interval(si.lower, si.upper);
            //log.info("getBounds: wcs = " + wbounds);

            // compute intersection
            Interval inter = Interval.intersection(wbounds, bounds);
            //log.info("getBounds: intersection = " + inter);
            if (inter == null)
            {
                log.debug("bounds INTERSECT wcs.function == null");
                return null;
            }

            double a = inter.getLower();
            double b = inter.getUpper();

            WCSKeywords kw = new WCSWrapper(wcs,1);
            Transform trans = new Transform(kw);

            String ctype = wcs.getAxis().getAxis().getCtype();
            if ( !ctype.startsWith(EnergyConverter.CORE_CTYPE) )
            {
                //log.debug("toInterval: transform from " + ctype + " to " + EnergyConverter.CORE_CTYPE + "-???");
                kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
                trans = new Transform(kw);
            }

            Transform.Result p1 = trans.sky2pix(new double[] { a });
            //log.debug("getBounds: sky2pix " + a + " -> " + p1.coordinates[0] + p1.units[0]);
            Transform.Result p2 = trans.sky2pix(new double[] { b });
            //log.debug("getBounds: sky2pix " + b + " -> " + p2.coordinates[0] + p2.units[0]);

            // values can be inverted if WCS is in freq or energy instead of wavelength
            long x1 = (long) Math.min(p1.coordinates[0], p2.coordinates[0]);
            long x2 = (long) Math.max(p1.coordinates[0], p2.coordinates[0]);

            return doClipCheck(wcs.getAxis().function.getNaxis().longValue(), x1, x2);
        }
        
        if (wcs.getAxis().bounds != null)
        {
            //log.info("getBounds: " + bounds);
            // find min and max sky coords
            double pix1 = Double.MAX_VALUE;
            double pix2 = Double.MIN_VALUE;
            long maxPixValue = 0;
            boolean foundOverlap = false;
            for (CoordRange1D tile : wcs.getAxis().bounds.getSamples())
            {
                //log.warn("getBounds: tile = " + tile);
                maxPixValue = Math.max(maxPixValue, (long) tile.getEnd().pix);
                SubInterval bwmRange = toInterval(wcs, tile);
                Interval wbounds = new Interval(bwmRange.getLower(), bwmRange.getUpper());
                // compute intersection
                Interval inter = Interval.intersection(wbounds, bounds);
                //log.warn("getBounds: " + inter + " = " + wbounds + " X " + bounds);
                if (inter != null)
                {
                    pix1 = Math.min(pix1, tile.getStart().pix);
                    pix2 = Math.max(pix2, tile.getEnd().pix);
                    //log.warn("getBonds: pix range is now " + pix1 + "," + pix2);
                    foundOverlap = true;
                }
            }
            if (foundOverlap)
            {
                long p1 = (long) (pix1 + 0.5); // round up
                long p2 = (long) pix2;         // round down
                
                return doClipCheck(maxPixValue, p1, p2);
            }
            log.debug("bounds INTERSECT wcs.bounds == null");
            return null;
        }
        
        if (wcs.getAxis().range != null)
        {
            // can only check for complete non-overlap
            SubInterval bwmRange = toInterval(wcs, wcs.getAxis().range);
            Interval wbounds = new Interval(bwmRange.getLower(), bwmRange.getUpper());
            // compute intersection
            Interval inter = Interval.intersection(wbounds, bounds);
            //log.info("getBounds: intersection = " + inter);
            if (inter == null)
            {
                log.debug("bounds INTERSECT wcs.range == null");
                return null;
            }
            return new long[0]; // overlap
        }
        
        return null;
    }

    private static long[] doClipCheck(long len, long x1, long x2)
    {
        if (x1 < 1)
            x1 = 1;
        if (x2 > len)
            x2 = len;

        // validity check
        if (x1 >= len || x2 <= 1) // no pixels included
        {
            return null;
        }
        if (x1 == 1 && x2 == len) // all pixels includes
        {
            return new long[0];
        }
        return new long[] { x1, x2 }; // an actual cutout
    }
}
