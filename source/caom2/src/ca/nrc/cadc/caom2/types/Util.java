
package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * TODO.
 *
 * @author pdowler
 */
public final class Util
{
    private static final Logger log = Logger.getLogger(Util.class);
    
    private Util() { }

    static double roundToNearest(double d)
    {
        return Math.floor(d+0.5);
    }

    public static ProductType choseProductType(Set<Artifact> artifacts)
    {
        ProductType ret = null;
        for (Artifact a : artifacts)
        {
            if (ProductType.SCIENCE.equals(a.getProductType()))
                return ProductType.SCIENCE;
            if (ProductType.CALIBRATION.equals(a.getProductType()))
                ret = ProductType.CALIBRATION;
            for (Part p : a.getParts())
            {
                if (ProductType.SCIENCE.equals(p.productType))
                    return ProductType.SCIENCE;
                if (ProductType.CALIBRATION.equals(p.productType))
                    ret = ProductType.CALIBRATION;
                /*
                for (Chunk c : p.getChunks())
                {
                    if (ProductType.SCIENCE.equals(c.productType))
                        return ProductType.SCIENCE;
                    if (ProductType.CALIBRATION.equals(c.productType))
                        ret = ProductType.CALIBRATION;
                }
                */
            }
        }
        return ret;
    }
    
    public static boolean usePart(ProductType atype, ProductType ptype, ProductType matches)
    {
        if (matches == null)
            return false;
        if (atype != null && matches.equals(atype))
        {
            log.debug("useChunk: Artifact.productType="+atype);
            return true;
        }
        if (ptype != null && matches.equals(ptype))
        {
            log.debug("useChunk: Part.productType="+ptype);
            return true;
        }
        log.debug("useChunk: productType="+atype + "," + ptype);
        return false;
    }
    /*
    public static boolean useChunk(ProductType atype, ProductType ptype, ProductType ctype, ProductType matches)
    {
        if (matches == null)
            return false;
        if (atype != null && matches.equals(atype))
        {
            log.debug("useChunk: Artifact.productType="+atype);
            return true;
        }
        if (ptype != null && matches.equals(ptype))
        {
            log.debug("useChunk: Part.productType="+ptype);
            return true;
        }
        if (ctype != null && matches.equals(ctype))
        {
            log.debug("useChunk: Chunk.productType="+ctype);
            return true;
        }
        log.debug("useChunk: productType="+atype + "," + ptype + "," + ctype);
        return false;
    }
    */
    
    static double getNumPixels(CoordAxis2D axis)
    {
        CoordRange2D range = axis.range;
        CoordBounds2D bounds = axis.bounds;
        CoordFunction2D function = axis.function;
        if (range != null)
        {
            double dx = (range.getEnd().getCoord1().pix - range.getStart().getCoord1().pix);
            double dy = (range.getEnd().getCoord2().pix - range.getStart().getCoord2().pix);
            return Math.abs(dx*dy);
        }
        else if (function != null)
        {
            double dx = function.getDimension().naxis1;
            double dy = function.getDimension().naxis2;
            return Math.abs(dx*dy);
        }
        return 0.0;
    }
    
    static double getPixelScale(CoordAxis2D axis)
    {
        CoordRange2D range = axis.range;
        CoordBounds2D bounds = axis.bounds;
        CoordFunction2D function = axis.function;
        if (range != null)
        {
            double xs = (range.getEnd().getCoord1().val - range.getStart().getCoord1().val) /
                (range.getEnd().getCoord1().pix - range.getStart().getCoord1().pix);
            xs = Math.abs(xs);
            double ys = (range.getEnd().getCoord2().val - range.getStart().getCoord2().val) /
                (range.getEnd().getCoord2().pix - range.getStart().getCoord2().pix);
            ys = Math.abs(ys);
            //return Math.max(xs, ys); // maximum
            return (xs + ys)/2.0;      // average
        }
        else if (function != null)
        {
            return getPixelScale(function);
        }
        return 0.0;
    }
    static double getPixelScale(CoordFunction2D function)
    {
        double xs = Math.sqrt(function.getCd11()*function.getCd11() + function.getCd21()*function.getCd21());
        double ys = Math.sqrt(function.getCd12()*function.getCd12() + function.getCd22()*function.getCd22());
        //return Math.max(xs, ys); // maximum
        return (xs + ys)/2.0;      // average
    }

    static double pix2val(CoordFunction1D function, double pix)
    {
        // compute at middle of pixel (whole number)
        double refPix = function.getRefCoord().pix; //Util.roundToNearest(function.getRefCoord().pix);
        //double refPix = Util.roundToNearest(function.getRefCoord().pix);
        return function.getRefCoord().val + function.getDelta() * (pix - refPix);
    }
    
    static double val2pix(CoordFunction1D function, double val)
    {
        double refVal = function.getRefCoord().val;
        return function.getRefCoord().pix + (val - refVal)/function.getDelta();
    }
    
    static double getNumPixels(CoordAxis1D axis)
    {
        return getNumPixels(axis, true);
    }
    static double getNumPixels(CoordAxis1D axis, boolean useFunc)
    {
        CoordRange1D range = axis.range;
        CoordBounds1D bounds = axis.bounds;
        CoordFunction1D function = axis.function;
        if (range != null)
        {
            return Math.abs(range.getEnd().pix - range.getStart().pix);
        }

        if (bounds != null)
        {
            // count number of distinct bins
            List<SubInterval> bins = new ArrayList<SubInterval>();
            for (CoordRange1D cr : bounds.getSamples())
            {
                SubInterval si = new SubInterval(cr.getStart().pix, cr.getEnd().pix);
                Util.mergeIntoList(si, bins, 0.0);
            }
            double ret = 0.0;
            for (SubInterval si : bins)
            {
                ret += Math.abs(si.getUpper() - si.getLower());
            }
            return ret;
        }
        if (useFunc && function != null)
        {
            return function.getNaxis();
        }
        return 0.0;
    }

    // merge a SubInterval into a List of SubInterval
    static void mergeIntoList(SubInterval si, List<SubInterval> samples, double unionScale)
    {
        //log.debug("[mergeIntoList] " + si.lower + "," + si.upper + " ->  " + samples.size());
        if (samples.size() > 0)
        {
            double f = unionScale*(si.getUpper() - si.getLower());
            double a = si.getLower() - f;
            double b = si.getUpper() + f;

            ArrayList<SubInterval> tmp = new ArrayList<SubInterval>(samples.size());

            // find intervals that overlap the new one, move from samples -> tmp
            for (int i=0; i<samples.size(); i++)
            {
                SubInterval s1 = (SubInterval) samples.get(i);
                f = unionScale*(s1.getUpper() - s1.getLower());
                double c = s1.getLower() - f;
                double d = s1.getUpper() + f;

                // [a,b] U [c,d]
                //System.out.println("[mergeIntoList] " + a + "," + b + " U " + c + "," + d);
                if ( b < c || d < a ) // no overlap
                {
                    //System.out.println("[mergeIntoList] no overlap: " + si + " and " + s1);
                }
                else
                {
                    //System.out.println("[mergeIntoList] ** overlap: " + si + " and " + s1);
                    tmp.add(s1);
                    samples.remove(s1);
                    i--;
                }
            }
            // merge all of them into one interval, eg the new one
            for (int i=0; i<tmp.size(); i++)
            {
                SubInterval s = (SubInterval) tmp.get(i);
                if (si.getLower() > s.getLower())
                    si.lower = s.getLower();
                if (si.getUpper() < s.getUpper())
                    si.upper = s.getUpper();
            }
        }
        // insert new sub to preserve order
        boolean added = false;
        for (int i=0; i<samples.size(); i++)
        {
            SubInterval ss = samples.get(i);
            if (si.getLower() < ss.getLower())
            {
                samples.add(i, si);
                added = true;
                break;
            }
        }
        if (!added)
            samples.add(si);
    }
}
