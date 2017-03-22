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
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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

    private static void computeIdentifiers(Observation o, Plane p)
    {
        // publisherID: ivo://<authority>/<collection>?<observationID>/<productID>
        // TODO: where to get authority??
        URI resourceID = URI.create("ivo://cadc.nrc.ca/" + o.getCollection());
        p.planeURI = new PlaneURI(o.getURI(), p.getProductID());
        p.publisherID = new PublisherID(resourceID, o.getObservationID(), p.getProductID());
    }
     
    public static void clearTransientState(Plane p)
    {
        p.publisherID = null;
        p.position = null;
        p.energy = null;
        p.time = null;
        p.polarization = null;
        // clear metaRelease to children
        for (Artifact a : p.getArtifacts())
        {
            a.metaRelease = null;
            for (Part pp : a.getParts())
            {
                pp.metaRelease = null;
                for (Chunk c : pp.getChunks())
                {
                    c.metaRelease = null;
                }
            }
        }
    }
    
    public static void computeTransientState(Observation o, Plane p)
    {
        computeIdentifiers(o, p);
        computePosition(p);
        computeEnergy(p);
        computeTime(p);
        computePolarization(p);
        
        propagateMetaRelease(p);
    }
    
    private static void computePosition(Plane p)
    {
        try
        {
            p.position = PositionUtil.compute(p.getArtifacts());
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

    private static void propagateMetaRelease(Plane p)
    {
        // propagate metaRelease to children of the plane
        for (Artifact a : p.getArtifacts())
        {
            a.metaRelease = p.metaRelease;
            for (Part pp : a.getParts())
            {
                pp.metaRelease = p.metaRelease;
                for (Chunk c : pp.getChunks())
                {
                    c.metaRelease = p.metaRelease;
                }
            }
        }
    }
    
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
                for (Chunk c : p.getChunks())
                {
                    if (ProductType.SCIENCE.equals(c.productType))
                        return ProductType.SCIENCE;
                    if (ProductType.CALIBRATION.equals(c.productType))
                        ret = ProductType.CALIBRATION;
                }
            }
        }
        return ret;
    }
    
    
    public static boolean useChunk(ProductType atype, ProductType ptype, ProductType ctype, ProductType matches)
    {
        if (matches == null)
            return false;
        if (ctype != null && !matches.equals(ctype))
        {
            log.debug("useChunk=false: Chunk.productType="+ctype);
            return false;
        }
        if (ptype != null && !matches.equals(ptype))
        {
            log.debug("useChunk=false: Part.productType="+ptype);
            return false;
        }
        // artifact.productType never null
        if (matches.equals(atype))
        {
            log.debug("useChunk=true: Artifact.productType="+atype);
            return true;
        }
        
        
        log.debug("useChunk=false: productType="+atype + "," + ptype + "," + ctype);
        return false;
    }
    
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
        SubInterval snew = si;
        
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
            // merge all overlapping sub-intervals one interval
            // compute the outer bounds of the sub-intervals
            if (!tmp.isEmpty())
            {
                double lb = si.getLower();
                double ub = si.getUpper();
                for (SubInterval s : tmp)
                {
                    if (lb > s.getLower())
                        lb = s.getLower();
                    if (ub < s.getUpper())
                        ub = s.getUpper();
                }
                snew = new SubInterval(lb, ub);
            }
        }
        // insert new sub to preserve order
        boolean added = false;
        for (int i=0; i<samples.size(); i++)
        {
            SubInterval ss = samples.get(i);
            if (snew.getLower() < ss.getLower())
            {
                samples.add(i, snew);
                added = true;
                break;
            }
        }
        if (!added)
            samples.add(snew);
    }
}
