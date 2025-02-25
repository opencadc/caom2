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

import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyBand;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.util.EnergyConverter;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.dali.DoubleInterval;
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
public final class EnergyUtil {
    private static final Logger log = Logger.getLogger(EnergyUtil.class);

    private static final EnergyConverter conv = new EnergyConverter();

    private EnergyUtil() {
    }

    static class ComputedBounds {
        DoubleInterval bounds;
        List<DoubleInterval> samples;
    }
    
    /**
     * Compute all possible energy metadata from the specified artifacts.
     *
     * @param artifacts
     * @return
     * @throws NoSuchKeywordException
     * @throws WCSLibRuntimeException
     */
    public static Energy compute(Set<Artifact> artifacts)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        DataLinkSemantics productType = DataLinkSemantics.THIS;
        log.debug("compute: " + productType);

        ComputedBounds cb = computeBounds(artifacts, productType);
        if (cb != null) {
            Energy e = new Energy(cb.bounds);
            e.getSamples().addAll(cb.samples);
            e.dimension = computeDimensionFromRangeBounds(artifacts, productType);
            if (e.dimension == null) {
                e.dimension = computeDimensionFromWCS(e.getBounds(), artifacts, productType);
            }
            e.sampleSize = computeSampleSize(artifacts, productType);
            e.resolvingPower = computeResolution(artifacts, productType);
            e.bandpassName = computeBandpassName(artifacts, productType);
            e.transition = computeTransition(artifacts, productType);
            e.getEnergyBands().addAll(EnergyBand.getEnergyBand(e.getBounds()));
            e.rest = computeRestWav(artifacts, productType);
            
            return e;
        }

        return null;
    }

    /**
     * Computes the union.
     */
    static ComputedBounds computeBounds(Set<Artifact> artifacts, DataLinkSemantics productType)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        double smooth = 0.02;
        List<DoubleInterval> subs = new ArrayList<>();
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null) {
                            CoordRange1D range = c.energy.getAxis().range;
                            CoordBounds1D bounds = c.energy.getAxis().bounds;
                            CoordFunction1D function = c.energy.getAxis().function;
                            if (range != null) {
                                log.debug("computeBounds: " + range);
                                DoubleInterval s = EnergyUtil.toInterval(c.energy, range);
                                Util.mergeIntoList(s, subs, smooth);
                            } else if (bounds != null) {
                                log.debug("computeBounds: " + bounds);
                                for (CoordRange1D sr : bounds.getSamples()) {
                                    DoubleInterval s = EnergyUtil.toInterval(c.energy, sr);
                                    Util.mergeIntoList(s, subs, smooth);
                                }
                            } else if (function != null) {
                                log.debug("computeBounds: " + function);
                                DoubleInterval s = toInterval(c.energy, function);
                                Util.mergeIntoList(s, subs, smooth);
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
        for (DoubleInterval sub : subs) {
            lb = Math.min(lb, sub.getLower());
            ub = Math.max(ub, sub.getUpper());
        }

        ComputedBounds ret = new ComputedBounds();
        ret.bounds = new DoubleInterval(lb, ub);
        ret.samples = subs;
        return ret;
    }

    /**
     * Compute mean sample size (pixel scale).
     *
     * @param artifacts
     * @param productType
     * @return a new Polygon computed with the default union scale
     */
    static Double computeSampleSize(Set<Artifact> artifacts, DataLinkSemantics productType)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        double totSampleSize = 0.0;
        double numPixels = 0.0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null) {
                            double num = Util.getNumPixels(c.energy.getAxis());
                            double tot = 0.0;

                            CoordRange1D range = c.energy.getAxis().range;
                            CoordBounds1D bounds = c.energy.getAxis().bounds;
                            CoordFunction1D function = c.energy.getAxis().function;
                            if (range != null) {
                                DoubleInterval si = EnergyUtil.toInterval(c.energy, range);
                                tot = si.getUpper() - si.getLower();
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    DoubleInterval si = EnergyUtil.toInterval(c.energy, cr);
                                    tot += si.getUpper() - si.getLower();
                                }
                            } else if (function != null) {
                                DoubleInterval si = toInterval(c.energy, function);
                                tot = si.getUpper() - si.getLower();
                            }
                            totSampleSize += tot;
                            numPixels += num;
                        }
                    }
                }
            }
        }

        if (totSampleSize > 0.0 && numPixels > 0.0) {
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
     * @param bounds
     * @param artifacts
     * @return number of pixels (approximate)
     * @params productType
     */
    static Long computeDimensionFromWCS(DoubleInterval bounds, Set<Artifact> artifacts, DataLinkSemantics productType)
        throws NoSuchKeywordException {
        log.debug("computeDimensionFromWCS: " + bounds + " " + productType);
        if (bounds == null) {
            return null;
        }

        SpectralWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null && c.energy.getAxis().function != null) {
                            num++;
                            double ss = Math.abs(c.energy.getAxis().function.getDelta());
                            if (ss >= scale) {
                                scale = ss;
                                sw = c.energy;
                            }
                        }
                    }
                }
            }
        }

        log.debug("computeDimensionFromWCS: " + bounds + " numA: " + num + " wcs: " + sw);

        if (sw == null) {
            return null;
        }

        if (sw.getAxis().function == null) {
            return null;
        }

        if (num == 1) {
            return sw.getAxis().function.getNaxis();
        }

        WCSKeywords kw = new WCSWrapper(sw, 1);
        Transform trans = new Transform(kw);

        String ctype = sw.getAxis().getAxis().getCtype();
        if (!ctype.startsWith(EnergyConverter.CORE_CTYPE)) {
            log.debug("toSampledInterval: transform from " + ctype + " to " + EnergyConverter.CORE_CTYPE + "-???");
            kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
            trans = new Transform(kw);
        }

        // NOTE: this works correctly because wcslib assumes arg is in SI units (m)
        Transform.Result pix;
        pix = trans.sky2pix(new double[] {bounds.getLower()});
        double x1 = pix.coordinates[0];
        pix = trans.sky2pix(new double[] {bounds.getUpper()});
        double x2 = pix.coordinates[0];

        log.debug("computeDimensionFromWCS: " + x1 + "," + x2);
        return new Long(Math.round(Math.abs(x2 - x1)));
    }

    /**
     * Compute dimensionality (number of pixels).
     *
     * @param artifacts
     * @param productType
     * @return number of pixels (approximate)
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, DataLinkSemantics productType) {
        // ASSUMPTION: different Chunks (different WCS) are always different pixels
        // so we simply add up the pixels from each chunk
        double numPixels = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null) {
                            double num = Util.getNumPixels(c.energy.getAxis(), false);
                            log.debug("[computeDimension] num=" + num + ", numPixels=" + numPixels);
                            numPixels += num;
                        }
                    }
                }
            }
        }

        if (numPixels > 0.0) {
            return new Long((long) numPixels);
        }
        return null;
    }

    /**
     * Compute the mean resolving power per chunk, weighted by the number of pixels.
     * in the chunk.
     *
     * @param artifacts
     * @param productType
     * @return resolving power
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
                        if (c.energy != null && c.energy.resolvingPower != null) {
                            double num = Util.getNumPixels(c.energy.getAxis());
                            totResolution += c.energy.resolvingPower * num;
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

    static EnergyTransition computeTransition(Set<Artifact> artifacts, DataLinkSemantics productType) {
        EnergyTransition ret = null;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null) {
                            if (ret == null) {
                                ret = c.energy.transition;
                            }
                            // check for conflict and return null immediately
                            if (ret != null && c.energy.transition != null) {
                                if (!ret.getSpecies().equals(c.energy.transition.getSpecies())
                                    || !ret.getTransition().equals(c.energy.transition.getTransition())) {
                                    return null;
                                }
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    static String computeBandpassName(Set<Artifact> artifacts, DataLinkSemantics productType) {
        String ret = null;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.energy != null) {
                            if (ret == null) {
                                ret = c.energy.bandpassName;
                            }
                            // check for conflict and return null immediately
                            if (ret != null && c.energy.bandpassName != null) {
                                if (!ret.equals(c.energy.bandpassName)) {
                                    return null;
                                }
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
     * @param artifacts
     * @param productType
     * @return rest wavelength in meters
     */
    static Double computeRestWav(Set<Artifact> artifacts, DataLinkSemantics productType) {
        double minW = Double.MAX_VALUE;
        double maxW = 0.0;
        boolean found = false;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        Double rw = getRestWav(c.energy);
                        if (rw != null) {
                            minW = Math.min(minW, rw);
                            maxW = Math.max(maxW, rw);
                            found = true;
                        }
                    }
                }
            }
        }
        if (found) {
            double delta = Math.abs(maxW - minW);
            Double ret = minW + delta / 2.0;
            if (delta / ret < 0.01) {
                return ret;
            }
        }
        return null;
    }

    private static Double getRestWav(SpectralWCS w) {
        if (w == null) {
            return null;
        }
        if (w.restwav != null) {
            return w.restwav;
        }
        if (w.restfrq != null) {
            return conv.toMeters(w.restfrq, EnergyConverter.BASE_UNIT_FREQ);
        }
        return null;
    }

    static DoubleInterval toInterval(SpectralWCS wcs, CoordRange1D r) {
        double a = r.getStart().val;
        double b = r.getEnd().val;

        String specsys = wcs.getSpecsys();
        if (!EnergyConverter.CORE_SPECSYS.equals(specsys)) {
            a = conv.convertSpecsys(a, specsys);
            b = conv.convertSpecsys(b, specsys);
        }

        String ctype = wcs.getAxis().getAxis().getCtype();
        String cunit = wcs.getAxis().getAxis().getCunit();
        if (!ctype.startsWith(EnergyConverter.CORE_CTYPE)
            || !EnergyConverter.CORE_UNIT.equals(cunit)) {
            log.debug("toInterval: converting " + a + cunit);
            a = conv.convert(a, EnergyConverter.CORE_CTYPE, cunit);
            log.debug("toInterval: converting " + b + cunit);
            b = conv.convert(b, EnergyConverter.CORE_CTYPE, cunit);
        }
        return new DoubleInterval(Math.min(a, b), Math.max(a, b));
    }

    static DoubleInterval toInterval(SpectralWCS wcs, CoordFunction1D f)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        // convert to TARGET_CTYPE
        WCSKeywords kw = new WCSWrapper(wcs, 1);
        Transform trans = new Transform(kw);
        String ctype = wcs.getAxis().getAxis().getCtype();
        if (!ctype.startsWith(EnergyConverter.CORE_CTYPE)) {
            log.debug("toInterval: transform from " + ctype + " to " + EnergyConverter.CORE_CTYPE + "-???");
            kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
            trans = new Transform(kw);
        }
        double naxis = kw.getDoubleValue("NAXIS1"); // axis set to 1 above
        double p1 = 0.5;
        double p2 = naxis + 0.5;
        Transform.Result start = trans.pix2sky(new double[] {p1});
        Transform.Result end = trans.pix2sky(new double[] {p2});

        double a = start.coordinates[0];
        double b = end.coordinates[0];
        log.debug("toInterval: wcslib returned " + a + start.units[0] + "," + b + end.units[0]);

        String specsys = wcs.getSpecsys();
        if (!EnergyConverter.CORE_SPECSYS.equals(specsys)) {
            a = conv.convertSpecsys(a, specsys);
            b = conv.convertSpecsys(b, specsys);
        }

        // wcslib convert to WAVE-??? but units might be a multiple of EnergyConverter.CORE_UNIT
        String cunit = start.units[0]; // assume same as end.units[0]
        if (!EnergyConverter.CORE_UNIT.equals(cunit)) {
            log.debug("toInterval: converting " + a + " " + cunit);
            a = conv.convert(a, EnergyConverter.CORE_CTYPE, cunit);
            log.debug("toInterval: converting " + b + " " + cunit);
            b = conv.convert(b, EnergyConverter.CORE_CTYPE, cunit);
        }

        return new DoubleInterval(Math.min(a, b), Math.max(a, b));
    }
}
