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
import ca.nrc.cadc.caom2.CustomAxis;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.dali.DoubleInterval;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.log4j.Logger;

/**
 * Utility class for Custom calculations.
 *
 * @author hjeeves
 */
public final class CustomAxisUtil {

    private static final Logger log = Logger.getLogger(CustomAxisUtil.class);

    private static final TreeMap<String, String> ctypeCunitMap = new TreeMap<String,String>() {{
                put("FARADAY", "rad/m**2");
                put("RM", "rad/m**2");
        }
    };

    private CustomAxisUtil() {
    }

    private static String getCtype(Set<Artifact> artifacts, DataLinkSemantics productType) {
        String firstCtype = null;

        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (c.custom != null
                        && Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {

                        String currentCtype = c.custom.getAxis().getAxis().getCtype();
                        if (firstCtype == null) {
                            if (ctypeCunitMap.get(currentCtype) == null) {
                                throw new IllegalArgumentException("Unsupported CTYPE: " + currentCtype);
                            }
                            firstCtype = currentCtype;
                        }
                        if (currentCtype.compareTo(firstCtype) != 0) {
                            throw new IllegalArgumentException("CTYPE must be the same across all Artifacts. Found: "
                               + currentCtype + " and " + firstCtype);
                        }
                    }
                }
            }
        }

        return firstCtype;
    }

    static class ComputedBounds {
        DoubleInterval bounds;
        List<DoubleInterval> samples;
    }

    public static CustomAxis compute(Set<Artifact> artifacts) {
        DataLinkSemantics productType = DataLinkSemantics.THIS;
        String axisCtype = getCtype(artifacts, productType);

        if (axisCtype != null && productType != null) {
            ComputedBounds cb = computeBounds(artifacts, productType, axisCtype);
            if (cb != null) {
                CustomAxis c = new CustomAxis(axisCtype, cb.bounds);
                c.getSamples().addAll(cb.samples);
                c.dimension = computeDimensionFromRangeBounds(artifacts, productType, axisCtype);
                if (c.dimension == null) {
                    c.dimension = computeDimensionFromWCS(c.getBounds(), artifacts, productType, axisCtype);
                }
                return c;
            }
        }
        return null;
    }

    /**
     * Computes the union.
     *
     * @param artifacts
     * @param productType
     * @return
     */
    static ComputedBounds computeBounds(Set<Artifact> artifacts, DataLinkSemantics productType, String expectedCtype) {
        double unionScale = 0.02;
        List<DoubleInterval> subs = new ArrayList<>();

        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        String currentCtype = c.custom.getAxis().getAxis().getCtype();
                        if (currentCtype == null || currentCtype.compareTo(expectedCtype) != 0) {
                            throw new IllegalArgumentException("CTYPE must be the same across all Artifacts. Found: "
                                + currentCtype + ". Expected: " + expectedCtype);
                        } else {
                            CoordRange1D range = c.custom.getAxis().range;
                            CoordBounds1D bounds = c.custom.getAxis().bounds;
                            CoordFunction1D function = c.custom.getAxis().function;
                            if (range != null) {
                                DoubleInterval s = toInterval(c.custom, range);
                                log.debug("[computeBounds] range -> sub: " + s);
                                Util.mergeIntoList(s, subs, unionScale);
                            } else if (bounds != null) {
                                for (CoordRange1D cr : bounds.getSamples()) {
                                    DoubleInterval s = toInterval(c.custom, cr);
                                    log.debug("[computeBounds] bounds -> sub: " + s);
                                    Util.mergeIntoList(s, subs, unionScale);
                                }
                            } else if (function != null) {
                                DoubleInterval s = CustomAxisUtil.toInterval(c.custom, function);
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
     * Compute dimensionality (number of pixels) from WCS function. This method
     * assumes that the energy axis is roughly continuous (e.g. it ignores gaps).
     *
     * @param bounds
     * @param artifacts
     * @param productType
     * @return
     */
    static Long computeDimensionFromWCS(DoubleInterval bounds, Set<Artifact> artifacts, DataLinkSemantics productType, String expectedCtype) {
        log.debug("computeDimensionFromWCS: " + bounds);
        if (bounds == null) {
            return null;
        }

        // pick the WCS with the largest pixel size
        CustomWCS sw = null;
        double scale = 0.0;
        int num = 0;
        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        String currentCtype = c.custom.getAxis().getAxis().getCtype();
                        if (currentCtype == null || currentCtype.compareTo(expectedCtype) != 0) {
                            throw new IllegalArgumentException("CTYPE must be the same across all Artifacts. Found: "
                                + currentCtype + ". Expected: " + expectedCtype);
                        } else {
                            num++;
                            double ss = Math.abs(c.custom.getAxis().function.getDelta());
                            if (ss >= scale) {
                                scale = ss;
                                sw = c.custom;
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
     * * Compute dimensionality (number of pixels).
     *
     * @param artifacts
     * @param productType
     * @return
     */
    static Long computeDimensionFromRangeBounds(Set<Artifact> artifacts, DataLinkSemantics productType, String expectedCtype) {
        // assumption: all   pixels are distinct so just add up the number of pixels
        double numPixels = 0;

        for (Artifact a : artifacts) {
            for (Part p : a.getParts()) {
                for (Chunk c : p.getChunks()) {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType)) {
                        if (c.custom != null) {
                            String currentCtype = c.custom.getAxis().getAxis().getCtype();
                            if (currentCtype == null || currentCtype.compareTo(expectedCtype) != 0) {
                                throw new IllegalArgumentException("CTYPE must be the same across all Artifacts. Found: "
                                    + currentCtype + ". Expected: " + expectedCtype);
                            } else {
                                double n = Util.getNumPixels(c.custom.getAxis(), false);
                                numPixels += n;
                            }
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
     * Calculate interval for CoordRange1D.
     *
     * @param wcs
     * @param r
     * @return
     */
    static DoubleInterval toInterval(CustomWCS wcs, CoordRange1D r) {
        validateWCS(wcs);

        double np = Math.abs(r.getStart().pix - r.getEnd().pix);
        double a = r.getStart().val;
        double b = r.getEnd().val;
        double delta = Math.abs(b - a);
        if (delta == 0.0 && np > 1.0) {
            throw new IllegalArgumentException("invalid CoordRange1D: found " + np + " + pixels and delta = 0.0 in [" + a + "," + b + "]");
        }

        return new DoubleInterval(Math.min(a, b), Math.max(a, b));
    }

    /**
     * Calculate interval for CoordFunction1D.
     *
     * @param wcs
     * @param func
     * @return
     */
    static DoubleInterval toInterval(CustomWCS wcs, CoordFunction1D func) {
        validateWCS(wcs);

        if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
            throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
        }
        
        double p1 = 0.5;
        double p2 = func.getNaxis().doubleValue() + 0.5;
        double a = Util.pix2val(func, p1);
        double b = Util.pix2val(func, p2);

        return new DoubleInterval(Math.min(a, b), Math.max(a, b));
    }

    /**
     *
     * @param wcs
     * Throws IllegalArgumentException if CTYPE or CUNIT invalid
     */
    private static void validateWCS(CustomWCS wcs) {
        String ctype = wcs.getAxis().getAxis().getCtype();
        String mapCunit = ctypeCunitMap.get(ctype); // this is OK until someone justifies support for a dimensionless custom axis
        if (mapCunit == null) {
            throw new IllegalArgumentException("Invalid CTYPE: " + ctype);
        }

        String rawCunit = wcs.getAxis().getAxis().cunit;
        String cunit = normalizeUnit(rawCunit);
        if (!mapCunit.equals(cunit)) {
            throw new IllegalArgumentException("Invalid CUNIT for CTYPE: " + ctype + ". Expected: "
                + mapCunit + ". Found: " + cunit + " (normalized, raw: " + rawCunit + ")");
        }
    }

    private static String normalizeUnit(String rawUnit) {
        String normalizedUnit = rawUnit;
        if (rawUnit != null && rawUnit.contains("^")) {
            normalizedUnit = rawUnit.replaceAll("\\^", "**");
            log.debug("normalized unit: " + rawUnit + " to: " + normalizedUnit);
        }
        return normalizedUnit;
    }


    public static double val2pix(CustomWCS wcs, CoordFunction1D func, double val) {
        validateWCS(wcs);
        return Util.val2pix(func, val);
    }

    public static String getUnits(String customCtype) {
        return ctypeCunitMap.get(customCtype);
    }
}
