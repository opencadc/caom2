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
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SampledInterval;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.util.EnergyConverter;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.WCSKeywords;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import jsky.coords.wcscon;
import org.apache.log4j.Logger;

public final class CutoutUtil {
    private static final Logger log = Logger.getLogger(CutoutUtil.class);

    private static final String POS1_CUT = "px";
    private static final String POS2_CUT = "py";
    private static final String NRG_CUT = "ee";
    private static final String TIM_CUT = "tt";
    private static final String POL_CUT = "pp";
    private static final String OBS_CUT = "oo";
    private static final int CUT_LEN = 2;

    private CutoutUtil() {
    }

//    /**
//     * Compute a cfitsio-style cutout string in pixel coordinates for the specified
//     * artifact and bounds.
//     *
//     * @param a
//     * @param shape
//     * @param energyInter
//     * @param timeInter
//     * @param polarStates
//     * @return
//     * @throws NoSuchKeywordException
//     */
//    public static List<String> computeCutout(Artifact a, Shape shape, Interval energyInter, Interval timeInter, List<PolarizationState> polarStates)
//        throws NoSuchKeywordException {
//            return computeCutout(a, shape, energyInter, timeInter, polarStates, null);
//    }

    /**
     * Compute a cfitsio-style cutout string in pixel coordinates for the specified
     * artifact and bounds.
     *
     * @param a
     * @param shape
     * @param energyInter
     * @param timeInter
     * @param polarStates
     * @param customInter
     * @return
     * @throws NoSuchKeywordException
     */
    public static List<String> computeCutout(Artifact a, Shape shape, Interval energyInter, Interval timeInter, List<PolarizationState> polarStates, Interval customInter)
        throws NoSuchKeywordException {
        if (a == null) {
            throw new IllegalArgumentException("null Artifact");
        }

        // costly string conversions here
        if (log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("computeCutout: ").append(a.getURI());
            if (shape != null) {
                sb.append(" vs ").append(shape);
            }
            if (energyInter != null) {
                sb.append(" vs ").append(energyInter);
            }
            log.debug(sb.toString());
        }

        // for each chunk, we make a [part][chunk] cutout aka [extno][<pix range>,...]
        List<String> ret = new ArrayList<String>();
        // currently, only FITS files have parts and chunks
        for (Part p : a.getParts()) {
            boolean doCutObservable = false;
            boolean doCut = true;
            long[] posCut = null;
            long[] nrgCut = null;
            long[] timCut = null;
            long[] polCut = null;
            long[] obsCut = null;
            long[] customCut = null;

            for (Chunk c : p.getChunks()) {
                // check if spatial axes are part of the actual data array
                if (shape != null) {
                    if (canPositionCutout(c)) {
                        // cut.length==0 means circle contains all pixels
                        // cut.length==4 means circle picks a subset of pixels
                        long[] cut = getPositionBounds(c.position, shape);
                        if (posCut == null) {
                            posCut = cut;
                        } else if (posCut.length == 4 && cut != null) { // subset
                            if (cut.length == 0) {
                                posCut = cut;
                            } else { // both are length 4
                                posCut[0] = Math.min(posCut[0], cut[0]);
                                posCut[1] = Math.max(posCut[1], cut[1]);
                                posCut[2] = Math.min(posCut[2], cut[2]);
                                posCut[3] = Math.max(posCut[3], cut[3]);
                            }
                        }
                    }
                }
                // energy cutout
                if (energyInter != null) {
                    if (canEnergyCutout(c)) {
                        long[] cut = getEnergyBounds(c.energy, energyInter);
                        if (nrgCut == null) {
                            nrgCut = cut;
                            log.info("energy interval cut:" + cut);
                        } else if (nrgCut.length == 2 && cut != null) { // subset
                            if (cut.length == 0) {
                                nrgCut = cut;
                            } else { // both are length 4
                                nrgCut[0] = Math.min(nrgCut[0], cut[0]);
                                nrgCut[1] = Math.max(nrgCut[1], cut[1]);
                            }
                        }
                    }
                }

                // time cutout
                if (timeInter != null) {
                    if (canTimeCutout(c)) {
                        long[] cut = getTimeBounds(c.time, timeInter);
                        if (timCut == null) {
                            timCut = cut;
                        } else if (timCut.length == 2 && cut != null) { // subset
                            if (cut.length == 0) {
                                timCut = cut;
                            } else { // both are length 4
                                timCut[0] = Math.min(timCut[0], cut[0]);
                                timCut[1] = Math.max(timCut[1], cut[1]);
                            }
                        }
                    }
                }

                // polarization cutout
                if (polarStates != null && !polarStates.isEmpty()) {
                    if (canPolarizationCutout(c)) {
                        long[] cut = getPolarizationBounds(c.polarization, polarStates);
                        if (polCut == null) {
                            polCut = cut;
                        } else if (polCut.length == 2 && cut != null) { // subset
                            if (cut.length == 0) {
                                polCut = cut;
                            } else { // both are length 4
                                polCut[0] = Math.min(polCut[0], cut[0]);
                                polCut[1] = Math.max(polCut[1], cut[1]);
                            }
                        }
                    }
                }

                // custom cutout
                if (customInter != null) {
                    if (canCustomCutout(c)) {
                        long[] cut = getCustomAxisBounds(c.custom, customInter);
                        System.out.println(cut);
                        if (customCut == null) {
                            customCut = cut;
                        } else if (customCut.length == 2 && cut != null) { // subset
                            if (cut.length == 0) {
                                log.info("cut length is 0");
                                customCut = cut;
                            } else { // both are length 4
                                log.info("cut length is 4??");
                                customCut[0] = Math.min(customCut[0], cut[0]);
                                customCut[1] = Math.max(customCut[1], cut[1]);
                            }
                        }
                    }
                }

                // no input observable cutout, but merge
                if (canObservableCutout(c)) {
                    long[] cut = getObservableCutout(c.observable);
                    log.debug("checking chunk " + c.getID() + " obs cut: " + toString(cut));
                    if (obsCut == null) {
                        log.debug("observable cut: " + toString(obsCut) + " -> " + toString(cut));
                        obsCut = cut;
                    } else if (obsCut.length == 2 && cut != null) {
                        if (cut.length == 0) {
                            log.debug("observable cut: " + toString(obsCut) + " -> " + toString(cut));
                            obsCut = cut;
                        } else { // both are length 2
                            log.debug("observable cut merge before: " + toString(obsCut));
                            obsCut[0] = Math.min(obsCut[0], cut[0]);
                            obsCut[1] = Math.max(obsCut[1], cut[1]);
                            log.debug("observable cut merge after: " + toString(obsCut));
                        }
                    }
                }
            }

            // now inject the pixel ranges into the cutout spec
            StringBuilder sb = initCutout(p.getName(), p);
            if (posCut != null) {
                // cut.length==0 means circle contains all pixels
                // cut.length==4 means circle picks a subset of pixels
                if (posCut.length == 4) {
                    String cutX = posCut[0] + ":" + posCut[1];
                    String cutY = posCut[2] + ":" + posCut[3];
                    int i1 = sb.indexOf(POS1_CUT);
                    sb.replace(i1, i1 + CUT_LEN, cutX);
                    int i2 = sb.indexOf(POS2_CUT);
                    sb.replace(i2, i2 + CUT_LEN, cutY);
                    doCutObservable = true;
                } else {
                    int i1 = sb.indexOf(POS1_CUT);
                    sb.replace(i1, i1 + CUT_LEN, "*");
                    int i2 = sb.indexOf(POS2_CUT);
                    sb.replace(i2, i2 + CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("position cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            } else if (shape != null) {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no position overlap");
                doCut = false;
            }

            if (nrgCut != null) {
                // cut.length==0 means circle contains all pixels
                // cut.length==2 means interval picks a subset of pixels
                int i = sb.indexOf(NRG_CUT);
                if (nrgCut.length == 2) {
                    sb.replace(i, i + CUT_LEN, nrgCut[0] + ":" + nrgCut[1]);
                    doCutObservable = true;// cut.length==0 means circle contains all pixels
                } else {
                    sb.replace(i, i + CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("energy cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            } else if (energyInter != null) {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no energy overlap");
                doCut = false;
            }

            // time cutout: not supported

            if (polCut != null) {
                // cut.length==0 means cut contains all pixels
                // cut.length==2 means cut picks a subset of pixels
                int i = sb.indexOf(POL_CUT);
                if (polCut.length == 2) {
                    sb.replace(i, i + CUT_LEN, polCut[0] + ":" + polCut[1]);
                    doCutObservable = true;
                } else {
                    sb.replace(i, i + CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("polarization cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            } else if (polarStates != null && !polarStates.isEmpty()) {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no polarization overlap");
                doCut = false;
            }

            if (obsCut != null) {
                int i = sb.indexOf(OBS_CUT);
                sb.replace(i, i + CUT_LEN, obsCut[0] + ":" + obsCut[1]);
                String cs = sb.toString();
                log.debug("observable cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            } else {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no Observable axis");
            }

            // for any axis in the data but not in the cutout: keep all pixels
            int i;
            i = sb.indexOf(POS1_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            i = sb.indexOf(POS2_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            i = sb.indexOf(NRG_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            i = sb.indexOf(TIM_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            i = sb.indexOf(POL_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            i = sb.indexOf(OBS_CUT);
            if (i > 0) {
                sb.replace(i, i + CUT_LEN, "*");
            }

            if (doCut) {
                ret.add(sb.toString());
            }
        }
        return ret;
    }

    /**
     * Check if any child part has enough metadata to support cutout.
     * 
     * @param a
     * @return 
     */
    public static boolean canCutout(Artifact a) {
        for (Part p : a.getParts()) {
            if (canCutout(p)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check if any child chunk has enough metadata to support cutout.
     * 
     * @param p
     * @return 
     */
    public static boolean canCutout(Part p) {
        for (Chunk c : p.getChunks()) {
            if (canCutout(c)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check if the specified chunk has enough metadata to support cutout.
     * 
     * @param c
     * @return 
     */
    public static boolean canCutout(Chunk c) {

        boolean posCutout = canPositionCutout(c);
        boolean energyCutout = canEnergyCutout(c);
        boolean timeCutout = canTimeCutout(c);
        boolean polCutout = canPolarizationCutout(c);
        boolean customCutout = canCustomCutout(c);

        return posCutout || energyCutout || timeCutout || polCutout || customCutout;
    }
    
    private static String toString(long[] cut) {
        if (cut == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (long v : cut) {
            sb.append(v);
            sb.append(",");
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    static long[] getObservableCutout(ObservableAxis o) {

        long o1 = o.getDependent().getBin();
        long o2 = o.getDependent().getBin();

        if (o.independent != null) {
            if (o1 < o.independent.getBin()) {
                o2 = o.independent.getBin();
            } else {
                o1 = o.independent.getBin();
            }
        }
        return new long[] {o1, o2};
    }

    private static StringBuilder initCutout(String partName, Part p) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(partName).append("]");
        // create template cutout for each axis in the data array in the right order
        sb.append("[");
        boolean pos1 = false;
        boolean pos2 = false;
        boolean nrg = false;
        boolean tim = false;
        boolean pol = false;
        boolean obs = false;
        int naxis = 0;
        for (Chunk c : p.getChunks()) {
            naxis = Math.max(naxis, c.naxis);
            for (int i = 1; i <= c.naxis.intValue(); i++) {
                pos1 = pos1 || (c.positionAxis1 != null && i == c.positionAxis1);
                pos2 = pos2 || (c.positionAxis2 != null && i == c.positionAxis2);
                nrg = nrg || (c.energyAxis != null && i == c.energyAxis);
                tim = tim || (c.timeAxis != null && i == c.timeAxis);
                pol = pol || (c.polarizationAxis != null && i == c.polarizationAxis);
                obs = obs || (c.observableAxis != null && i == c.observableAxis);
            }
        }
        if (pos1) {
            sb.append(POS1_CUT).append(",");
        }
        if (pos2) {
            sb.append(POS2_CUT).append(",");
        }
        if (nrg) {
            sb.append(NRG_CUT).append(",");
        }
        if (tim) {
            sb.append(TIM_CUT).append(",");
        }
        if (pol) {
            sb.append(POL_CUT).append(",");
        }
        if (obs) {
            sb.append(OBS_CUT).append(",");
        }
        sb.setCharAt(sb.length() - 1, ']'); // last comma to ]
        log.debug("cutout template: " + sb.toString());
        return sb;
    }

    

    // check if spatial cutout is possible (currently function only)
    protected static boolean canPositionCutout(Chunk c) {
        boolean posCutout = (c.naxis != null && c.naxis.intValue() >= 2
            && c.position != null && c.position.getAxis().function != null
            && c.positionAxis1 != null && c.positionAxis1.intValue() <= c.naxis.intValue()
            && c.positionAxis2 != null && c.positionAxis2.intValue() <= c.naxis.intValue());
        return posCutout;
    }

    // check if energy cutout is possible (currently function only)
    protected static boolean canEnergyCutout(Chunk c) {
        boolean energyCutout = (c.naxis != null && c.naxis.intValue() >= 1
            && c.energy != null
            && c.energyAxis != null && c.energyAxis.intValue() <= c.naxis.intValue()
            && (
            c.energy.getAxis().bounds != null
                ||
                c.energy.getAxis().function != null
        )
        );
        return energyCutout;
    }

    // check if time cutout is possible (currently function only)
    protected static boolean canTimeCutout(Chunk c) {
        boolean timeCutout = false;
        //(c.naxis != null && c.naxis.intValue() >= 1
        //            && c.time != null && c.time.getAxis().function != null
        //            && c.timeAxis != null && c.timeAxis.intValue() <= c.naxis.intValue());
        return timeCutout;
    }

    // check if polarization cutout is possible (currently function only)
    protected static boolean canPolarizationCutout(Chunk c) {
        boolean polarizationCutout = (c.naxis != null && c.naxis.intValue() >= 1
                    && c.polarization != null && c.polarization.getAxis().function != null
                    && c.polarizationAxis != null && c.polarizationAxis.intValue() <= c.naxis.intValue());
        return polarizationCutout;
    }

    // check if custom cutout is possible (currently function only)
    protected static boolean canCustomCutout(Chunk c) {
        boolean customCutout = (c.naxis != null && c.naxis.intValue() >= 1
            && c.custom != null && c.custom.getAxis().function != null
            && c.customAxis != null && c.customAxis.intValue() <= c.naxis.intValue()
            && c.custom.getAxis().getAxis().getCtype() != null
        );
        return customCutout;
    }

    // check if polarization cutout is possible (currently function only)
    protected static boolean canObservableCutout(Chunk c) {
        boolean observableCutout = (c.naxis != null && c.naxis.intValue() >= 1
            && c.observable != null
            && c.observableAxis != null && c.observableAxis.intValue() <= c.naxis.intValue());
        return observableCutout;
    }


    static long[] getPositionBounds(SpatialWCS wcs, Shape s)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        if (s == null) {
            return null;
        }
        if (s instanceof Circle) {
            return getPositionBounds(wcs, (Circle) s);
        }
        if (s instanceof Polygon) {
            return getPositionBounds(wcs, ((Polygon) s).getSamples());
        }
        throw new IllegalArgumentException("unsupported cutout shape: " + s.getClass().getSimpleName());
    }

    /**
     * Find the pixel bounds that enclose the specified circle.
     *
     * @param wcs a spatial wcs solution
     * @param c   circle with center in ICRS coordinates
     * @return int[4] holding [x1, x2, y1, y2], int[0] if all pixels are included,
     *     or null if the circle does not intersect the WCS
     */
    static long[] getPositionBounds(SpatialWCS wcs, Circle c)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        // convert the Circle -> Box ~ Polygon
        // TODO: a WCS-aligned polygon would be better than an axis-aligned Box
        double x = c.getCenter().cval1;
        double y = c.getCenter().cval2;
        double dy = c.getRadius();
        double dx = Math.abs(dy / Math.cos(Math.toRadians(y)));
        MultiPolygon poly = new MultiPolygon();
        poly.getVertices().add(PositionUtil.rangeReduce(new Vertex(x - dx, y - dy, SegmentType.MOVE)));
        poly.getVertices().add(PositionUtil.rangeReduce(new Vertex(x + dx, y - dy, SegmentType.LINE)));
        poly.getVertices().add(PositionUtil.rangeReduce(new Vertex(x + dx, y + dy, SegmentType.LINE)));
        poly.getVertices().add(PositionUtil.rangeReduce(new Vertex(x - dx, y + dy, SegmentType.LINE)));
        poly.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
        return getPositionBounds(wcs, poly);
    }

    /**
     * Compute the range of pixel indices that correspond to the supplied
     * polygon. This method computes the cutout ranges within the image. The
     * returned value is null if there is no intersection, an int[4] for a
     * cutout, and an int[0] when the image is wholly contained within the
     * polygon (and no cutout is necessary).
     *
     * @param wcs  a spatial wcs solution
     * @param poly the shape to cutout
     * @return int[4] holding [x1, x2, y1, y2], int[0] if all pixels are included,
     *     or null if the circle does not intersect the WCS
     */
    static long[] getPositionBounds(SpatialWCS wcs, MultiPolygon poly)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        PositionUtil.CoordSys coordsys = PositionUtil.inferCoordSys(wcs);

        // detect necessary conversion of target coords to native WCS coordsys
        boolean gal = false;
        boolean fk4 = false;

        if (PositionUtil.CoordSys.GAL.equals(coordsys.getName())) {
            gal = true;
        } else if (PositionUtil.CoordSys.FK4.equals(coordsys.getName())) {
            fk4 = true;
        } else if (!PositionUtil.CoordSys.ICRS.equals(coordsys.getName()) && !PositionUtil.CoordSys.FK5.equals(coordsys.getName())) {
            throw new UnsupportedOperationException("unexpected coordsys: " + coordsys.getName());
        }

        WCSWrapper map = new WCSWrapper(wcs, 1, 2);
        Transform transform = new Transform(map);

        // convert wcs/footprint to sky coords
        log.debug("computing footprint from wcs");
        MultiPolygon foot = PositionUtil.toICRSPolygon(wcs);
        log.debug("wcs poly: " + foot);
        log.debug("input poly: " + poly);

        log.debug("computing poly INTERSECT footprint");
        MultiPolygon npoly = PolygonUtil.intersection(poly, foot);
        if (npoly == null) {
            log.debug("poly INTERSECT footprint == null");
            return null;
        }
        //log.debug("intersection: " + npoly);

        // npoly is in ICRS
        if (gal || fk4) {
            // convert npoly to native coordsys, in place since we created a new npoly above
            log.debug("converting poly to " + coordsys);
            for (Vertex v : npoly.getVertices()) {
                if (!SegmentType.CLOSE.equals(v.getType())) {
                    Point2D.Double pp = new Point2D.Double(v.cval1, v.cval2);

                    // convert poly coords to native WCS coordsys
                    if (gal) {
                        pp = wcscon.fk52gal(pp);
                    } else if (fk4) {
                        pp = wcscon.fk524(pp);
                    }
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
        for (Vertex v : npoly.getVertices()) {
            if (!SegmentType.CLOSE.equals(v.getType())) {
                coords[0] = v.cval1;
                coords[1] = v.cval2;

                if (coordsys.isSwappedAxes()) {
                    coords[0] = v.cval2;
                    coords[1] = v.cval1;
                }
                Transform.Result tr = transform.sky2pix(coords);
                // if p2 is null, it was a long way away from the WCS and does not
                // impose a limit/cutout - so we can safely skip it
                if (tr != null) {
                    //log.warn("pixel coords: " + tr.coordinates[0] + "," + tr.coordinates[1]);
                    x1 = (long) Math.floor(Math.min(x1, tr.coordinates[0] + 0.5));
                    x2 = (long) Math.ceil(Math.max(x2, tr.coordinates[0]) - 0.5);
                    y1 = (long) Math.floor(Math.min(y1, tr.coordinates[1] + 0.5));
                    y2 = (long) Math.ceil(Math.max(y2, tr.coordinates[1]) - 0.5);
                    //log.warn("clipped: " + x1 + ":" + x2 + " " + y1 + ":" + y2);
                }
                //else
                //    System.out.println("[GeomUtil] failed to convert " + v + ": skipping");
            }
        }

        // clipping
        long naxis1 = wcs.getAxis().function.getDimension().naxis1;
        long naxis2 = wcs.getAxis().function.getDimension().naxis2;
        return doPositionClipCheck(naxis1, naxis2, x1, x2, y1, y2);
    }

    private static long[] doPositionClipCheck(long w, long h, long x1, long x2, long y1, long y2) {
        if (x1 < 1) {
            x1 = 1;
        }
        if (x2 > w) {
            x2 = w;
        }
        if (y1 < 1) {
            y1 = 1;
        }
        if (y2 > h) {
            y2 = h;
        }

        // validity check
        // no pixels included
        if (x1 >= w || x2 <= 1 || y1 >= h || y2 <= 1) {
            return null;
        }
        // all pixels includes
        if (x1 == 1 && y1 == 1 && x2 == w && y2 == h) {
            return new long[0];
        }
        return new long[] {x1, x2, y1, y2}; // an actual cutout
    }

    /**
     * Compute a pixel cutout for the specified bounds. The bounds are assumed to be
     * barycentric wavelength in meters.
     *
     * @param wcs
     * @param bounds
     * @return int[2] with the pixel bounds, int[0] if all pixels are included, or
     *     null if no pixels are included
     */
    static long[] getEnergyBounds(SpectralWCS wcs, Interval bounds)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {
            // convert wcs to energy axis interval
            Interval si = EnergyUtil.toInterval(wcs, wcs.getAxis().function);

            // compute intersection
            Interval inter = Interval.intersection(si, bounds);
            //log.info("getBounds: intersection = " + inter);
            if (inter == null) {
                log.debug("bounds INTERSECT wcs.function == null");
                return null;
            }

            double a = inter.getLower();
            double b = inter.getUpper();

            WCSKeywords kw = new WCSWrapper(wcs, 1);
            Transform trans = new Transform(kw);

            String ctype = wcs.getAxis().getAxis().getCtype();
            if (!ctype.startsWith(EnergyConverter.CORE_CTYPE)) {
                kw = trans.translate(EnergyConverter.CORE_CTYPE + "-???"); // any linearization algorithm
                trans = new Transform(kw);
            }

            Transform.Result p1 = trans.sky2pix(new double[] {a});
            //log.debug("getBounds: sky2pix " + a + " -> " + p1.coordinates[0] + p1.units[0]);
            Transform.Result p2 = trans.sky2pix(new double[] {b});
            //log.debug("getBounds: sky2pix " + b + " -> " + p2.coordinates[0] + p2.units[0]);

            // values can be inverted if WCS is in freq or energy instead of wavelength
            long x1 = (long) Math.floor(Math.min(p1.coordinates[0], p2.coordinates[0] + 0.5));
            long x2 = (long) Math.ceil(Math.max(p1.coordinates[0], p2.coordinates[0]) - 0.5);

            return doClipCheck1D(wcs.getAxis().function.getNaxis().longValue(), x1, x2);
        }

        if (wcs.getAxis().bounds != null) {
            //log.info("getBounds: " + bounds);
            // find min and max sky coords
            double pix1 = Double.MAX_VALUE;
            double pix2 = Double.MIN_VALUE;
            long maxPixValue = 0;
            boolean foundOverlap = false;
            for (CoordRange1D tile : wcs.getAxis().bounds.getSamples()) {
                //log.warn("getBounds: tile = " + tile);
                maxPixValue = Math.max(maxPixValue, (long) tile.getEnd().pix);
                Interval bwmRange = EnergyUtil.toInterval(wcs, tile);
                // compute intersection
                Interval inter = Interval.intersection(bwmRange, bounds);
                //log.warn("getBounds: " + inter + " = " + wbounds + " X " + bounds);
                if (inter != null) {
                    pix1 = Math.min(pix1, tile.getStart().pix);
                    pix2 = Math.max(pix2, tile.getEnd().pix);
                    //log.warn("getBonds: pix range is now " + pix1 + "," + pix2);
                    foundOverlap = true;
                }
            }
            if (foundOverlap) {
                long p1 = (long) (pix1 + 0.5); // round up
                long p2 = (long) pix2;         // round down

                return doClipCheck1D(maxPixValue, p1, p2);
            }
            log.debug("bounds INTERSECT wcs.bounds == null");
            return null;
        }

        if (wcs.getAxis().range != null) {
            // can only check for complete non-overlap
            Interval bwmRange = EnergyUtil.toInterval(wcs, wcs.getAxis().range);
            // compute intersection
            Interval inter = Interval.intersection(bwmRange, bounds);
            //log.info("getBounds: intersection = " + inter);
            if (inter == null) {
                log.debug("bounds INTERSECT wcs.range == null");
                return null;
            }
            return new long[0]; // overlap
        }

        return null;
    }

    static long[] getPolarizationBounds(PolarizationWCS wcs, List<PolarizationState> states) {
        List<PolarizationState> dataPols = PolarizationUtil.wcsToStates(wcs);
        // find dataPols intersect states
        List<PolarizationState> keep = new ArrayList<PolarizationState>();
        for (PolarizationState ps : states) {
            if (dataPols.contains(ps)) {
                keep.add(ps);
                //log.warn("getPolarizationBounds keep: " + ps);
            }
        }
        if (keep.isEmpty()) {
            //log.warn("getPolarizationBounds: empty");
            return null; // no intersection
        }
        if (keep.size() == dataPols.size()) {
            //log.warn("getPolarizationBounds: all");
            return new long[0]; // keep all pixels
        }
        
        // keep some states AKA cutout
        double pix1 = Double.MAX_VALUE;
        double pix2 = Double.MIN_VALUE;
        for (PolarizationState ps : keep) {
            // function only: see canPolarizationCut above
            double wval = PolarizationState.intValue(ps);
            double px = Util.val2pix(wcs.getAxis().function, wval);
            pix1 = Math.min(pix1, px);
            pix2 = Math.max(pix2, px);
        }
        
        return doClipCheck1D(wcs.getAxis().function.getNaxis(), (long) pix1, (long) pix2);
    }

    /**
     * Compute custom axis bounds.
     *
     * @param wcs
     * @param bounds
     * @return int[2] with the pixel bounds, int[0] if all pixels are included, or
     *     null if no pixels are included
     */
    static long[] getCustomAxisBounds(CustomWCS wcs, Interval bounds)
        throws WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {

            CoordFunction1D func = wcs.getAxis().function;
            log.info("func" + func);

            if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
                throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
            }

            // convert wcs to custom axis interval
            Interval si = CustomAxisUtil.toInterval(wcs, wcs.getAxis().function);
            log.info("si: " + si);

            double d1 = CustomAxisUtil.val2pix(wcs, wcs.getAxis().function, si.getLower());
            double d2 = CustomAxisUtil.val2pix(wcs, wcs.getAxis().function, si.getUpper());
            log.info("d1, d2: " + d1 + " " + d2);

            long x1 = (long) Math.floor(Math.min(d1, d2 + 0.5));
            long x2 = (long) Math.ceil(Math.max(d1, d2) - 0.5);
            log.info("x1, x2: " + x1 + " " + x2);
            log.info("naxis long: " + wcs.getAxis().function.getNaxis().longValue());

            return doClipCheck1D(wcs.getAxis().function.getNaxis().longValue(), x1, x2);
        }

        return null;
    }

    /**
     * Compute time bounds.
     *
     * @param wcs
     * @param bounds
     * @return int[2] with the pixel bounds, int[0] if all pixels are included, or
     *     null if no pixels are included
     */
    static long[] getTimeBounds(TemporalWCS wcs, Interval bounds)
        throws WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {

            CoordFunction1D func = wcs.getAxis().function;

            if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
                throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
            }

            // convert wcs to custom axis interval
            Interval si = TimeUtil.toInterval(wcs, wcs.getAxis().function);

            double d1 = TimeUtil.val2pix(wcs, wcs.getAxis().function, si.getLower());
            double d2 = TimeUtil.val2pix(wcs, wcs.getAxis().function, si.getUpper());

            long x1 = (long) Math.floor(Math.min(d1, d2 + 0.5));
            long x2 = (long) Math.ceil(Math.max(d1, d2) - 0.5);

            return doClipCheck1D(wcs.getAxis().function.getNaxis().longValue(), x1, x2);
        }

        return null;
    }


    private static long[] doClipCheck1D(long len, long x1, long x2) {
        if (x1 < 1) {
            x1 = 1;
        }
        if (x2 > len) {
            x2 = len;
        }
        log.warn("doClipCheck1D: " + len + " " + x1 + ":" + x2);

        // validity check
        //if (len == 1 && x1 == 1 && x2 == 1) {
        //    log.warn("doClipCheck1D: single");
        //    return new long[0]; // the single pixel is included
        //}
        
        // all pixels includes
        if (x1 == 1 && x2 == len) {
            log.warn("doClipCheck1D: all");
            return new long[0];
        }
        
        // no pixels included
        if (x1 > len || x2 < 1) {
            log.warn("doClipCheck1D: none");
            return null;
        }
        
        // an actual cutout
        return new long[] {x1, x2};
    }
}
