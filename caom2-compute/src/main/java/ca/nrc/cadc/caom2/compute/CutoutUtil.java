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
import org.opencadc.caom2.Part;
import org.opencadc.caom2.PolarizationState;
import org.opencadc.caom2.util.EnergyConverter;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.CustomWCS;
import org.opencadc.caom2.wcs.ObservableAxis;
import org.opencadc.caom2.wcs.PolarizationWCS;
import org.opencadc.caom2.wcs.SpatialWCS;
import org.opencadc.caom2.wcs.SpectralWCS;
import org.opencadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.InvalidPolygonException;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
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
    private static final String CUST_CUT = "cc";
    private static final int CUT_LEN = 2;

    private CutoutUtil() {
    }

    /**
     * Compute a cfitsio-style cutout string in pixel coordinates for the specified
     * artifact and bounds.
     *
     * @param a
     * @param shape
     * @param energyInter
     * @param timeInter
     * @param polarStates
     * @param customCtype
     * @param customInter
     * @return
     * @throws NoSuchKeywordException
     */
    public static List<String> computeCutout(Artifact a, Shape shape,
            DoubleInterval energyInter, DoubleInterval timeInter, List<PolarizationState> polarStates,
            String customCtype, DoubleInterval customInter)
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
                            log.debug("energy interval cut:" + cut);
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
                    if (customCtype == null) {
                        throw new IllegalArgumentException("ctype not declared for custom interval.");
                    }
                    if (canCustomCutout(c, customCtype)) {
                        long[] cut = getCustomAxisBounds(c.custom, customInter);
                        if (customCut == null) {
                            customCut = cut;
                        } else if (customCut.length == 2 && cut != null) { // subset
                            if (cut.length == 0) {
                                log.debug("cut length is 0");
                                customCut = cut;
                            } else { // both are length 4
                                log.debug("cut length is 4??");
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

            if (timCut != null) {
                // cut.length==0 means circle contains all pixels
                // cut.length==2 means interval picks a subset of pixels
                int i = sb.indexOf(TIM_CUT);
                if (timCut.length == 2) {
                    sb.replace(i, i + CUT_LEN, timCut[0] + ":" + timCut[1]);
                } else {
                    sb.replace(i, i + CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("time cutout: " + a.getURI() + "," + p.getName() + ", Chunk: " + cs);
            } else if (timeInter != null) {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ", Chunk: no time overlap");
                doCut = false;
            }

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

            if (customCut != null) {
                // cut.length==0 means circle contains all pixels
                // cut.length==2 means interval picks a subset of pixels
                int i = sb.indexOf(CUST_CUT);
                log.debug("cust cut index, length: " + i + ", " + customCut.length);
                if (customCut.length == 2) {
                    sb.replace(i, i + CUT_LEN, customCut[0] + ":" + customCut[1]);
                } else {
                    sb.replace(i, i + CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("custom cutout: " + a.getURI() + "," + p.getName() + ", Chunk: " + cs);
                doCut = true;
            } else if (customInter != null) {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ", Chunk: no custom overlap");
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

            i = sb.indexOf(CUST_CUT);
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
        boolean customCutout = c.custom != null && canCustomCutout(c, c.custom.getAxis().getAxis().getCtype());

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

    // protected for unit test
    static StringBuilder initCutout(String partName, Part p) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(partName).append("]");
        if (!p.getChunks().isEmpty()) {
            sb.append("[");

            // create template cutout for each axis in the data array in the right order
            // if there are multiple chunks, they should only differ in observable 
        
            Chunk c = p.getChunks().iterator().next();
            //log.debug("initCutout: " + c.naxis + "," + c.positionAxis1 + "," + c.positionAxis2
            //        + "," + c.energyAxis + "," + c.timeAxis + "," + c.polarizationAxis 
            //        + "," + c.customAxis + "," + c.observableAxis);
            
            //for (Chunk c : p.getChunks()) {
            int n = 0;
            if (c.naxis != null) {
                n = c.naxis;
            }
            for (int i = 1; i <= n; i++) {
                if (c.positionAxis1 != null && i == c.positionAxis1) {
                    sb.append(POS1_CUT).append(",");
                } else if (c.positionAxis2 != null && i == c.positionAxis2) {
                    sb.append(POS2_CUT).append(",");
                } else if (c.energyAxis != null && i == c.energyAxis) {
                    sb.append(NRG_CUT).append(",");
                } else if (c.timeAxis != null && i == c.timeAxis) {
                    sb.append(TIM_CUT).append(",");
                } else if (c.polarizationAxis != null && i == c.polarizationAxis) {
                    sb.append(POL_CUT).append(",");
                } else if (c.customAxis != null && i == c.customAxis) {
                    sb.append(CUST_CUT).append(",");
                } else if (c.observableAxis != null && i == c.observableAxis) {
                    sb.append(OBS_CUT).append(",");
                }
            }
            //}
            if (sb.indexOf(",") > 0) {
                sb.setCharAt(sb.length() - 1, ']'); // last comma to ]
            } else {
                sb.append("]");
            }
        }

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
        return (c.naxis != null && c.naxis.intValue() >= 1
                    && c.time != null && c.time.getAxis().function != null
                    && c.timeAxis != null && c.timeAxis.intValue() <= c.naxis.intValue());
    }

    // check if polarization cutout is possible (currently function only)
    protected static boolean canPolarizationCutout(Chunk c) {
        boolean polarizationCutout = (c.naxis != null && c.naxis.intValue() >= 1
                    && c.polarization != null && c.polarization.getAxis().function != null
                    && c.polarizationAxis != null && c.polarizationAxis.intValue() <= c.naxis.intValue());
        return polarizationCutout;
    }

    // check if custom cutout is possible (currently function only)
    protected static boolean canCustomCutout(Chunk c, String ctype) {
        boolean customCutout = (c.naxis != null && c.naxis.intValue() >= 1
            && c.custom != null && c.custom.getAxis().function != null
            && c.customAxis != null && c.customAxis.intValue() <= c.naxis.intValue()
            && c.custom.getAxis().getAxis().getCtype() != null
            && c.custom.getAxis().getAxis().getCtype().equals(ctype)
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
        try {
            if (s instanceof Circle) {
                return getPositionBounds(wcs, (Circle) s);
            }
            if (s instanceof Polygon) {
                return getPositionBounds(wcs, (Polygon) s);
            }
        } catch(InvalidPolygonException ex) {
            throw new RuntimeException("BUG or CONTENT: generated invalid polygon whihc computing cutout", ex);
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
        throws NoSuchKeywordException, WCSLibRuntimeException, InvalidPolygonException {
        Polygon poly = Circle.generatePolygonApproximation(c, 11);
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
    static long[] getPositionBounds(SpatialWCS wcs, Polygon poly)
        throws InvalidPolygonException, NoSuchKeywordException, WCSLibRuntimeException {
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
        Shape foot = PositionUtil.toShapeICRS(wcs);
        log.warn("wcs shape: " + foot);
        
        Polygon fpoly = null;
        if (foot instanceof Circle) {
            fpoly = Circle.generatePolygonApproximation((Circle) foot, 11);
        } else {
            fpoly = (Polygon) foot;
        }
        
        log.warn("input poly: " + poly);
        log.warn("wcs poly: " + fpoly);

        log.debug("computing poly INTERSECT footprint");
        Polygon npoly = PolygonUtil.intersection(poly, fpoly);
        log.warn("input INTERSECT wcs: " + npoly);
        if (npoly == null) {
            log.debug("poly INTERSECT footprint == null");
            return null;
        }
        //log.debug("intersection: " + npoly);

        // npoly is in ICRS
        if (gal || fk4) {
            // convert npoly to native coordsys, in place since we created a new npoly above
            log.warn("converting intersection to " + coordsys.getName());
            //Point v : npoly.getVertices()
            for (int i = 0; i < npoly.getVertices().size(); i++) {
                Point v = npoly.getVertices().get(i);
                Point2D.Double pp = new Point2D.Double(v.getLongitude(), v.getLatitude());

                // convert poly coords to native WCS coordsys
                if (gal) {
                    pp = wcscon.fk52gal(pp);
                } else if (fk4) {
                    pp = wcscon.fk524(pp);
                }
                Point r = new Point(pp.x, pp.y);
                npoly.getVertices().set(i, r); // in-place
            }
        }
        log.warn("sky cutout: " + npoly);

        // convert npoly to pixel coordinates and find min/max
        double x1 = Double.MAX_VALUE;
        double x2 = -1 * x1;
        double y1 = x1;
        double y2 = -1 * y1;
        log.debug("converting npoly to pixel coordinates");
        double[] coords = new double[2];
        for (Point v : npoly.getVertices()) {
            coords[0] = v.getLongitude();
            coords[1] = v.getLatitude();

            if (coordsys.isSwappedAxes()) {
                coords[0] = v.getLatitude();
                coords[1] = v.getLongitude();
            }
            Transform.Result tr = transform.sky2pix(coords);
            // if p2 is null, it was a long way away from the WCS and does not
            // impose a limit/cutout - so we can safely skip it
            if (tr != null) {
                //log.warn("pixel coords: " + tr.coordinates[0] + "," + tr.coordinates[1]);
                x1 = Math.min(x1, tr.coordinates[0]);
                x2 = Math.max(x2, tr.coordinates[0]);
                y1 = Math.min(y1, tr.coordinates[1]);
                y2 = Math.max(y2, tr.coordinates[1]);
            }
            //else
            //    System.out.println("[GeomUtil] failed to convert " + v + ": skipping");
        }
        //log.warn(x1 + " " + x2 + " " + y1 + " " +y2);
        long ix1 = (long) Math.floor(x1 + 0.5);
        long ix2 = (long) Math.ceil(x2 - 0.5);
        long iy1 = (long) Math.floor(y1 + 0.5);
        long iy2 = (long) Math.ceil(y2 - 0.5);

        // clipping
        long naxis1 = wcs.getAxis().function.getDimension().naxis1;
        long naxis2 = wcs.getAxis().function.getDimension().naxis2;
        log.warn("doClip: " + naxis1 + "," + naxis2 + "," + ix1 + "," + ix2 + "," + iy1 + "," + iy2);
        return doPositionClipCheck(naxis1, naxis2, ix1, ix2, iy1, iy2);
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
    static long[] getEnergyBounds(SpectralWCS wcs, DoubleInterval bounds)
        throws NoSuchKeywordException, WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {
            // convert wcs to energy axis interval
            DoubleInterval si = EnergyUtil.toInterval(wcs, wcs.getAxis().function);

            // compute intersection
            DoubleInterval inter = DoubleInterval.intersection(si, bounds);
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
            long x1 = (long) Math.floor(Math.min(p1.coordinates[0], p2.coordinates[0]) + 0.5);
            long x2 = (long) Math.ceil(Math.max(p1.coordinates[0], p2.coordinates[0]) - 0.5);

            return doClipCheck1D(wcs.getAxis().function.getNaxis().longValue(), x1, x2);
        }

        if (wcs.getAxis().bounds != null) {
            // find min and max sky coords
            double pix1 = Double.MAX_VALUE;
            double pix2 = Double.MIN_VALUE;
            long maxPixValue = 0;
            boolean foundOverlap = false;
            for (CoordRange1D tile : wcs.getAxis().bounds.getSamples()) {
                //log.warn("getBounds: tile = " + tile);
                maxPixValue = Math.max(maxPixValue, (long) tile.getEnd().pix);
                DoubleInterval bwmRange = EnergyUtil.toInterval(wcs, tile);
                // compute intersection
                DoubleInterval inter = DoubleInterval.intersection(bwmRange, bounds);
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
            DoubleInterval bwmRange = EnergyUtil.toInterval(wcs, wcs.getAxis().range);
            // compute intersection
            DoubleInterval inter = DoubleInterval.intersection(bwmRange, bounds);
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
    static long[] getCustomAxisBounds(CustomWCS wcs, DoubleInterval bounds)
        throws WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {

            CoordFunction1D func = wcs.getAxis().function;

            if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
                throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
            }

            // convert wcs to custom axis interval
            DoubleInterval si = CustomAxisUtil.toInterval(wcs, wcs.getAxis().function);

            // compute intersection
            DoubleInterval inter = DoubleInterval.intersection(si, bounds);

            if (inter == null) {
                log.debug("bounds INTERSECT wcs.function == null");
                return null;
            }

            log.debug("interval upper/lower: " + inter.getLower() + ", " + inter.getUpper());

            double d1 = CustomAxisUtil.val2pix(wcs, wcs.getAxis().function, inter.getLower());
            double d2 = CustomAxisUtil.val2pix(wcs, wcs.getAxis().function, inter.getUpper());
            log.debug("d1, d2: " + d1 + " " + d2);

            long x1 = (long) Math.floor(Math.min(d1, d2) + 0.5);
            long x2 = (long) Math.ceil(Math.max(d1, d2) - 0.5);
            log.debug("x1, x2: " + x1 + " " + x2);

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
    static long[] getTimeBounds(TemporalWCS wcs, DoubleInterval bounds)
        throws WCSLibRuntimeException {
        if (wcs.getAxis().function != null) {

            CoordFunction1D func = wcs.getAxis().function;

            if (func.getDelta() == 0.0 && func.getNaxis() > 1L) {
                throw new IllegalArgumentException("invalid CoordFunction1D: found " + func.getNaxis() + " pixels and delta = 0.0");
            }

            // convert wcs to time axis interval
            DoubleInterval si = TimeUtil.toInterval(wcs, wcs.getAxis().function);

            // compute intersection
            DoubleInterval inter = DoubleInterval.intersection(si, bounds);
            if (inter == null) {
                log.debug("bounds INTERSECT wcs.function == null");
                return null;
            }

            double d1 = TimeUtil.val2pix(wcs, wcs.getAxis().function, inter.getLower());
            double d2 = TimeUtil.val2pix(wcs, wcs.getAxis().function, inter.getUpper());

            long x1 = (long) Math.floor(Math.min(d1, d2) + 0.5);
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
