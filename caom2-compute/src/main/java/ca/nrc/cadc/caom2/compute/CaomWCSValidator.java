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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 * Created by jeevesh
 */
public class CaomWCSValidator {
    private static final Logger log = Logger.getLogger(CaomWCSValidator.class);

    private static final String AXES_VALIDATION_ERROR = "Invalid Axes";
    private static final String CUSTOM_WCS_VALIDATION_ERROR = "Invalid CustomWCS: ";
    private static final String METADATA_NOT_FOUND = "%s: %s found (%d) but metadata not found";
    private static final String POLARIZATION_WCS_VALIDATION_ERROR = "Invalid PolarizationWCS: ";
    private static final String SPATIAL_WCS_VALIDATION_ERROR = "Invalid SpatialWCS: ";
    private static final String SPECTRAL_WCS_VALIDATION_ERROR = "Invalid SpectralWCS: ";
    private static final String TEMPORAL_WCS_VALIDATION_ERROR = "Invalid TemporalWCS: ";

    // temporary hack to limit validation in order to grandfather in some old metadata
    private static boolean filterByProductType = false;
    
    static {
        filterByProductType = "true".equals(System.getProperty(CaomWCSValidator.class.getName() + ".filterByProductType"));
    }
    
    private static boolean typeFilter(ProductType t) {
        if (!filterByProductType) {
            return true;
        }
        return t == null || ProductType.SCIENCE.equals(t) || ProductType.CALIBRATION.equals(t);
    }
    
    /**
     * Validate all WCS types included in the Chunks belonging to the given Artifact.
     *
     * @param a
     * @throws IllegalArgumentException
     */
    public static void validate(Artifact a) 
        throws IllegalArgumentException {
        if (a != null && typeFilter(a.getProductType())) {
            for (Part p : a.getParts()) {
                if (p != null && typeFilter(p.productType)) {
                    for (Chunk c : p.getChunks()) {
                        if (typeFilter(c.productType)) {
                            String context = a.getURI().toASCIIString() 
                                    + "[" + p.getName() + "]:" + c.getID().toString() + " ";
                            validateChunk(context, c);
                        }
                    }
                }
            }
        }
    }


    /**
     * Validate all WCS values in the given Chunk.
     *
     * @param c
     * @throws IllegalArgumentException
     */
    public static void validateChunk(String context, Chunk c)
        throws IllegalArgumentException {
        validateAxes(c);
        validateSpatialWCS(context, c.position);
        validateSpectralWCS(context, c.energy);
        validateTemporalWCS(context, c.time);
        validatePolarizationWCS(context, c.polarization);
        validateCustomWCS(context, c.custom);
    }

    public static void validateSpatialWCS(String context, SpatialWCS position) {
        if (position != null) {
            try {
                // Convert to polygon using native coordinate system
                PositionUtil.CoordSys csys = PositionUtil.inferCoordSys(position);
                Point center = null;
                MultiPolygon mp = PositionUtil.toPolygon(position, csys.swappedAxes);
                if (mp != null) {
                    center = mp.getCenter();
                }
                
                if (center != null && position.getAxis().function != null) {
                    log.debug("center: " + center);
                    double[] coords = new double[2];
                    coords[0] = center.cval1;
                    coords[1] = center.cval2;
                    
                    WCSWrapper map = new WCSWrapper(position, 1, 2);
                    Transform transform = new Transform(map);
                    Transform.Result tr = transform.sky2pix(coords);
                    log.debug("center pixels: " + tr.coordinates[0] + "," + tr.coordinates[1]);
                }
            } catch (NoSuchKeywordException ex) {
                throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            } catch (WCSLibRuntimeException ex) {
                throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context , ex);
            } catch (UnsupportedOperationException ex) {
                // error thrown from toPolygon if WCS is too near a pole, or if the bounds
                // value is not recognized
                throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            }
        }
    }

    public static void validateSpectralWCS(String context, SpectralWCS energy) {
        if (energy != null) {
            try {
                CoordAxis1D energyAxis = energy.getAxis();
                Interval si = null;

                if (energyAxis.range != null) {
                    si = EnergyUtil.toInterval(energy, energyAxis.range);
                }

                if (energyAxis.bounds != null) {
                    for (CoordRange1D tile : energyAxis.bounds.getSamples()) {
                        si = EnergyUtil.toInterval(energy, tile);
                    }
                }

                if (energyAxis.function != null) {
                    si = EnergyUtil.toInterval(energy, energyAxis.function);

                    WCSWrapper map = new WCSWrapper(energy, 1);
                    Transform transform = new Transform(map);

                    double[] coord = new double[1];
                    Transform.Result tr = null;
                    coord[0] = (si.getUpper() - si.getLower()) / 2;
                    tr = transform.sky2pix(coord);
                }
            } catch (NoSuchKeywordException ex) {
                throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            } catch (WCSLibRuntimeException ex) {
                throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            }
        }

    }

    public static void validateTemporalWCS(String context, TemporalWCS time) {
        if (time != null) {
            try {
                CoordAxis1D timeAxis = time.getAxis();
                if (timeAxis.range != null) {
                    Interval s = TimeUtil.toInterval(time, timeAxis.range);
                }
                if (timeAxis.bounds != null) {
                    for (CoordRange1D cr : timeAxis.bounds.getSamples()) {
                        Interval s1 = TimeUtil.toInterval(time, cr);

                    }
                }
                if (timeAxis.function != null) {
                    Interval s2 = TimeUtil.toInterval(time, timeAxis.function);

                    // Currently there is no WCSWrapper for time, so sky2pix
                    // transformation can't be done
                    //                        WCSWrapper map = new WCSWrapper(time, 1);
                    //                        Transform transform = new Transform(map);
                    //                        double[] coord = new double[1];
                    //                        coord[0] = (s1.getUpper() - s1.getLower())/2;
                    //                        Transform.Result tr = transform.sky2pix(coord);
                }
            } catch (UnsupportedOperationException ex) {
                // timesys or CUNIT error
                throw new IllegalArgumentException(TEMPORAL_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            }
        }
    }

    public static void validatePolarizationWCS(String context, PolarizationWCS polarization) {
        if (polarization != null) {
            try {
                CoordAxis1D polarizationAxis = polarization.getAxis();

                if (polarizationAxis.range != null) {
                    int lb = (int) polarizationAxis.range.getStart().val;
                    int ub = (int) polarizationAxis.range.getEnd().val;
                    for (int i = lb; i <= ub; i++) {
                        PolarizationState.toValue(i);
                    }
                } else if (polarizationAxis.bounds != null) {
                    for (CoordRange1D cr : polarizationAxis.bounds.getSamples()) {
                        int lb = (int) cr.getStart().val;
                        int ub = (int) cr.getEnd().val;
                        for (int i = lb; i <= ub; i++) {
                            PolarizationState.toValue(i);
                        }
                    }
                } else if (polarizationAxis.function != null) {
                    for (int i = 1; i <= polarizationAxis.function.getNaxis(); i++) {
                        double pix = (double) i;
                        int val = (int) Util.pix2val(polarizationAxis.function, pix);
                        PolarizationState.toValue(val);
                    }
                }
            } catch (UnsupportedOperationException ex) {
                throw new IllegalArgumentException(POLARIZATION_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            }
        }
    }

    private static void checkDuplicateAxis(HashMap axisMap, Integer axis, String varName) {
        log.debug("checking duplicate: " + axis);
        if (axisMap.get(axis) != null) {
            throw new IllegalArgumentException("Duplicate axis found: (" + axis + ") " + axisMap.get(axis) + " & " + varName);
        }
    }

    public static void validateAxes(Chunk chunk)  {
        if (chunk.naxis != null) {
            // Have axisList offset by 1 because the list will be counted
            // from 1 to naxis. Nulls in the list are missing axis definitions.
                HashMap<Integer,String> axisMap = new HashMap<>();

            // Go through each axis and validate

            // If positionAxis1 is defined, positionsAxis2 must be defined and position must
            // also be defined.
            if (chunk.positionAxis1 != null || chunk.positionAxis2 != null) {
                if (chunk.positionAxis2 == null || chunk.positionAxis1 == null) {
                    throw new IllegalArgumentException(AXES_VALIDATION_ERROR
                        + ": positionAxis1 or positionAxis2 is null.");
                }
                checkDuplicateAxis(axisMap, chunk.positionAxis1, "positionAxis1");
                axisMap.put(chunk.positionAxis1,"positionAxis1");
                checkDuplicateAxis(axisMap, chunk.positionAxis2, "positionAxis2");
                axisMap.put(chunk.positionAxis2, "positionAxis2");
                if (chunk.position == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, "positionAxis1", chunk.positionAxis1));
                }
            }

            String axisName = "timeAxis";
            if (chunk.timeAxis != null) {
                // Throws an illegal argument exception if it's duplicate
                checkDuplicateAxis(axisMap, chunk.timeAxis, axisName);
                axisMap.put(chunk.timeAxis, axisName);
                if (chunk.time == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, axisName, chunk.timeAxis)
                    );
                }
            }

            axisName = "energyAxis";
            if (chunk.energyAxis != null) {
                // Throws an illegal argument exception if it's duplicate
                checkDuplicateAxis(axisMap, chunk.energyAxis, axisName);
                axisMap.put(chunk.energyAxis, axisName);
                if (chunk.energy == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, axisName, chunk.energyAxis));
                }
            }

            axisName = "customAxis";
            if (chunk.customAxis != null) {
                // Throws an illegal argument exception if it's duplicate
                checkDuplicateAxis(axisMap, chunk.customAxis, axisName);
                axisMap.put(chunk.customAxis, axisName);
                if (chunk.custom == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, axisName, chunk.customAxis));
                }
            }

            axisName = "polarizationAxis";
            if (chunk.polarizationAxis != null) {
                // Throws an illegal argument exception if it's duplicate
                checkDuplicateAxis(axisMap, chunk.polarizationAxis, axisName);
                axisMap.put(chunk.polarizationAxis, axisName);
                if (chunk.polarization == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, axisName, chunk.polarizationAxis));
                }
            }

            axisName = "observableAxis";
            log.debug("OBSERVABLEAXIS: " + chunk.observableAxis + "- metadata: " + chunk.observable);
            if (chunk.observableAxis != null && chunk.observableAxis <= chunk.naxis) {
                // Throws an illegal argument exception if it's duplicate
                checkDuplicateAxis(axisMap, chunk.observableAxis, axisName);
                axisMap.put(chunk.observableAxis, axisName);
                if (chunk.observable == null) {
                    throw new IllegalArgumentException(
                        String.format(METADATA_NOT_FOUND, AXES_VALIDATION_ERROR, axisName, chunk.observableAxis));
                }
            }

            // Validate the number and quality of the axis definitions
            // Count from 1, as 0 will never be filled
            if (axisMap.get(0) != null) {
                throw new IllegalArgumentException(AXES_VALIDATION_ERROR + ": axis definition (0) not allowed: " + axisMap.get(0));
            }
            for (int i = 1; i <= chunk.naxis; i++) {
                if (axisMap.get(i) == null) {
                    throw new IllegalArgumentException(AXES_VALIDATION_ERROR + ": missing axis " + i);
                }
            }

        }
    }

    public static void validateCustomWCS(String context, CustomWCS custom) {
        if (custom != null) {
            try {
                CoordAxis1D customAxis = custom.getAxis();
                if (customAxis.range != null) {
                    Interval s = CustomAxisUtil.toInterval(custom, customAxis.range);
                }
                if (customAxis.bounds != null) {
                    for (CoordRange1D cr : customAxis.bounds.getSamples()) {
                        Interval s1 = CustomAxisUtil.toInterval(custom, cr);
                    }
                }
                if (customAxis.function != null) {
                    Interval s2 = CustomAxisUtil.toInterval(custom, customAxis.function);
                }
            } catch (UnsupportedOperationException ex) {
                // axis is null, most likely
                throw new IllegalArgumentException(CUSTOM_WCS_VALIDATION_ERROR + ex.getMessage() + " in " + context, ex);
            }
        }
    }

}
