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
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;


/**
 * Created by jeevesh
 */
public class CaomWCSValidator {

    private static final String SPATIAL_WCS_VALIDATION_ERROR = "Invalid SpatialWCS in Chunk: ";
    private static final String SPECTRAL_WCS_VALIDATION_ERROR = "Invalid SpectralWCS in Chunk: ";
    private static final String TEMPORAL_WCS_VALIDATION_ERROR = "Invalid TemporalWCS in Chunk: ";
    private static final String POLARIZATION_WCS_VALIDATION_ERROR = "Invalid PolarizationWCS in Chunk: ";

    /**
     * Validate all WCS types included in the Chunks belonging to the given Artifact.
     *
     * @param a
     * @throws IllegalArgumentException
     */
    public static void validate(Artifact a)
        throws IllegalArgumentException {
        if (a != null) {
            for (Part p : a.getParts()) {
                if (p != null) {
                    for (Chunk c : p.getChunks()) {
                        validateSpatialWCS(c.position);
                        validateSpectralWCS(c.energy);
                        validateTemporalWCS(c.time);
                        validatePolarizationWCS(c.polarization);
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
    public static void validateChunk(Chunk c)
        throws IllegalArgumentException {
        validateSpatialWCS(c.position);
        validateSpectralWCS(c.energy);
        validateTemporalWCS(c.time);
        validatePolarizationWCS(c.polarization);
    }


    public static void validateSpatialWCS(SpatialWCS position) {
        if (position != null) {
            try {
                // Convert to polygon using native coordinate system
                MultiPolygon nextMP = PositionUtil.toPolygon(position);

                if (position.getAxis().function != null) {
                    Point center = nextMP.getCenter();

                    WCSWrapper map = new WCSWrapper(position, 1, 2);
                    Transform transform = new Transform(map);

                    double[] coords = new double[2];
                    coords[0] = center.cval1;
                    coords[1] = center.cval2;
                    Transform.Result tr = transform.sky2pix(coords);
                }
            } catch (NoSuchKeywordException ne) {
                throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
            } catch (UnsupportedOperationException uoe) {
                // error thrown from toPolygon if WCS is too near a pole, or if the bounds
                // value is not recognized
                throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR, uoe);
            }
        }
    }


    public static void validateSpectralWCS(SpectralWCS energy) {
        if (energy != null) {
            try {
                if (energy.getAxis() != null) {
                    CoordAxis1D energyAxis = energy.getAxis();
                    SubInterval si = null;

                    if (energyAxis.range != null) {
                        si = EnergyUtil.toInterval(energy, energyAxis.range);
                    }

                    if (energyAxis.bounds != null) {
                        for (CoordRange1D tile : energyAxis.bounds.getSamples()) {
                            si = EnergyUtil.toInterval(energy, tile);
                        }
                    }

                    if (energyAxis.function != null) {

                        SubInterval sei = EnergyUtil.toInterval(energy, energyAxis.function);

                        WCSWrapper map = new WCSWrapper(energy, 1);
                        Transform transform = new Transform(map);

                        double[] coord = new double[1];
                        Transform.Result tr = null;
                        coord[0] = (si.getUpper() - si.getLower()) / 2;
                        tr = transform.sky2pix(coord);
                    }
                } else {
                    throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + " energy axis cannot be null.");
                }

            } catch (NoSuchKeywordException ne) {
                throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
            }
        }

    }


    public static void validateTemporalWCS(TemporalWCS time) {
        if (time != null) {
            try {
                if (time.getAxis() != null) {
                    CoordAxis1D timeAxis = time.getAxis();
                    if (timeAxis.range != null) {
                        SubInterval s = TimeUtil.toInterval(time, timeAxis.range);
                    }
                    if (timeAxis.bounds != null) {
                        for (CoordRange1D cr : timeAxis.bounds.getSamples()) {
                            SubInterval s1 = TimeUtil.toInterval(time, cr);

                        }
                    }
                    if (timeAxis.function != null) {
                        SubInterval s2 = TimeUtil.toInterval(time, timeAxis.function);

                        // Currently there is no WCSWrapper for time, so sky2pix
                        // transformation can't be done
                        //                        WCSWrapper map = new WCSWrapper(time, 1);
                        //                        Transform transform = new Transform(map);
                        //                        double[] coord = new double[1];
                        //                        coord[0] = (s1.getUpper() - s1.getLower())/2;
                        //                        Transform.Result tr = transform.sky2pix(coord);
                    }
                } else {
                    throw new IllegalArgumentException(TEMPORAL_WCS_VALIDATION_ERROR + " time axis cannot be null.");
                }
            } catch (UnsupportedOperationException uoe) {
                // timesys or CUNIT error
                throw new IllegalArgumentException(TEMPORAL_WCS_VALIDATION_ERROR, uoe);
            }
        }
    }


    public static void validatePolarizationWCS(PolarizationWCS polarization) {
        if (polarization != null) {
            try {
                if (polarization.getAxis() != null) {
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
                } else {
                    throw new IllegalArgumentException(POLARIZATION_WCS_VALIDATION_ERROR + " polarization axis cannot be null.");
                }

            } catch (UnsupportedOperationException uoe) {
                throw new IllegalArgumentException(POLARIZATION_WCS_VALIDATION_ERROR, uoe);
            }
        }
    }

}
