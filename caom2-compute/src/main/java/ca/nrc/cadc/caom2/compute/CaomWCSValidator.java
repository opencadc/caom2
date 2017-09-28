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
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jeevesh
 */
public class CaomWCSValidator
{

    private static final String SPATIAL_WCS_VALIDATION_ERROR = "Invalid SpatialWCS in Chunk: ";
    private static final String SPECTRAL_WCS_VALIDATION_ERROR = "Invalid SpectralWCS in Chunk: ";
    private static final String TEMPORAL_WCS_VALIDATION_ERROR = "Invalid TemporalWCS in Chunk: ";
    private static final String POLARIZATION_WCS_VALIDATION_ERROR = "Invalid PolarizationWCS in Chunk: ";

    public CaomWCSValidator()
    {
    }

    public static void validate(Artifact a)
        throws IllegalArgumentException
    {

        for (Part p : a.getParts())
        {
            for (Chunk c : p.getChunks())
            {

                // Position check
                try
                {
                    // Convert to polygon using native coordinate system
                    MultiPolygon nextMP = PositionUtil.toPolygon(c.position);

                    Point centre = nextMP.getCenter();

                    WCSWrapper map = new WCSWrapper(c.position, 1, 2);
                    Transform transform = new Transform(map);

                    double[] coords = new double[2];
                    coords[0] = centre.cval1;
                    coords[1] = centre.cval2;
                    Transform.Result tr = transform.pix2sky(coords);

                }
                catch (NoSuchKeywordException ne)
                {
                    throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
                }
                catch (UnsupportedOperationException uoe)
                {
                    // from toPolygon if WCS is too near a pole, or if the bounds
                    // value is not recognized
                    throw new IllegalArgumentException(SPATIAL_WCS_VALIDATION_ERROR, uoe);
                }


                // Energy interval checks
                try
                {
                    // convert wcs to energy axis interval
                    CoordAxis1D energyAxis = c.energy.getAxis();

                    if (energyAxis.range != null)
                    {
                        SubInterval s = EnergyUtil.toInterval(c.energy, energyAxis.range);
                    }

                    if (energyAxis.bounds != null)
                    {
                        for (CoordRange1D tile : energyAxis.bounds.getSamples())
                        {
                            SubInterval bwmRange = EnergyUtil.toInterval(c.energy, tile);
                        }
                    }

                    if (energyAxis.function != null)
                    {
                        // check range, bounds and function versions
                        SubInterval sei = EnergyUtil.toInterval(c.energy, energyAxis.function);
                    }

                }
                catch (NoSuchKeywordException ne)
                {
                    throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
                }



                // Time checks
                try
                {
                    SubInterval sti = TimeUtil.toInterval(c.time, c.time.getAxis().function);
                    CoordAxis1D timeAxis = c.time.getAxis();
                    if (timeAxis.range != null)
                    {
                        SubInterval s = TimeUtil.toInterval(c.time, timeAxis.range);
                    }
                    if (timeAxis.bounds != null)
                    {
                        for (CoordRange1D cr : timeAxis.bounds.getSamples())
                        {
                            SubInterval s1 = TimeUtil.toInterval(c.time, cr);
                        }
                    }
                    if (timeAxis.function != null)
                    {
                        SubInterval s2 = TimeUtil.toInterval(c.time, timeAxis.function);
                    }
                }
                catch (UnsupportedOperationException uoe)
                {
                    // timesys or CUNIT error
                    throw new IllegalArgumentException(TEMPORAL_WCS_VALIDATION_ERROR, uoe);
                }


                // Polarization checks
                try
                {
                    CoordAxis1D polarizationAxis = c.polarization.getAxis();
                    List<PolarizationState> pol = new ArrayList<PolarizationState>();

                    if (polarizationAxis.range != null)
                    {
                        int lb = (int) polarizationAxis.range.getStart().val;
                        int ub = (int) polarizationAxis.range.getEnd().val;
                        for (int i=lb; i <= ub; i++)
                        {
                            pol.add(PolarizationState.toValue(i));
                        }
                    }
                    else if (polarizationAxis.bounds != null)
                    {
                        for (CoordRange1D cr : polarizationAxis.bounds.getSamples())
                        {
                            int lb = (int) cr.getStart().val;
                            int ub = (int) cr.getEnd().val;
                            for (int i=lb; i <= ub; i++)
                            {
                                pol.add(PolarizationState.toValue(i));
                            }
                        }
                    }
                    else if (polarizationAxis.function != null)
                    {
                        for (int i=1; i <= polarizationAxis.function.getNaxis(); i++)
                        {
                            double pix = (double) i;
                            int val = (int) Util.pix2val(polarizationAxis.function, pix);
                            pol.add(PolarizationState.toValue(val));
                        }
                    }

                }
                catch (UnsupportedOperationException uoe)
                {
                    // from toPolygon if WCS is too near a pole, or if the bounds
                    // value is not recognized
                    throw new IllegalArgumentException(POLARIZATION_WCS_VALIDATION_ERROR, uoe);
                }

            }
        }
    }

}
