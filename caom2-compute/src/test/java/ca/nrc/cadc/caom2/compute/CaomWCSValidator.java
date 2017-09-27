package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Created by jeevesh on 2017-09-26.
 */
public class CaomWCSValidator
{

    private final String SPATIAL_WCS_VALIDATION_ERROR = "Invalid SpatialWCS in Chunk: ";
    private final String SPECTRAL_WCS_VALIDATION_ERROR = "Invalid SpectralWCS in Chunk: ";
    private final String TEMPORAL_WCS_VALIDATION_ERROR = "Invalid TemporalWCS in Chunk: ";
    private final String POLARIZATION_WCS_VALIDATION_ERROR = "Invalid PolarizationWCS in Chunk: ";

    public CaomWCSValidator()
    {
    }


    public void validate(Artifact a)
        throws IllegalArgumentException
    {
        // This needs to:
        // for each Chunk in the artifact...

        // TODO: remove this later, only for ref for building code paths
        //        public Integer naxis;
        //        public Integer observableAxis;
        //        public Integer positionAxis1;
        //        public Integer positionAxis2;
        //        public Integer energyAxis;
        //        public Integer timeAxis;
        //        public Integer polarizationAxis;
        //
        //        public ObservableAxis observable;
        //        public SpatialWCS position;
        //        public SpectralWCS energy;
        //        public TemporalWCS time;
        //        public PolarizationWCS polarization;


        for (Part p : a.getParts()) // currently, only FITS files have parts and chunks
        {
            for (Chunk c : p.getChunks())
            {

                // Attempt to convert to a polygon
                if (CutoutUtil.canPositionCutout(c))
                {
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
                }

                // Energy interval check
                if (CutoutUtil.canEnergyCutout(c))
                {
                    try
                    {
                        // convert wcs to energy axis interval
                        SubInterval sei = EnergyUtil.toInterval(c.energy, c.energy.getAxis().function);
                    }
                    catch (NoSuchKeywordException ne)
                    {
                        throw new IllegalArgumentException(SPECTRAL_WCS_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
                    }

                }

                // Time check
                try
                {
                    // convert wcs to energy axis interval
                    SubInterval sti = TimeUtil.toInterval(c.time, c.time.getAxis().function);
                }
                catch (UnsupportedOperationException uoe)
                {
                    // from toPolygon if WCS is too near a pole, or if the bounds
                    // value is not recognized
                    throw new IllegalArgumentException(TEMPORAL_WCS_VALIDATION_ERROR, uoe);
                }

                // Polarization check
                try
                {
                    //Q: calculation for this and time intervals do this fall through: range, or bounds, last function.
                    // Does the same pattern need to be applied here?
                    Set<PolarizationState> pol = EnumSet.noneOf(PolarizationState.class);
                    CoordFunction1D function = c.polarization.getAxis().function;
                    for (int i=1; i <= function.getNaxis(); i++)
                    {
                        double pix = (double) i;
                        int val = (int) Util.pix2val(function, pix);
                        pol.add(PolarizationState.toValue(val));
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
