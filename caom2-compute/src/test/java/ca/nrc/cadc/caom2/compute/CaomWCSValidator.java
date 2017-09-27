package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.types.IllegalPolygonException;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jeevesh on 2017-09-26.
 */
public class CaomWCSValidator
{
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
                        throw new IllegalArgumentException(CHUNK_VALIDATION_ERROR + ": invalid keyword in WCS", ne);
                    }
                    catch (UnsupportedOperationException uoe)
                    {
                        // from toPolygon if WCS is too near a pole, or if the bounds
                        // value is not recognized
                        throw new IllegalArgumentException(CHUNK_VALIDATION_ERROR, uoe);
                    }
                }

                if (CutoutUtil.canEnergyCutout(c))
                {

                    long[] cut = getEnergyBounds(c.energy, energyInter);
                    if (nrgCut == null)
                        nrgCut = cut;
                    else if (nrgCut.length == 2 && cut != null) // subset
                    {
                        if (cut.length == 0)
                            nrgCut = cut;
                        else // both are length 4
                        {
                            nrgCut[0] = Math.min(nrgCut[0], cut[0]);
                            nrgCut[1] = Math.max(nrgCut[1], cut[1]);
                        }
                    }
                }
            }
        }

        // take the energy SpectralWCS and get the interval?

        // temporal WCS

    }

    private final String CHUNK_VALIDATION_ERROR = "Invalid Chunk: ";

    public MultiPolygon validateSpatialWCS(Chunk c)
            throws IllegalArgumentException
    {
        // transform it to a polygon, trap and sanely process
        // any returned exceptions
        MultiPolygon newMP = new MultiPolygon();
        try
        {
            newMP = PositionUtil.toPolygon(c.position);
        }
        catch (NoSuchKeywordException nske)
        {
            // Comes from the Transform class constructor
            // throw the illegalArgumentException?
            throw new IllegalArgumentException(CHUNK_VALIDATION_ERROR + " no such keyword", nske);
        }
        catch (UnsupportedOperationException uoe)
        {
            // from toPolygon if WCS is too near a pole, or if the bounds
            // value is not recognized
            throw new IllegalArgumentException(CHUNK_VALIDATION_ERROR, uoe);
        }
        // TODO : this only applies to cutouts. Should it still be checked for here?
//        catch (IllegalPolygonException ipe)
//        {
//            // Thrown from toPolygon if polygon area is too large or if
//            // centre of polygon isn't sane.
//            throw new IllegalArgumentException(CHUNK_VALIDATION_ERROR + " illegal polygon.", ipe);
//        }

        return newMP;
    }


}
