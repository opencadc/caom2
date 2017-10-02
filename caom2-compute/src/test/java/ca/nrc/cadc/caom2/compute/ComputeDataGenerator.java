package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jeevesh
 */
public class ComputeDataGenerator
{
    private static final Logger log = Logger.getLogger(ComputeDataGenerator.class);

    private EnergyUtilTest euTest = new EnergyUtilTest();
    String BANDPASS_NAME= "H-Alpha-narrow";
    EnergyTransition TRANSITION = new EnergyTransition("H", "alpha");

    private TimeUtilTest tiTest = new TimeUtilTest();

    //    private SpatialWCS mkGoodSpatialWCS()
    //    {
    //        Axis axis1 = new Axis("RA---TAN", "deg");
    //        Axis axis2 = new Axis("DEC--TAN", "deg");
    //        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
    //        SpatialWCS wcs = new SpatialWCS(axis);
    //        wcs.equinox = null;
    //        Dimension2D dim = new Dimension2D(1024, 1024);
    //        Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
    //        axis.function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 0.0); // singular CD matrix
    //    }
    //    private SpatialWCS mkBadSpatialWCS()
    //    {
    //
    //    }
    //
    //

    Plane getTestPlane(ProductType ptype)
            throws URISyntaxException
    {
        Plane plane = new Plane(ptype.getClass().getName());
        Artifact na = new Artifact(new URI("foo", "bar", null), ptype, ReleaseType.DATA);
        plane.getArtifacts().add(na);
        Part np = new Part("baz");
        na.getParts().add(np);
        np.getChunks().add(new Chunk());
        return plane;
    }

    Chunk getTestChunk(ProductType ptype)
            throws URISyntaxException
    {
        Plane testPlane = getTestPlane(ptype);
        return testPlane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
    }

    Artifact getTestArtifact(ProductType ptype)
            throws URISyntaxException
    {
        Plane testPlane = getTestPlane(ptype);
        return testPlane.getArtifacts().iterator().next();
    }

    // Functions for generating WCS flavours


    SpatialWCS mkGoodSpatialWCS()
    {
        double px = 0.5;
        double py = 0.5;
        double sx = 20.0;
        double sy = 10.0;
        double dp = 1000.0;
        double ds = 1.0;

        return PositionUtilTest.getTestFunction(px, py, sx, sy, false);
    }

    SpatialWCS mkBadSpatialWCS()
    {
        Axis axis1 = new Axis("RA---TAN", "deg");
        Axis axis2 = new Axis("DEC--TAN", "deg");
        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
        SpatialWCS wcs = new SpatialWCS(axis);
        wcs.equinox = null;
        Dimension2D dim = new Dimension2D(1024, 1024);
        Coord2D ref = new Coord2D(new RefCoord(512, 10), new RefCoord(512, 20));
        axis.function = new CoordFunction2D(dim, ref, 1.0e-3, 0.0, 0.0, 0.0); // singular CD matrix
        return wcs;
    }

    SpectralWCS mkGoodSpectralWCS()
    {
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;

        return euTest.getTestRange(true, px, sx * nx * ds, nx, ds);

    }

    SpectralWCS mkBadSpectralWCSFn ()
            throws URISyntaxException
    {

        CoordAxis1D axis = new CoordAxis1D(new Axis("WAVE", "Angstroms"));
//        log.debug("test axis: " + axis);
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        wcs.bandpassName = BANDPASS_NAME;
        wcs.restwav = 6563.0e-10; // meters
        wcs.resolvingPower = 33000.0;
        wcs.transition = TRANSITION;

        Double delta = 0.05;
        RefCoord c1 = new RefCoord(0.5, 2000.0);
        wcs.getAxis().function = new CoordFunction1D((long) 100.0, 10.0, c1);
        log.debug("test function: " + axis.function);
        return wcs;

    }

    //    private SpectralWCS mkBadSpectralWCSBounds()
    //    {
    //
    //    }
    //    private SpectralWCS  mkBadSpectralWCSRange()
    //    {
    //
    //    }
    //
    TemporalWCS mkGoodTemporalWCS()
    {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;

        return tiTest.getTestFunction(true, px, sx*nx*ds, nx, ds);

    }


    TemporalWCS mkBadTemporalWCSCunit()
    {

        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "foo"));
        TemporalWCS wcs = new TemporalWCS(axis);
//        wcs.exposure = 300.0;
//        wcs.resolution = 0.1;
//
//
//        // divide into 2 samples with a gap between
//        RefCoord c1 = new RefCoord(px, sx);
//        RefCoord c2 = new RefCoord(px + nx*0.33, sx + nx*ds*0.33);
//        RefCoord c3 = new RefCoord(px + nx*0.66, sx + nx*ds*0.66);
//        RefCoord c4 = new RefCoord(px + nx,      sx + nx*ds);
//        wcs.getAxis().bounds = new CoordBounds1D();
//        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
//        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));

        return wcs;

    }


    TemporalWCS mkBadTemporalWCSRange()
    {
        double px = 0.5;
        double sx = 54321.0;
        double nx = 200.0;
        double ds = 0.01;

        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);
        wcs.exposure = 300.0;
        wcs.resolution = 0.1;

        // divide into 2 samples with a gap between
        RefCoord c1 = new RefCoord(px, sx);
        RefCoord c2 = new RefCoord(0,0);
        RefCoord c3 = new RefCoord(px + nx*0.66, sx + nx*ds*0.66);
        RefCoord c4 = new RefCoord(px + nx,      sx + nx*ds);
        wcs.getAxis().bounds = new CoordBounds1D();
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c1, c2));
        wcs.getAxis().bounds.getSamples().add(new CoordRange1D(c3, c4));

        return wcs;

    }
    //    private TemporalWCS mkBadTemporalWCSBounds()
    //    {
    //
    //    }
    //    private TemporalWCS mkBadTemporalWCSFn()
    //    {
    //
    //    }
    //
    //
    PolarizationWCS mkGoodPolarizationWCS() throws URISyntaxException
    {
        CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
        PolarizationWCS w = new PolarizationWCS(axis);
        RefCoord c1 = new RefCoord(0.5, PolarizationState.intValue(PolarizationState.CPOLI));
        RefCoord c2 = new RefCoord(1.5, PolarizationState.intValue(PolarizationState.CPOLI));
        w.getAxis().range = new CoordRange1D(c1, c2);
        return w;
    }

    PolarizationWCS mkBadPolarizationWCS() throws URISyntaxException
    {
        double lowErr = -9.0;
        double highErr = 11.0;
        double zeroErr = 0.0;
        RefCoord c1, c2;

        CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
        PolarizationWCS w = new PolarizationWCS(axis);


        c1 = new RefCoord(0.5, zeroErr);
        c2 = new RefCoord(1.5, zeroErr);
        w.getAxis().range = new CoordRange1D(c1, c2);

        return w;
    }

    //    private PolarizationWCS mkBadPolarizationWCSBounds()
    //    {
    //
    //    }
    //    private PolarizationWCS mkBadPolarizationWCSFn()
    //    {
    //
    //    }


}
