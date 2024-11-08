package ca.nrc.cadc.caom2.compute;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jeevesh
 */
public class ComputeDataGenerator {
    private static final Logger log = Logger.getLogger(ComputeDataGenerator.class);

    private EnergyUtilTest euTest = new EnergyUtilTest();
    String BANDPASS_NAME = "H-Alpha-narrow";
    EnergyTransition TRANSITION = new EnergyTransition("H", "alpha");

    private TimeUtilTest tiTest = new TimeUtilTest();
    private CustomAxisUtilTest cuTest = new CustomAxisUtilTest();

    Chunk getFreshChunk() {
        Chunk testChunk = new Chunk();

        // Define some sort of axis set that may or may not make sense in reality,
        // but will pass validation

        testChunk.naxis = 5;
        testChunk.observableAxis = 1;
        testChunk.observable = mkGoodObservableAxis("WAVE", "m");
        testChunk.positionAxis1 = 2;
        testChunk.positionAxis2 = 3;
        testChunk.position = mkGoodSpatialWCS();
        testChunk.timeAxis = 4;
        testChunk.time = mkGoodTemporalWCS();
        testChunk.customAxis = 5;
        testChunk.custom = mkGoodCustomWCS();
        return testChunk;
    }

    Plane getTestPlane(DataLinkSemantics ptype)
        throws URISyntaxException {
        Plane plane = new Plane(ptype.getClass().getName());
        Artifact na = new Artifact(new URI("foo", "bar", null), ptype, ReleaseType.DATA);
        plane.getArtifacts().add(na);
        Part np = new Part("baz");
        na.getParts().add(np);
        np.getChunks().add(getFreshChunk());
        return plane;
    }

    Chunk getTestChunk(DataLinkSemantics ptype)
        throws URISyntaxException {
        Plane testPlane = getTestPlane(ptype);
        return testPlane.getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
    }

    Artifact getTestArtifact(DataLinkSemantics ptype)
        throws URISyntaxException {
        Plane testPlane = getTestPlane(ptype);
        return testPlane.getArtifacts().iterator().next();
    }


    // Functions for generating WCS flavours
    SpatialWCS mkGoodSpatialWCS() {
        double px = 0.5;
        double py = 0.5;
        double sx = 20.0;
        double sy = 10.0;
        double dp = 1000.0;
        double ds = 1.0;

        return PositionUtilTest.getTestFunction(px, py, sx, sy, false);
    }

    SpatialWCS mkBadSpatialWCS() {
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

    SpectralWCS mkGoodSpectralWCS() {
        double px = 0.5;
        double sx = 400.0;
        double nx = 200.0;
        double ds = 1.0;
        SpectralWCS energy = euTest.getTestRange(true, px, sx * nx * ds, nx, ds);

        RefCoord c1 = new RefCoord(0.5, 2000.0);
        energy.getAxis().function = new CoordFunction1D((long) 100.0, 10.0, c1);
        return energy;
    }

    SpectralWCS mkBadSpectralWCSFn()
        throws URISyntaxException {

        CoordAxis1D axis = new CoordAxis1D(new Axis("WAVE", "Angstroms"));
        SpectralWCS wcs = new SpectralWCS(axis, "TOPOCENT");
        wcs.bandpassName = BANDPASS_NAME;
        wcs.restwav = 6563.0e-10; // meters
        wcs.resolvingPower = 33000.0;
        wcs.transition = TRANSITION;

        RefCoord c1 = new RefCoord(0.5, 2000.0);
        wcs.getAxis().function = new CoordFunction1D((long) 100.0, 10.0, c1);
        log.debug("test function: " + axis.function);
        return wcs;

    }

    TemporalWCS mkGoodTemporalWCS() {
        double px = 0.5;
        double sx = 1.0;
        double nx = 200.0;
        double ds = 1;

        return tiTest.getTestFunction(true, px, sx * nx * ds, nx, ds);
    }

    TemporalWCS mkBadTemporalWCSCunit() {
        RefCoord c1 = new RefCoord(0.5, 54321.0);
        RefCoord c2 = new RefCoord(100.5, 54321.5);
        
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "foo"));
        TemporalWCS wcs = new TemporalWCS(axis);
        wcs.getAxis().range = new CoordRange1D(c1, c2);
        return wcs;

    }

    TemporalWCS mkBadTemporalWCSFunction() {
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);

        // invalid: delta == 0.0 with 100 pixels
        RefCoord c1 = new RefCoord(0.5, 2000.0);
        wcs.getAxis().function = new CoordFunction1D((long) 100.0, 0.0, c1);

        return wcs;

    }
    
    TemporalWCS mkBadTemporalWCSRange() {
        CoordAxis1D axis = new CoordAxis1D(new Axis("UTC", "d"));
        TemporalWCS wcs = new TemporalWCS(axis);

        // implied multiple pixerls with delta = 0
        RefCoord c1 = new RefCoord(0.5, 54321.0);
        RefCoord c2 = new RefCoord(2.5, 54321.0);
        wcs.getAxis().range = new CoordRange1D(c1, c2);

        return wcs;

    }

    PolarizationWCS mkGoodPolarizationWCS() throws URISyntaxException {
        CoordAxis1D axis = new CoordAxis1D(new Axis("STOKES", null));
        PolarizationWCS w = new PolarizationWCS(axis);
        RefCoord c1 = new RefCoord(0.5, PolarizationState.intValue(PolarizationState.CPOLI));
        RefCoord c2 = new RefCoord(1.5, PolarizationState.intValue(PolarizationState.CPOLI));
        w.getAxis().range = new CoordRange1D(c1, c2);
        return w;
    }

    PolarizationWCS mkBadPolarizationWCS() throws URISyntaxException {
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

    CustomWCS mkGoodCustomWCS() {
        double px = 0.5;
        double sx = 1.0;
        double nx = 200.0;
        double ds = 1;

        return cuTest.getTestFunction(px, sx * nx * ds, nx, ds);
    }

    ObservableAxis mkGoodObservableAxis(String ctype, String cunit) {
        Axis sliceAxis = new Axis(ctype, cunit);
        long bin = 1;
        Slice dependent = new Slice(sliceAxis, bin);
        return new ObservableAxis(dependent);
    }

    CustomWCS mkBadCtypeCustomWCS() {
        RefCoord c1 = new RefCoord(0.5, 10.0);
        RefCoord c2 = new RefCoord(100.5, 20.0);

        CoordAxis1D axis = new CoordAxis1D(new Axis("BAD_CTYPE", CustomAxisUtilTest.TEST_CUNIT));
        CustomWCS wcs = new CustomWCS(axis);
        wcs.getAxis().range = new CoordRange1D(c1, c2);
        return wcs;
    }

    CustomWCS mkBadCunitCustomWCS() {
        RefCoord c1 = new RefCoord(0.5, 10.0);
        RefCoord c2 = new RefCoord(100.5, 20.0);

        CoordAxis1D axis = new CoordAxis1D(new Axis(CustomAxisUtilTest.TEST_RM_CTYPE, "HelloKitty"));
        CustomWCS wcs = new CustomWCS(axis);
        wcs.getAxis().range = new CoordRange1D(c1, c2);
        return wcs;
    }


}
