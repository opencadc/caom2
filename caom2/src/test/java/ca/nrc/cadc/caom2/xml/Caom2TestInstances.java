/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.xml;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CustomAxis;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.DerivedObservation;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyBand;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Observable;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Quality;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.Requirements;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Status;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.TargetType;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SampledInterval;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class Caom2TestInstances
{
    private static Logger log = Logger.getLogger(Caom2TestInstances.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }
    
    private int childCount;
    private int depth;
    private boolean complete;
    private boolean boundsIsCircle;
    
     // using TEST collection so the permanent test document can be re-used in caom2-test-repo
    public static String collection = "TEST";
    public static String observationID = "observationID";
    public static String productID = "productID";
    public static List<String> keywords;
    public static List<URI> metaGroups;
    public static List<URI> dataGroups;
    
    static
    {
        keywords = new ArrayList<String>();
        keywords.add("keyword1");
        keywords.add("keyword2");
        
        metaGroups = new ArrayList<URI>();
        metaGroups.add(URI.create("ivo://cadc.nrc.ca/gms?A"));
        metaGroups.add(URI.create("ivo://cadc.nrc.ca/gms?B"));
        
        dataGroups = new ArrayList<URI>();
        dataGroups.add(URI.create("ivo://cadc.nrc.ca/gms?C"));
        dataGroups.add(URI.create("ivo://cadc.nrc.ca/gms?D"));
    }
    public static Date ivoaDate = Calendar.getInstance().getTime();

    public Caom2TestInstances()
    {
        this.childCount = 1;
        this.depth = 5;
        this.complete = true;
        this.boundsIsCircle = true;
    }

    public void setChildCount(int childCount)
    {
        this.childCount = childCount;
    }
    
    public void setDepth(int depth)
    {
        this.depth = depth;
    }
    
    public void setComplete(boolean complete)
    {
        this.complete = complete;
    }
    
    public void setBoundsIsCircle(boolean boundsIsCircle)
    {
        this.boundsIsCircle = boundsIsCircle;
    }
    
    public SimpleObservation getSimpleObservation()
        throws Exception
    {        
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        if (complete)
        {
            observation.metaProducer = URI.create("test:observation/rountrip-1.0");
            observation.type = "OBJECT";
            observation.intent = ObservationIntentType.SCIENCE;
            observation.metaRelease = ivoaDate;
            observation.getMetaReadGroups().addAll(metaGroups);
            observation.sequenceNumber = new Integer(123);
            observation.proposal = getProposal();
            observation.target = getTarget();
            observation.targetPosition = getTargetPosition("ICRS", null);
            observation.requirements = new Requirements(Status.FAIL);
            observation.telescope = getTelescope();
            observation.instrument = getInstrument();
            observation.environment = getEnvironment();
        }
        if (depth > 1)
            observation.getPlanes().addAll(getPlanes());
     
        return observation;
    }

    public SimpleObservation getSimpleObservationSetAlgorithm()
            throws Exception
    {
        SimpleObservation observation = new SimpleObservation(collection, observationID, getAlgorithm());
        if (complete)
        {
            observation.metaProducer = URI.create("test:observation/rountrip-1.0");
            observation.type = "OBJECT";
            observation.intent = ObservationIntentType.SCIENCE;
            observation.metaRelease = ivoaDate;
            observation.getMetaReadGroups().addAll(metaGroups);
            observation.sequenceNumber = new Integer(123);
            observation.proposal = getProposal();
            observation.target = getTarget();
            observation.targetPosition = getTargetPosition("ICRS", null);
            observation.requirements = new Requirements(Status.FAIL);
            observation.telescope = getTelescope();
            observation.instrument = getInstrument();
            observation.environment = getEnvironment();
        }
        if (depth > 1)
            observation.getPlanes().addAll(getPlanes());

        return observation;
    }
    
    public DerivedObservation getCompositeObservation()
        throws Exception
    {
        DerivedObservation observation = new DerivedObservation(collection, observationID, getAlgorithm());
        if (complete)
        {
            observation.metaProducer = URI.create("test:observation/rountrip-1.0");
            observation.type = "field";
            observation.intent = ObservationIntentType.SCIENCE;
            observation.metaRelease = ivoaDate;
            observation.getMetaReadGroups().addAll(metaGroups);
            observation.sequenceNumber = new Integer(123);
            observation.proposal = getProposal();
            observation.target = getTarget();
            observation.targetPosition = getTargetPosition("FK5", 2000.0);
            observation.requirements = new Requirements(Status.FAIL);
            observation.telescope = getTelescope();
            observation.instrument = getInstrument();
            observation.environment = getEnvironment();
        }
        if (depth > 1)
        {
            observation.getPlanes().addAll(getPlanes());
            observation.getMembers().addAll(getMembers());
        }
        
        return observation;
    }
    
    protected Algorithm getAlgorithm()
    {
        return new Algorithm("algorithmName");
    }
    
    protected Proposal getProposal()
    {
        Proposal proposal = new Proposal("proposalId");
        proposal.pi = "proposalPi";
        proposal.project = "proposalProject";
        proposal.title = "proposalTitle";
        proposal.getKeywords().addAll(keywords);
        return proposal;
    }
    
    protected Target getTarget()
    {
        Target target = new Target("targetName");
        target.targetID = URI.create("naif:1701");
        target.type = TargetType.OBJECT;
        target.standard = Boolean.FALSE;
        target.redshift = 1.5;
        target.moving = Boolean.FALSE;
        target.getKeywords().addAll(keywords);
        return target;
    }
    
    protected TargetPosition getTargetPosition(String coordsys, Double equinox)
    {
        TargetPosition tp = new TargetPosition(coordsys, new Point(1.0, 2.0));
        tp.equinox = equinox;
        return tp;
    }
    
    protected Telescope getTelescope()
    {
        Telescope telescope = new Telescope("telescopeName");
        telescope.geoLocationX = 1.0;
        telescope.geoLocationY = 2.0;
        telescope.geoLocationZ = 3.0;
        telescope.getKeywords().addAll(keywords);
        return telescope;
    }
    
    protected Instrument getInstrument()
    {
        Instrument instrument = new Instrument("instrumentName");
        instrument.getKeywords().addAll(keywords);
        return instrument;
    }

    protected Environment getEnvironment()
    {
        Environment env = new Environment();
        env.seeing = 0.08;
        env.humidity = 0.35;
        env.elevation = 2.7;
        env.tau = 1.7;
        env.wavelengthTau = 450e-6;
        env.ambientTemp = 666.0;
        env.photometric = Boolean.TRUE;
        return env;
    }
    
    protected Set<ObservationURI> getMembers()
        throws Exception
    {
        Set<ObservationURI> members = new TreeSet<ObservationURI>();
        members.add(new ObservationURI(new URI("caom:foo/bar")));
        members.add(new ObservationURI(new URI("caom:foo/baz")));
        return members;
    }
    
    protected Set<Plane> getPlanes()
        throws Exception
    {
        Set<Plane> planes = new TreeSet<Plane>();
        
        for (int i=0; i<childCount; i++)
        {
            Plane p = new Plane("productID"+i);
            if (complete)
            {
                p.metaProducer = URI.create("test:plane/rountrip-1.0");
                p.creatorID = new URI("ivo://example.org/foo?" + p.getProductID());
                p.metaRelease = ivoaDate;
                p.getMetaReadGroups().addAll(metaGroups);
                p.dataRelease = ivoaDate;
                p.getDataReadGroups().addAll(dataGroups);
                p.dataProductType = DataProductType.IMAGE;
                p.calibrationLevel = CalibrationLevel.PRODUCT;
                p.provenance = getProvenance();
                p.metrics = getMetrics();
                p.quality = new DataQuality(Quality.JUNK);
                p.observable = new Observable("phot.count");
                
                // alphabetical so easier to trace and debug metaChecksum computation
            
                p.energy = new Energy();
                p.energy.bandpassName = "V";
                p.energy.bounds = new SampledInterval(400e-6, 900e-6);
                p.energy.bounds.getSamples().add(new Interval(400e-6, 500e-6));
                p.energy.bounds.getSamples().add(new Interval(800e-6, 900e-6));
                p.energy.dimension = 2L;
                for (EnergyBand eb : EnergyBand.values()) {
                    p.energy.getEnergyBands().add(eb);
                }
                p.energy.resolvingPower = 2.0;
                p.energy.resolvingPowerBounds = new Interval(2.0, 5.0);
                p.energy.restwav = 600e-9;
                p.energy.sampleSize = 100e-6;
                p.energy.transition = new EnergyTransition("H", "alpha");

                p.polarization = new Polarization();
                p.polarization.dimension = 3L;
                p.polarization.states = new TreeSet<PolarizationState>();
                p.polarization.states.add(PolarizationState.I);
                p.polarization.states.add(PolarizationState.Q);
                p.polarization.states.add(PolarizationState.U);

                p.position = new Position();
                if (i == 0) {
                    MultiPolygon poly = new MultiPolygon();
                    poly.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
                    poly.getVertices().add(new Vertex(1.0, 4.0, SegmentType.LINE));
                    poly.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
                    poly.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
                    List<Point> points = new ArrayList<Point>();
                    for (Vertex v : poly.getVertices()) {
                        if (!SegmentType.CLOSE.equals(v.getType())) {
                            points.add(new Point(v.cval1, v.cval2));
                        }
                    }
                    p.position.bounds = new Polygon(points, poly);
                } else { 
                    p.position.bounds = new Circle(new Point(2.0, 4.0), 1.0);
                }
                p.position.dimension = new Dimension2D(1024, 2048);
                p.position.resolution = 0.05;
                p.position.resolutionBounds = new Interval(0.04, 0.06);
                p.position.sampleSize = 0.025;
                p.position.timeDependent = false;

                p.time = new Time();
                p.time.bounds = new SampledInterval(50000.25, 50000.75);
                p.time.bounds.getSamples().add(new Interval(50000.25, 50000.40));
                p.time.bounds.getSamples().add(new Interval(50000.50, 50000.75));
                p.time.dimension = 2L;
                p.time.exposure = 666.0;
                p.time.resolution = 0.5;
                p.time.resolutionBounds = new Interval(0.5, 0.5);
                p.time.sampleSize = 0.15;
                
                p.custom = new CustomAxis("FARADAY");
                p.custom.bounds = new SampledInterval(10.0, 20.0);
                p.custom.bounds.getSamples().add(new Interval(10.0, 13.0));
                p.custom.bounds.getSamples().add(new Interval(17.0, 20.0));
                p.custom.dimension = 10L;
            }
            if (depth > 2)
                p.getArtifacts().addAll(getArtifacts());
        
            planes.add(p);
        }
        return planes;
    }
    
    protected Provenance getProvenance()
        throws Exception
    {
        Provenance provenance = new Provenance("provenanceName");
        provenance.version = "provenanceVersion";
        provenance.project = "provenanceProject";
        provenance.producer = "provenanceProducer";
        provenance.runID = "provenanceRunID";
        provenance.reference = new URI("http://foo/bar");
        provenance.lastExecuted = ivoaDate;
        provenance.getKeywords().addAll(keywords);
        provenance.getInputs().addAll(getInputs());
        return provenance;
    }

    protected Metrics getMetrics()
    {
        Metrics met = new Metrics();
        met.sourceNumberDensity = 100.0;
        met.background = 33.3;
        met.backgroundStddev = 0.2;
        met.fluxDensityLimit = 1.0e-8;
        met.magLimit = 27.5;
        met.sampleSNR = 2.7;
        return met;
    }
    
    protected Set<PlaneURI> getInputs()
        throws Exception
    {
        Set<PlaneURI> inputs = new TreeSet<PlaneURI>();
        inputs.add(new PlaneURI(new URI("caom:foo/bar/plane1")));
        inputs.add(new PlaneURI(new URI("caom:foo/bar/plane2")));
        return inputs;
    }
    
    protected Set<Artifact> getArtifacts()
        throws Exception
    {
        Set<Artifact> artifacts = new TreeSet<Artifact>();
        
        for (int i=0; i<childCount; i++)
        {
            Artifact artifact = new Artifact(new URI("ad:foo/bar"+i), ProductType.SCIENCE, ReleaseType.DATA);
            if (complete)
            {
                artifact.metaProducer = URI.create("test:artifact/rountrip-1.0");
                artifact.contentType = "application/fits";
                artifact.contentLength = 12345L;
                artifact.contentChecksum = new URI("md5:d41d8cd98f00b204e9800998ecf8427e");
                artifact.contentRelease = ivoaDate;
                artifact.getContentReadGroups().addAll(dataGroups);
            }
            if (depth > 3)
                artifact.getParts().addAll(getParts());

            artifacts.add(artifact);
        }
        return artifacts;
    }
    
    protected Set<Part> getParts()
        throws Exception
    {
        Set<Part> parts = new TreeSet<Part>();
        for (int i=0; i<childCount; i++)
        {
            Part part = new Part("x"+i);
            if (complete)
            {
                part.metaProducer = URI.create("test:part/rountrip-1.0");
                part.productType = ProductType.SCIENCE;
            }
            if (depth > 4)
                part.getChunks().addAll(getChunks());

            parts.add(part);
        }
        return parts;
    }
    
    protected Set<Chunk> getChunks()
        throws Exception
    {
        Set<Chunk> chunks = new TreeSet<Chunk>();
        for (int i=0; i<childCount; i++)
        {
            Chunk chunk = new Chunk();
            if (complete)
            {
                chunk.metaProducer = URI.create("test:chunk/rountrip-1.0");
                chunk.naxis = 5;
                chunk.positionAxis1 = 1;
                chunk.positionAxis2 = 2;
                chunk.energyAxis = 3;
                chunk.timeAxis = 4;
                chunk.polarizationAxis = 5;
                chunk.observableAxis = 6;
                chunk.customAxis = 7;

                chunk.observable = getObservableAxis();
                chunk.position = getSpatialWCS();
                chunk.energy = getSpectralWCS();
                chunk.time = getTemporalWCS();
                chunk.polarization = getPolarizationWCS();
                chunk.custom = getCustomWCS();
            }
            chunks.add(chunk);
        }
        return chunks;
    }
    
    protected ObservableAxis getObservableAxis()
        throws Exception
    {
        ObservableAxis observable = new ObservableAxis(getSlice());
        if (complete)
        {
            observable.independent = getSlice();
        }
        return observable;
    }
    
    protected SpatialWCS getSpatialWCS()
        throws Exception
    {
        CoordAxis2D coordAxis2D = getCoordAxis2D();
        SpatialWCS position = new SpatialWCS(coordAxis2D);
        if (complete)
        {
            position.coordsys = "ICRS";
            position.equinox = 2000.0;
            position.resolution = 0.5;
        }
        return position;
    }
    
    protected SpectralWCS getSpectralWCS()
        throws Exception
    {
        CoordAxis1D axis = getCoordAxis1D(true);
        SpectralWCS energy = new SpectralWCS(axis, "BARCENT");
        if (complete)
        {    
            energy.ssysobs = "BARCENT";
            energy.ssyssrc = "BARCENT";
            energy.restfrq = 1.0;
            energy.restwav = 2.0;
            energy.velosys = 3.0;
            energy.zsource = 4.0;
            energy.velang = 5.0;
            energy.bandpassName = "energy bandpassName";
            energy.resolvingPower = 6.0;
            energy.transition = new EnergyTransition("H", "21cm");
        }
        return energy;
    }
    
    protected TemporalWCS getTemporalWCS()
        throws Exception
    {
        CoordAxis1D axis = getCoordAxis1D(false);
        TemporalWCS time = new TemporalWCS(axis);
        if (complete)
        { 
            time.exposure = 1.0;
            time.resolution = 2.0;
            time.timesys = "UTC";
            time.trefpos = "TOPOCENTER";
        }
        return time;
    }
    
    protected PolarizationWCS getPolarizationWCS()
        throws Exception
    {
        Axis axis = new Axis("STOKES", null);
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        if (complete)
        {
            coordAxis1D.error = new CoordError(1.0, 1.5);
            coordAxis1D.range = new CoordRange1D(new RefCoord(0.5, 1.0), new RefCoord(3.5, 4.0)); // I Q U V
            coordAxis1D.function = new CoordFunction1D(4L, 1.0, new RefCoord(0.5, 1.0)); // I Q U V

            CoordBounds1D bounds = new CoordBounds1D();
            bounds.getSamples().add(new CoordRange1D(new RefCoord(0.5, -4.0), new RefCoord(3.5, -1.0))); // LR RL LL RR
            bounds.getSamples().add(new CoordRange1D(new RefCoord(3.5, 1.0), new RefCoord(7.5, 4.0))); // I Q U V
            coordAxis1D.bounds = bounds;
        }
        return new PolarizationWCS(coordAxis1D);
    }
    
    protected CustomWCS getCustomWCS() {
        Axis axis = new Axis("RM");
        axis.cunit = "rad/m**2";
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        if (complete)
        {
            coordAxis1D.error = new CoordError(1.0e-4, 1.0e-6);
            coordAxis1D.range = new CoordRange1D(new RefCoord(0.5, 1.0), new RefCoord(10.5, 4.0));
            coordAxis1D.bounds = new CoordBounds1D();
            coordAxis1D.bounds.getSamples().add(coordAxis1D.range);
            coordAxis1D.function = new CoordFunction1D(10L, 1.0, new RefCoord(0.5, 1.0));
        }
        return new CustomWCS(coordAxis1D);
    }
    
    protected Slice getSlice()
    {
        Axis axis = new Axis("sliceCtype", "sliceCunit");
        return new Slice(axis, 1L);
    }
    
    protected CoordAxis1D getCoordAxis1D(boolean nrg)
    {
        Axis axis;
        if (nrg)
            axis = new Axis("WAVE", "m");
        else
            axis = new Axis("TIME", "d");
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        if (complete)
        {
            coordAxis1D.error = new CoordError(1.0, 1.5);
            coordAxis1D.range = new CoordRange1D(new RefCoord(0.5, 100.0), new RefCoord(4.5, 140.0));
            coordAxis1D.bounds = new CoordBounds1D();
            coordAxis1D.bounds.getSamples().add(coordAxis1D.range);
            coordAxis1D.function = new CoordFunction1D(4L, 10.0, new RefCoord(0.5, 100.0)); // [100,140] in steps of 10u
        }
        return coordAxis1D;
    }
    
    protected CoordAxis2D getCoordAxis2D()
    {
        Axis axis1 = new Axis("RA", "deg");
        Axis axis2 = new Axis("DEC", "deg");
        
        CoordAxis2D coordAxis2D = new CoordAxis2D(axis1, axis2);
        if (complete)
        {
            coordAxis2D.error1 = new CoordError(1.0, 1.5);
            coordAxis2D.error2 = new CoordError(2.0, 2.5);

            // 20x20, center @ 11,12
            
            Coord2D start = new Coord2D(new RefCoord(1, 10.0), new RefCoord(1, 11.0));
            Coord2D end = new Coord2D(new RefCoord(20, 12.0), new RefCoord(40, 13.0));
            coordAxis2D.range = new CoordRange2D(start, end);
            
            Dimension2D dimension = new Dimension2D(20, 20);
            Coord2D refCoord = new Coord2D(new RefCoord(10, 11.0), new RefCoord(20, 12.0));
            coordAxis2D.function = new CoordFunction2D(dimension, refCoord, 0.05, 0.0, 0.0, 0.05);
            if (boundsIsCircle)
            {
                ValueCoord2D center = new ValueCoord2D(11.0, 12.0);
                Double radius = 0.5;
                CoordCircle2D circle = new CoordCircle2D(center, radius);
                coordAxis2D.bounds = circle;
            }
            else
            {
                // a smaller polygon inside the pixel array
                CoordPolygon2D polygon = new CoordPolygon2D();
                polygon.getVertices().add(new ValueCoord2D(10.2, 11.2));
                polygon.getVertices().add(new ValueCoord2D(11.8, 11.2));
                polygon.getVertices().add(new ValueCoord2D(11.8, 12.8));
                polygon.getVertices().add(new ValueCoord2D(10.2, 12.8));
                coordAxis2D.bounds = polygon;
            }
        }
        return coordAxis2D;
    }
    
}
