/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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
package ca.nrc.cadc.caom2.fits;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.TargetType;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class FitsMapperTest
{
    private static Logger log = Logger.getLogger(FitsValuesMap.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }
    
    static FitsMapping simpleMapping;
    static FitsMapping compositeMapping;
    
    public FitsMapperTest() { }

    @BeforeClass
    public static void setUpClass()
        throws Exception
    {
        File df = new File("test/config/fits2caom2/fits2caom2-simple.default");
        File of = new File("test/config/fits2caom2/fits2caom2.override");
        
        Map<String,String> config = Util.loadConfig("config/fits2caom2.config");
        FitsValuesMap defaults = new FitsValuesMap(new FileReader(df), "default");
        FitsValuesMap override = new FitsValuesMap(new FileReader(of), "override");
        
        simpleMapping = new FitsMapping(config, defaults, override);
        simpleMapping.setArgumentProperty("Observation.collection", "theCollection");
        simpleMapping.setArgumentProperty("Observation.observationID", "theObservationID");
        simpleMapping.setArgumentProperty("Plane.productID", "theProductID");
        simpleMapping.positionAxis1 = 1;
        simpleMapping.positionAxis2 = 2;
        simpleMapping.energyAxis = 3;
        simpleMapping.polarizationAxis = 4;
        simpleMapping.timeAxis = 5;
        
        File cdf = new File("test/config/fits2caom2/fits2caom2-composite.default");
        FitsValuesMap compositeDefaults = new FitsValuesMap(new FileReader(cdf), "default");
        
        compositeMapping = new FitsMapping(config, compositeDefaults, override);
        compositeMapping.setArgumentProperty("Observation.collection", "theCollection");
        compositeMapping.setArgumentProperty("Observation.observationID", "theObservationID");
        compositeMapping.setArgumentProperty("Plane.productID", "theProductID");
    }
    
    @Test
    public void testGetPublicFields()
    {
        Class[] classes = new Class[]
        {
            CompositeObservation.class,
            SimpleObservation.class,
            Observation.class,
            Proposal.class,
            Algorithm.class,
            Target.class,
            TargetPosition.class,
            Telescope.class,
            Instrument.class,
            Plane.class,
            Metrics.class,
            Part.class,
            Artifact.class,
            Chunk.class
        };
        
        int[] members = new int[]
        {
            0, 0, 11, 3, 0, 4, 1, 3, 0, 7, 5, 1, 4, 13
        };
        
        FitsMapper mapper = new FitsMapper(simpleMapping);
        for (int i = 0; i < classes.length; i++)
        {
            List<Field> fields = mapper.getPublicFields(classes[i]);
            log.debug(classes[i].getSimpleName() + " public fields:");
            for (Field field : fields)
                log.debug("\t" + field.getName());
            Assert.assertEquals(members[i], fields.size());
        }
    }
    
    @Test
    public void testGetPrivateFieldsWithGetter()
    {
        Class[] classes = new Class[]
        {
            Target.class,
            TargetPosition.class,
            Telescope.class,
            Instrument.class,
            Part.class,
            Polygon.class,
            CoordBounds1D.class,
            CoordPolygon2D.class
        };
        
        int[] members = new int[]
        {
            2, 2, 2, 2, 2, 1, 1, 1
        };
        
        FitsMapper mapper = new FitsMapper(simpleMapping);
        for (int i = 0; i < classes.length; i++)
        {
            List<Field> fields = mapper.getPrivateFieldsWithGetter(classes[i]);
            log.debug(classes[i].getSimpleName() + " private fields with getter:");
            for (Field field : fields)
                log.debug("\t" + field.getName());
            Assert.assertEquals(members[i], fields.size());
        }
    }
    
    @Test
    public void testGetPublicGetCollectionMethods()
        throws Exception
    {
        Class[] classes = new Class[]
        {
            Observation.class,
            CompositeObservation.class,
            Proposal.class,
            Target.class,
            Telescope.class,
            Instrument.class,
            Plane.class,
            Artifact.class,
            Provenance.class
        };
        
        int[] members = new int[]
        {
            0, 1, 1, 1, 1, 1, 0, 0, 2
        };
        
        FitsMapper mapper = new FitsMapper(simpleMapping);
        for (int i = 0; i < classes.length; i++)
        {
            Map<String, Method> methods = mapper.getPublicGetCollectionMethods(classes[i]);
            log.debug(classes[i].getSimpleName() + " public get collection methods:");
            Set<Map.Entry<String, Method>> mapEntry = methods.entrySet();
            for (Map.Entry entry : mapEntry)
                log.debug("\t" + entry.getKey());
            Assert.assertEquals(members[i], methods.size());
        }
    }
    
    @Test
    public void testGetPublicSetMethods()
        throws Exception
    {
        Class[] classes = new Class[]
        {
            Observation.class,
            SimpleObservation.class,
            CompositeObservation.class
        };
        
        int[] members = new int[]
        {
            1, 1, 1
        };
        
        FitsMapper mapper = new FitsMapper(simpleMapping);
        for (int i = 0; i < classes.length; i++)
        {
            Map<String, Method> methods = mapper.getPublicSetMethods(classes[i]);
            log.debug(classes[i].getSimpleName() + " public set methods:");
            Set<Map.Entry<String, Method>> mapEntry = methods.entrySet();
            for (Map.Entry entry : mapEntry)
                log.debug("\t" + entry.getKey());
            Assert.assertEquals(members[i], methods.size());
        }
    }
    
    @Test
    public void testPrimativeValues()
        throws Exception
    {
        FitsMapper mapper = new FitsMapper(simpleMapping);
        
        Integer naxis = (Integer) mapper.populate(Integer.class, null, "Chunk.naxis");
        Assert.assertNotNull(naxis);
        Assert.assertEquals(new Integer(1), naxis);
        
        long bin = (Long) mapper.populate(long.class, null, "Chunk.observable.dependent.bin");
        Assert.assertNotNull(bin);
        Assert.assertEquals(1, bin);
        
        Double redshift = (Double) mapper.populate(Double.class, null, "Observation.target.redshift");
        Assert.assertNotNull(redshift);
        Assert.assertEquals(1.0, redshift);
    }
    
    @Test
    public void testInvokePublicGetCollectionMethods()
        throws Exception
    {
        // List of String.
        Instrument instrument = new Instrument("name");
        
        FitsMapper mapper = new FitsMapper(simpleMapping);
        mapper.invokePublicGetCollectionMethods(Instrument.class, instrument, "Observation.instrument", simpleMapping);
        
        Assert.assertNotNull(instrument);
        Assert.assertNotNull(instrument.getKeywords());
        Assert.assertEquals("name", instrument.getName());
        
        List<String> keywords = instrument.getKeywords();
        
        Assert.assertEquals(3, keywords.size());
        Assert.assertEquals("the", keywords.get(0));
        Assert.assertEquals("instrument", keywords.get(1));
        Assert.assertEquals("keywords", keywords.get(2));
        
        // Set of PlaneURI.
        Provenance provenance = new Provenance("name");
        mapper.invokePublicGetCollectionMethods(Provenance.class, provenance, "Plane.provenance", simpleMapping);
        
        Assert.assertNotNull(provenance);
        Assert.assertNotNull(provenance.getKeywords());
        Assert.assertNotNull(provenance.getInputs());
        Assert.assertEquals("name", provenance.getName());
        
        keywords = provenance.getKeywords();
        
        Assert.assertEquals(3, keywords.size());
        Assert.assertEquals("the", keywords.get(0));
        Assert.assertEquals("provenance", keywords.get(1));
        Assert.assertEquals("keywords", keywords.get(2));
        
        Set<PlaneURI> inputs = provenance.getInputs();
        
        Assert.assertEquals(2, inputs.size());
        PlaneURI[] uris = (PlaneURI[]) inputs.toArray(new PlaneURI[2]);
        Assert.assertEquals("caom:foo/bar/plane1", uris[0].getURI().toString());
        Assert.assertEquals("caom:foo/bar/plane2", uris[1].getURI().toString());
    }
    
    @Test
    public void testInvokePublicSetMethods()
        throws Exception
    {
        Algorithm algorithm = new Algorithm("algo name");
        CompositeObservation composite = new CompositeObservation("collection", "obsID", algorithm);
        
        Assert.assertEquals("algo name", composite.getAlgorithm().getName());
        
        FitsMapper mapper = new FitsMapper(compositeMapping);
        mapper.invokePublicSetMethods(CompositeObservation.class, composite, "Observation", compositeMapping);
        
        Assert.assertNotNull(composite);
        Assert.assertNotNull(composite.getAlgorithm());
        Assert.assertNotNull(composite.getAlgorithm().getName());
        Assert.assertEquals("algorithm name", composite.getAlgorithm().getName());
    }
    
    @Test
    public void testIncompleteObservation() throws Exception
    {
        File df = new File("test/config/fits2caom2/FitsMapperTest.default");
        
        Map<String,String> config = Util.loadConfig("config/fits2caom2.config");
        FitsValuesMap defaults = new FitsValuesMap(new FileReader(df), "default");
        
        FitsMapping mapping = new FitsMapping(config, defaults, null);
        mapping.setArgumentProperty("Observation.collection", "theCollection");
        mapping.setArgumentProperty("Observation.observationID", "theObservationID");
        mapping.setArgumentProperty("Plane.productID", "theProductID");
        
        FitsMapper mapper = new FitsMapper(mapping);
        SimpleObservation observation = new SimpleObservation("theCollection", "theObservationID");
        mapper.populate(Observation.class, observation, "Observation");
        Assert.assertNotNull(observation);
        
        Assert.assertEquals("theCollection", observation.getCollection());
        Assert.assertEquals("theObservationID", observation.getObservationID());
        Assert.assertEquals("Sun Feb 26 09:15:40 PST 2012", observation.metaRelease.toString());
        Assert.assertNull(observation.type);
        Assert.assertNull(observation.intent);
        
        Assert.assertNotNull(observation.instrument);
        Assert.assertEquals("instrument name", observation.instrument.getName());
        List<String> instrumentKeywords = observation.instrument.getKeywords();
        Assert.assertTrue(instrumentKeywords.isEmpty());
        
        Assert.assertNotNull(observation.proposal);
        Assert.assertEquals("proposal id", observation.proposal.getID());
        Assert.assertNull(observation.proposal.pi);
        Assert.assertNull(observation.proposal.project);
        Assert.assertNull(observation.proposal.title);
        List<String> proposalKeywords = observation.proposal.getKeywords();
        Assert.assertEquals("the", proposalKeywords.get(0));
        Assert.assertEquals("proposal", proposalKeywords.get(1));
        Assert.assertEquals("keywords", proposalKeywords.get(2));
        
        Assert.assertNotNull(observation.target);
        Assert.assertEquals("target name", observation.target.getName());
        Assert.assertEquals(TargetType.OBJECT, observation.target.type);
        Assert.assertNull(observation.target.redshift);
        List<String> targetKeywords = observation.target.getKeywords();
        Assert.assertEquals("the", targetKeywords.get(0));
        Assert.assertEquals("target", targetKeywords.get(1));
        Assert.assertEquals("keywords", targetKeywords.get(2));
        Assert.assertNull(observation.target.standard);
        
        Assert.assertNotNull(observation.telescope);
        Assert.assertEquals("telescope name", observation.telescope.getName());
        Assert.assertEquals(1.0, observation.telescope.geoLocationX);
        Assert.assertEquals(2.0, observation.telescope.geoLocationY);
        Assert.assertEquals(3.0, observation.telescope.geoLocationZ);
        List<String> telescopeKeywords = observation.telescope.getKeywords();
        Assert.assertTrue(telescopeKeywords.isEmpty());
        
        Assert.assertNull(observation.environment);
    }
    
    @Test
    public void testPopulateCompositeObservation()
        throws Exception
    {
        FitsMapper mapper = new FitsMapper(compositeMapping);
        Algorithm algorithm = new Algorithm("algorithm name");
        CompositeObservation observation = new CompositeObservation("theCollection", "theObservationID", algorithm);
        mapper.populate(CompositeObservation.class, observation, "CompositeObservation");
        Assert.assertNotNull(observation);
        
        Assert.assertEquals("theCollection", observation.getCollection());
        Assert.assertEquals("theObservationID", observation.getObservationID());
        Assert.assertEquals("algorithm name", observation.getAlgorithm().getName());
        
        ObservationURI[] members = (ObservationURI[]) observation.getMembers().toArray(new ObservationURI[2]);
        Assert.assertEquals(new ObservationURI(new URI("caom:collection1/observationID1")), members[0]);
        Assert.assertEquals(new ObservationURI(new URI("caom:collection2/observationID2")), members[1]);
    }
    
    @Test
    public void testPopulateObservation()
        throws Exception
    {
        FitsMapper mapper = new FitsMapper(simpleMapping);
        SimpleObservation observation = new SimpleObservation("theCollection", "theObservationID");
        mapper.populate(Observation.class, observation, "Observation");
        Assert.assertNotNull(observation);
        
        Assert.assertEquals("theCollection", observation.getCollection());
        Assert.assertEquals("theObservationID", observation.getObservationID());
        Assert.assertEquals("Sun Feb 26 09:15:40 PST 2012", observation.metaRelease.toString());
        Assert.assertEquals("dark", observation.type);
        Assert.assertEquals(ObservationIntentType.CALIBRATION.getValue(), observation.intent.getValue());
        
        Assert.assertNotNull(observation.instrument);
        Assert.assertEquals("instrument name", observation.instrument.getName());
        List<String> instrumentKeywords = observation.instrument.getKeywords();
        Assert.assertEquals("the", instrumentKeywords.get(0));
        Assert.assertEquals("instrument", instrumentKeywords.get(1));
        Assert.assertEquals("keywords", instrumentKeywords.get(2));
        
        Assert.assertNotNull(observation.proposal);
        Assert.assertEquals("proposal id", observation.proposal.getID());
        Assert.assertEquals("proposal pi", observation.proposal.pi);
        Assert.assertEquals("proposal project", observation.proposal.project);
        Assert.assertEquals("proposal title", observation.proposal.title);
        List<String> proposalKeywords = observation.proposal.getKeywords();
        Assert.assertEquals("the", proposalKeywords.get(0));
        Assert.assertEquals("proposal", proposalKeywords.get(1));
        Assert.assertEquals("keywords", proposalKeywords.get(2));
        Assert.assertTrue(observation.target.standard);
        
        Assert.assertNotNull(observation.target);
        Assert.assertEquals("target name", observation.target.getName());
        Assert.assertEquals(TargetType.OBJECT, observation.target.type);
        Assert.assertEquals(1.0, observation.target.redshift);
        List<String> targetKeywords = observation.target.getKeywords();
        Assert.assertEquals("the", targetKeywords.get(0));
        Assert.assertEquals("target", targetKeywords.get(1));
        Assert.assertEquals("keywords", targetKeywords.get(2));
        
        Assert.assertNotNull(observation.telescope);
        Assert.assertEquals("telescope name", observation.telescope.getName());
        Assert.assertEquals(1.0, observation.telescope.geoLocationX);
        Assert.assertEquals(2.0, observation.telescope.geoLocationY);
        Assert.assertEquals(3.0, observation.telescope.geoLocationZ);
        List<String> telescopeKeywords = observation.telescope.getKeywords();
        Assert.assertEquals("the", telescopeKeywords.get(0));
        Assert.assertEquals("telescope", telescopeKeywords.get(1));
        Assert.assertEquals("keywords", telescopeKeywords.get(2));
        
        Assert.assertNotNull(observation.environment);
        Assert.assertEquals(observation.environment.seeing, 1.0, 0.0);
        Assert.assertEquals(observation.environment.humidity, 2.0, 0.0);
        Assert.assertEquals(observation.environment.elevation, 3.0, 0.0);
        Assert.assertEquals(observation.environment.tau, 4.0, 0.0);
        Assert.assertEquals(observation.environment.wavelengthTau, 5.0, 0.0);
        Assert.assertEquals(observation.environment.ambientTemp, 6.0, 0.0);
        Assert.assertTrue(observation.environment.photometric);
    }

    @Test
    public void testPopulatePlane()
        throws Exception
    {
        try
        {
            FitsMapper mapper = new FitsMapper(simpleMapping);
            Plane plane = new Plane("theProductID");
            mapper.populate(Plane.class, plane, "Plane");
            Assert.assertNotNull(plane);

            Assert.assertEquals("theProductID", plane.getProductID());
            Assert.assertEquals("Sun Feb 26 09:15:40 PST 2012", plane.metaRelease.toString());
            Assert.assertEquals("Sun Feb 26 09:15:40 PST 2012", plane.dataRelease.toString());
            Assert.assertEquals(DataProductType.IMAGE.getValue(), plane.dataProductType.getValue());
            Assert.assertEquals(CalibrationLevel.PRODUCT, plane.calibrationLevel);

            Assert.assertNotNull(plane.provenance);
            Assert.assertEquals("provenance name", plane.provenance.getName());
            Assert.assertEquals("provenance version", plane.provenance.version);
            Assert.assertEquals("provenance project", plane.provenance.project);
            Assert.assertEquals("provenance producer", plane.provenance.producer);
            Assert.assertEquals("provenance runID", plane.provenance.runID);
            Assert.assertEquals("http://localhost/provenance/reference", plane.provenance.reference.toString());
            Assert.assertEquals("Sun Feb 26 09:15:40 PST 2012", plane.provenance.lastExecuted.toString());

            Assert.assertNotNull(plane.metrics);
            Assert.assertEquals(plane.metrics.sourceNumberDensity, 1.0, 0.0);
            Assert.assertEquals(plane.metrics.background, 2.0, 0.0);
            Assert.assertEquals(plane.metrics.backgroundStddev, 3.0, 0.0);
            Assert.assertEquals(plane.metrics.fluxDensityLimit, 4.0, 0.0);
            Assert.assertEquals(plane.metrics.magLimit, 5.0, 0.0);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPopulateArtifact()
        throws Exception
    {
        FitsMapper mapper = new FitsMapper(simpleMapping);
        Artifact artifact = new Artifact(new URI("ad:archive/fileID"));
        mapper.populate(Artifact.class, artifact, "Artifact");
        Assert.assertNotNull(artifact);
        
        // these are not configured and dopn't get set by mapping
        Assert.assertNull("artifact contentType", artifact.contentType);
        Assert.assertNull("artifact.contentLength", artifact.contentLength);
        
        Assert.assertEquals(ProductType.SCIENCE, artifact.productType);
        Assert.assertEquals(true, artifact.alternative);
    }

    @Test
    public void testPopulatePart()
        throws Exception
    {   
        // Test partNumber constructor.
        FitsMapper mapper = new FitsMapper(simpleMapping);
        Part part = new Part("partName");
        mapper.populate(Part.class, part, "Part");
        Assert.assertNotNull(part);

        Assert.assertEquals("partName", part.getName());
        Assert.assertEquals(ProductType.SCIENCE, part.productType);
    }
        
    //@Test // obsolete
    public void testPopulateChunk()
        throws Exception
    {
        FitsMapper mapper = new FitsMapper(simpleMapping);
        Chunk chunk = new Chunk();
        mapper.populate(Chunk.class, chunk, "Chunk");
        
        Assert.assertNotNull(chunk);
        Assert.assertEquals(ProductType.SCIENCE, chunk.productType);
        Assert.assertEquals(1, chunk.naxis.longValue());
        Assert.assertEquals(2, chunk.observableAxis.longValue());System.out.println("p1 " + chunk.positionAxis1);
        
        // Assigned in Ingest, thus null here.
        Assert.assertNull(chunk.positionAxis1);
        Assert.assertNull(chunk.positionAxis2);
        Assert.assertNull(chunk.energyAxis);
        Assert.assertNull(chunk.polarizationAxis);
        Assert.assertNull(chunk.timeAxis);
        
        Assert.assertNotNull(chunk.observable);
        Assert.assertEquals("chunk observable dependent ctype", chunk.observable.getDependent().getAxis().getCtype());
        Assert.assertEquals("chunk observable dependent cunit", chunk.observable.getDependent().getAxis().getCunit());
        Assert.assertEquals(Long.valueOf("1"), new Long(chunk.observable.getDependent().getBin()));
        Assert.assertEquals("chunk observable independent ctype", chunk.observable.independent.getAxis().getCtype());
        Assert.assertEquals("chunk observable independent cunit", chunk.observable.independent.getAxis().getCunit());
        Assert.assertEquals(Long.valueOf("2"), new Long(chunk.observable.independent.getBin()));
        
        Assert.assertNotNull(chunk.position);
        Assert.assertEquals("position coordsys", chunk.position.coordsys);
        Assert.assertEquals(1.0, chunk.position.equinox);
        Assert.assertEquals(2.0, chunk.position.resolution);
        Assert.assertEquals("RA", chunk.position.getAxis().getAxis1().getCtype());
        Assert.assertEquals("DEC", chunk.position.getAxis().getAxis1().getCunit());
        Assert.assertEquals("GLON", chunk.position.getAxis().getAxis2().getCtype());
        Assert.assertEquals("GLAT", chunk.position.getAxis().getAxis2().getCunit());
//      Throws a unsupported operation exception.
//        Assert.assertEquals(3.0, chunk.position.getAxis().getDelta().delta1);
//        Assert.assertEquals(4.0, chunk.position.getAxis().getDelta().delta2);
        Assert.assertEquals(5.0, chunk.position.getAxis().error1.syser);
        Assert.assertEquals(6.0, chunk.position.getAxis().error1.rnder);
        Assert.assertEquals(7.0, chunk.position.getAxis().error2.syser);
        Assert.assertEquals(8.0, chunk.position.getAxis().error2.rnder);
        Assert.assertEquals(9.0, chunk.position.getAxis().function.getCd11());
        Assert.assertEquals(10.0, chunk.position.getAxis().function.getCd12());
        Assert.assertEquals(11.0, chunk.position.getAxis().function.getCd21());
        Assert.assertEquals(12.0, chunk.position.getAxis().function.getCd22());
        Assert.assertEquals(13, chunk.position.getAxis().function.getDimension().naxis1);
        Assert.assertEquals(14, chunk.position.getAxis().function.getDimension().naxis2);
        Assert.assertEquals(15.0, chunk.position.getAxis().function.getRefCoord().getCoord1().pix);
        Assert.assertEquals(16.0, chunk.position.getAxis().function.getRefCoord().getCoord1().val);
        Assert.assertEquals(17.0, chunk.position.getAxis().function.getRefCoord().getCoord2().pix);
        Assert.assertEquals(18.0, chunk.position.getAxis().function.getRefCoord().getCoord2().val);
        Assert.assertEquals(19.0, chunk.position.getAxis().range.getStart().getCoord1().pix);
        Assert.assertEquals(20.0, chunk.position.getAxis().range.getStart().getCoord1().val);
        Assert.assertEquals(21.0, chunk.position.getAxis().range.getStart().getCoord2().pix);
        Assert.assertEquals(22.0, chunk.position.getAxis().range.getStart().getCoord2().val);
        Assert.assertEquals(23.0, chunk.position.getAxis().range.getEnd().getCoord1().pix);
        Assert.assertEquals(24.0, chunk.position.getAxis().range.getEnd().getCoord1().val);
        Assert.assertEquals(25.0, chunk.position.getAxis().range.getEnd().getCoord2().pix);
        Assert.assertEquals(26.0, chunk.position.getAxis().range.getEnd().getCoord2().val);
        
        Assert.assertNotNull(chunk.energy);
        Assert.assertEquals("energy specsys", chunk.energy.getSpecsys());
        Assert.assertEquals("energy ssysobs", chunk.energy.ssysobs);
        Assert.assertEquals(1.0, chunk.energy.restfrq);
        Assert.assertEquals(2.0, chunk.energy.restwav);
        Assert.assertEquals(3.0, chunk.energy.velosys);
        Assert.assertEquals(4.0, chunk.energy.zsource);
        Assert.assertEquals("energy ssyssrc", chunk.energy.ssyssrc);
        Assert.assertEquals(5.0, chunk.energy.velang);
        Assert.assertEquals("energy bandpassName", chunk.energy.bandpassName);
        Assert.assertEquals(6.0, chunk.energy.resolvingPower);
        Assert.assertEquals("WAVE", chunk.energy.getAxis().getAxis().getCtype());
        Assert.assertEquals("energy axis axis cunit", chunk.energy.getAxis().getAxis().getCunit());
//        Assert.assertNull(chunk.energy.getAxis().bounds.getSamples());        
        Assert.assertEquals(7.0, chunk.energy.getAxis().error.syser);
        Assert.assertEquals(8.0, chunk.energy.getAxis().error.rnder);
        Assert.assertEquals(9, chunk.energy.getAxis().function.getNaxis().longValue());
        Assert.assertEquals(10.0, chunk.energy.getAxis().function.getDelta());
        Assert.assertEquals(11.0, chunk.energy.getAxis().function.getRefCoord().pix);
        Assert.assertEquals(12.0, chunk.energy.getAxis().function.getRefCoord().val);
        Assert.assertEquals(13.0, chunk.energy.getAxis().range.getStart().pix);
        Assert.assertEquals(14.0, chunk.energy.getAxis().range.getStart().val);
        Assert.assertEquals(15.0, chunk.energy.getAxis().range.getEnd().pix);
        Assert.assertEquals(16.0, chunk.energy.getAxis().range.getEnd().val);
        Assert.assertEquals("species", chunk.energy.transition.getSpecies());
        Assert.assertEquals("transition", chunk.energy.transition.getTransition());

        Assert.assertNotNull(chunk.polarization);
        Assert.assertEquals("STOKES", chunk.polarization.getAxis().getAxis().getCtype());
        Assert.assertEquals("polarization axis cunit", chunk.polarization.getAxis().getAxis().getCunit());
//        Assert.assertEquals("", chunk.polarization.getAxis().bounds.getSamples());
        Assert.assertEquals(1.0, chunk.polarization.getAxis().error.syser);
        Assert.assertEquals(2.0, chunk.polarization.getAxis().error.rnder);
        Assert.assertEquals(3, chunk.polarization.getAxis().function.getNaxis().longValue());
        Assert.assertEquals(4.0, chunk.polarization.getAxis().function.getDelta());
        Assert.assertEquals(5.0, chunk.polarization.getAxis().function.getRefCoord().pix);
        Assert.assertEquals(6.0, chunk.polarization.getAxis().function.getRefCoord().val);
        Assert.assertEquals(7.0, chunk.polarization.getAxis().range.getStart().pix);
        Assert.assertEquals(8.0, chunk.polarization.getAxis().range.getStart().val);
        Assert.assertEquals(9.0, chunk.polarization.getAxis().range.getEnd().pix);
        Assert.assertEquals(10.0, chunk.polarization.getAxis().range.getEnd().val);

        Assert.assertNotNull(chunk.time);
        Assert.assertEquals(1.0, chunk.time.exposure);
        Assert.assertEquals(2.0, chunk.time.resolution);
        Assert.assertEquals("TIME", chunk.time.getAxis().getAxis().getCtype());
        Assert.assertEquals("time axis cunit", chunk.time.getAxis().getAxis().getCunit());
//        Assert.assertNull(chunk.time.getAxis().bounds.getSamples());
        Assert.assertEquals(3.0, chunk.time.getAxis().error.syser);
        Assert.assertEquals(4.0, chunk.time.getAxis().error.rnder);
        Assert.assertEquals(5, chunk.time.getAxis().function.getNaxis().longValue());
        Assert.assertEquals(6.0, chunk.time.getAxis().function.getDelta());
        Assert.assertEquals(7.0, chunk.time.getAxis().function.getRefCoord().pix);
        Assert.assertEquals(8.0, chunk.time.getAxis().function.getRefCoord().val);
        Assert.assertEquals(9.0, chunk.time.getAxis().range.getStart().pix);
        Assert.assertEquals(10.0, chunk.time.getAxis().range.getStart().val);
        Assert.assertEquals(11.0, chunk.time.getAxis().range.getEnd().pix);
        Assert.assertEquals(12.0, chunk.time.getAxis().range.getEnd().val);
    }

}
