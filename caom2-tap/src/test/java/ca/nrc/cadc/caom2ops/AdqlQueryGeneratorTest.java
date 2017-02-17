/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AdqlQueryGeneratorTest
{
    private static final Logger log = Logger.getLogger(AdqlQueryGeneratorTest.class);
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.INFO);
    }

    //@Test
    public void testTemplate()
    {
        try
        {

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactQuery()
    {
        try
        {
            URI uri = new URI("ad:FOO/bar.fits");
            AdqlQueryGenerator gen = new AdqlQueryGenerator();
            String adql = gen.getArtifactADQL(uri);
            log.info("testArtifactQuery:\n" + adql);
            
            adql = adql.toLowerCase();
            
            // TODO: assert something
            Assert.assertTrue(adql.contains("from caom2.artifact"));
            Assert.assertTrue(adql.contains("left outer join caom2.part"));
            Assert.assertTrue(adql.contains("left outer join caom2.chunk"));
            
            Assert.assertTrue(adql.contains("artifact.uri ="));
            
            Assert.assertTrue(adql.contains("order by"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactListQueryPlaneURI()
    {
        try
        {
            PlaneURI uri = new PlaneURI(new URI("caom:FOO/bar123/bar456"));
            AdqlQueryGenerator gen = new AdqlQueryGenerator();
            String adql = gen.getADQL(uri, false);
            log.info("testArtifactListQueryPlaneURI:\n" + adql);
            
            adql = adql.toLowerCase();
            
            // TODO: assert something
            Assert.assertTrue(adql.contains("from caom2.plane"));
            Assert.assertTrue(adql.contains("left outer join caom2.artifact"));
            Assert.assertTrue(adql.contains("left outer join caom2.part"));
            Assert.assertTrue(adql.contains("left outer join caom2.chunk"));
            
            Assert.assertTrue(adql.contains("plane.obsid"));
            Assert.assertTrue(adql.contains("isdownloadable(plane.obsid)"));
            
            Assert.assertTrue(adql.contains("plane.planeuri = "));
            
            Assert.assertTrue(adql.contains("order by"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactListQueryPublisherID()
    {
        try
        {
            PublisherID uri = new PublisherID(new URI("ivo://cadc.nrc.ca/FOO?bar123/bar456"));
            AdqlQueryGenerator gen = new AdqlQueryGenerator();
            String adql = gen.getADQL(uri, false);
            log.info("testArtifactListQueryPublisherID:\n" + adql);
            
            adql = adql.toLowerCase();
            
            // TODO: assert something
            Assert.assertTrue(adql.contains("from caom2.plane"));
            Assert.assertTrue(adql.contains("left outer join caom2.artifact"));
            Assert.assertTrue(adql.contains("left outer join caom2.part"));
            Assert.assertTrue(adql.contains("left outer join caom2.chunk"));
            
            Assert.assertTrue(adql.contains("plane.obsid as metareadable"));
            Assert.assertTrue(adql.contains("isdownloadable(plane.obsid) as datareadable"));
            
            Assert.assertTrue(adql.contains("plane.publisherid = "));
            
            Assert.assertTrue(adql.contains("order by"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testObservationQuery()
    {
        try
        {
            ObservationURI uri = new ObservationURI("FOO", "bar123");
            AdqlQueryGenerator gen = new AdqlQueryGenerator();
            String adql = gen.getADQL(uri);
            log.info("testObservationQuery:\n" + adql);
            
            adql = adql.toLowerCase();
            
            // TODO: assert something
            Assert.assertTrue(adql.contains("from caom2.observation"));
            Assert.assertTrue(adql.contains("left outer join caom2.plane"));
            Assert.assertTrue(adql.contains("left outer join caom2.artifact"));
            Assert.assertTrue(adql.contains("left outer join caom2.part"));
            Assert.assertTrue(adql.contains("left outer join caom2.chunk"));
            
            Assert.assertTrue(adql.contains("observation.collection = "));
            Assert.assertTrue(adql.contains("observation.observationid = "));
            
            Assert.assertTrue(adql.contains("order by"));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
