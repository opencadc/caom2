/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
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
            
            // TODO: assert something
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactListQuery()
    {
        try
        {
            PlaneURI uri = new PlaneURI(new URI("caom:FOO/bar123/bar.fits"));
            AdqlQueryGenerator gen = new AdqlQueryGenerator();
            String adql = gen.getADQL(uri, false);
            log.info("testArtifactListQuery:\n" + adql);
            
            // TODO: assert something
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
            
            // TODO: assert something
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
