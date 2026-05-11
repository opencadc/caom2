
package ca.nrc.cadc.sia;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class SiaRequestTest
{
    private static final Logger log = Logger.getLogger(SiaRequestTest.class);

    public SiaRequestTest() { }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc.sia", Level.INFO);
    }

    @Test
    public void testPos()
    {
        log.debug("testPos");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals(67.54, request.pos[0], 0.0);
            assertEquals(46.9544, request.pos[1], 0.0);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSize()
    {
        log.debug("testSize");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals(1.0, request.size[0], 0.0);
            assertEquals(2.0, request.size[1], 0.0);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSizeEqualsOne()
    {
        log.debug("testSize");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "10.0"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals(10.0, request.size[0], 0.0);
            assertEquals(10.0, request.size[1], 0.0);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMaxRec()
    {
        log.debug("testMaxRec");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("MAXREC", "10"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);
            assertEquals(new Integer(10), request.maxRec);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNegativeMaxRec()
    {
        log.debug("testNegativeMaxRec");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("MAXREC", "-1"));
            Job job = new Job();
            job.setParameterList(parameters);
            SiaRequest request = new SiaRequest(job, 10.0);
            assertNull("maxRec should be null for negative MAXREC parameters", request.maxRec);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFormatMetadata()
    {
        log.debug("testFormatMetadata");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("FORMAT", "METADATA"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals("METADATA", request.format);
            assertTrue(request.isMetadataFormat);
            assertFalse(request.isAllFormat);
            assertFalse(request.isFitsFormat);
            assertFalse(request.isCustomFormat);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFormatFits()
    {
        log.debug("testFormat");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("FORMAT", "image/fits"));
            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);
            assertTrue(request.isFitsFormat);
            assertFalse(request.isAllFormat);
            assertFalse(request.isMetadataFormat);
            assertFalse(request.isCustomFormat);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFormatAll()
    {
        log.debug("testFormatAll");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("FORMAT", "ALL"));

            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals("ALL", request.format);
            assertTrue(request.isAllFormat);
            assertFalse(request.isFitsFormat);
            assertFalse(request.isMetadataFormat);
            assertFalse(request.isCustomFormat);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFormatGraphic()
    {
        log.debug("testFormatGraphic");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("FORMAT", "GRAPHIC"));

            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals("GRAPHIC", request.format);
            assertFalse(request.isFitsFormat);
            assertFalse(request.isAllFormat);
            assertFalse(request.isMetadataFormat);
            assertTrue(request.isCustomFormat);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFormatPNG()
    {
        log.debug("testFormatGraphic");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "67.54, 46.9544"));
            parameters.add(new Parameter("SIZE", "1.0, 2.0"));
            parameters.add(new Parameter("FORMAT", "image/png"));

            Job job = new Job();
            job.setParameterList(parameters);

            SiaRequest request = new SiaRequest(job, 10.0);

            assertEquals("image/png", request.format);
            assertFalse(request.isFitsFormat);
            assertFalse(request.isAllFormat);
            assertFalse(request.isMetadataFormat);
            assertTrue(request.isCustomFormat);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testRequestPath()
    {
        log.debug("testRequestPath");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "1,2"));

            SiaRequest request;
            Job job = new Job();
            job.setParameterList(parameters);

            job.setRequestPath("/sia/query");
            request = new SiaRequest(job, 10.0);

            assertNull(request.collection);

            job.setRequestPath("/sia/CFHT/query");
            request = new SiaRequest(job, 10.0);

            assertEquals("CFHT", request.collection);

            job.setRequestPath("/sia/ALL/query");
            request = new SiaRequest(job, 10.0);

            assertEquals("ALL", request.collection);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }

    }

    @Test
    public void testCollectionParam()
    {
        log.debug("testCollectionParam");
        try
        {
            String[] collections = { "CFHT", "ALL" };

            for (String col : collections)
            {
                List<Parameter> parameters = new ArrayList<Parameter>();
                parameters.add(new Parameter("POS", "1,2"));
                parameters.add(new Parameter("collection", col));

                SiaRequest request;
                Job job = new Job();
                job.setParameterList(parameters);

                job.setRequestPath("/sia/query");
                request = new SiaRequest(job, 10.0);

                assertEquals(col, request.collection);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUnknownCollection() throws Exception
    {
        log.debug("testUnknownCollection");
        try
        {
            String[] collections = { "", "FOO" };

            for (String col : collections)
            {
                List<Parameter> parameters = new ArrayList<Parameter>();
                parameters.add(new Parameter("POS", "1,2"));
                parameters.add(new Parameter("collection", col));

                SiaRequest request;
                Job job = new Job();
                job.setParameterList(parameters);

                job.setRequestPath("/sia/query");
                request = new SiaRequest(job, 10.0);

                assertNull(request.collection);
                assertEquals(new Integer(0), request.maxRec);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testModeParam() throws Exception
    {
        log.debug("testModeParam");
        try
        {
            List<Parameter> parameters = new ArrayList<Parameter>();
            parameters.add(new Parameter("POS", "1,2"));
            parameters.add(new Parameter("mode", "cutout"));

            SiaRequest request;
            Job job = new Job();
            job.setParameterList(parameters);

            job.setRequestPath("/sia/query");
            request = new SiaRequest(job, 10.0);

            assertEquals("cutout", request.mode);

            // archive mode
            parameters.clear();
            parameters.add(new Parameter("POS", "1,2"));
            parameters.add(new Parameter("mode", "archive"));

            job.setParameterList(parameters);
            request = new SiaRequest(job, 10.0);

            assertEquals("archive", request.mode);

            // unknown mode
            parameters.clear();
            parameters.add(new Parameter("POS", "1,2"));
            parameters.add(new Parameter("mode", "foo"));

            job.setParameterList(parameters);
            request = new SiaRequest(job, 10.0);

            assertNull(request.mode);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}