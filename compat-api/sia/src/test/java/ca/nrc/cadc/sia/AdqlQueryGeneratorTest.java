/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.sia;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class AdqlQueryGeneratorTest
{
    private static Logger log = Logger.getLogger(AdqlQueryGeneratorTest.class);

    public static String JOB_ID = "123";
    public static String MAXREC = "10";
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.sia", Level.INFO);
    }
    
    public AdqlQueryGeneratorTest() { }

    private String decode(String s)
    {
        // URLEncode the query string.
        try
        {
            return URLDecoder.decode(s, "UTF-8");

        }
        catch (UnsupportedEncodingException impossible)
        {
            throw new RuntimeException("BUG", impossible);
        }
    }

    // simulate a web app server
    private Map<String,List<String>> paramsToMap(String paramStr)
    {
        Map<String,List<String>> ret = new HashMap<String,List<String>>();
        String[] params = paramStr.split("&");
        for (int i = 0; i < params.length; i++)
        {
            String parameter = params[i];
            String[] param = parameter.split("=");
            String name = param[0];
            String value = null;
            if (param.length > 1)
                value = param[1];

            // URL-decode the value to simulate a webapp container
            if (value != null)
                value = decode(value);
            
            List<String> vals = ret.get(name);
            if (vals == null)
            {
                vals = new ArrayList<String>();
                ret.put(name,vals);
            }
            vals.add(value);
        }
        return ret;
    }

    private void assertSingleValueEquals(Map<String, Object> parameters, String name, String expected)
    {
        String value = (String) parameters.get(name);
        Assert.assertEquals("value of " + name, expected, value);
    }
    private Job createJob()
    {
        return new Job()
        {
            @Override
            public String getID() { return JOB_ID; }
        };
    }
    
    @Test
    public void testPosToContainsPoint()
    {
        log.debug("testPosToContainsPoint");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));

            Job job = createJob();
            job.setParameterList(list);
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where CONTAINS(POINT('ICRS',1.0,2.0), position_bounds) = 1

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            assertTrue(query.contains("CONTAINS(POINT('ICRS',1.0,2.0), position_bounds) = 1"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testPosSizeToBox()
    {
        log.debug("testPosSizeToBox");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));

            Job job = createJob();
            job.setParameterList(list);
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1";

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testFormatGraphic()
    {
        log.debug("testFormatGraphic");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0,2.0"));
            list.add(new Parameter("SIZE", "4.0,3.0"));
            list.add(new Parameter("FORMAT", "GRAPHIC"));
            Job job = createJob();
            job.setParameterList(list);
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,4.0,3.0)) = 1";
            
            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,3.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));

            // this is the relevant hack to not find anything
            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch(Exception ex)
        {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }

    @Test
    public void testFormatFits()
    {
        log.debug("testFormatFits");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0,2.0"));
            list.add(new Parameter("SIZE", "3.0,4.0"));
            list.add(new Parameter("FORMAT", "image/fits"));
            Job job = createJob();
            job.setParameterList(list);
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1
            // and imageFormat = 'image/fits'

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));
            assertTrue(query.contains("imageFormat in ('image/fits', 'application/fits')"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCollectionPath()
    {
        log.debug("testCollectionPath");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            Job job = createJob();
            job.setParameterList(list);
            job.setRequestPath("/sia/CFHT/query");
            SiaRequest siaRequest = new SiaRequest(job, 5.0);
            
            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1
            // and collection = 'BLAST'

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));
            assertTrue(query.contains("collection = 'CFHT'"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);

            // test collection=ALL
            job.setRequestPath("/sia/ALL/query");
            list.clear();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            siaRequest = new SiaRequest(job, 5.0);
            instance = new AdqlQueryGenerator(siaRequest);
            parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            query = (String) parameters.get("QUERY");
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));
            assertTrue(!query.contains("collection = '"));

            maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCollectionParam()
    {
        log.debug("testCollectionParam");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            list.add(new Parameter("collection", "CFHT"));
            Job job = createJob();
            job.setParameterList(list);
            job.setRequestPath("/sia/query");
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX"
            //           + "('ICRS',1.0,2.0,3.0,4.0)) = 1 and collection = 'BLAST'

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));
            assertTrue(query.contains("collection = 'CFHT'"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);

            // test collection=ALL
            list.clear();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            list.add(new Parameter("collection", "ALL"));
            siaRequest = new SiaRequest(job, 5.0);
            instance = new AdqlQueryGenerator(siaRequest);
            parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            query = (String) parameters.get("QUERY");
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));
            assertTrue(!query.contains("collection = '"));

            maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    //@Test
    public void testModeArchive()
    {
        log.debug("testModeArchive");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            list.add(new Parameter("mode", "archive"));
            Job job = createJob();
            job.setParameterList(list);
            job.setRequestPath("/sia/query");
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX"
            //             + "('ICRS',1.0,2.0,3.0,4.0)) = 1

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("accessURL"));
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    //@Test
    public void testModeCutout()
    {
        log.debug("testModeCutout");
        try
        {
            List<Parameter> list = new ArrayList<Parameter>();
            list.add(new Parameter("POS", "1.0, 2.0"));
            list.add(new Parameter("SIZE", "3.0, 4.0"));
            list.add(new Parameter("mode", "cutout"));
            Job job = createJob();
            job.setParameterList(list);
            job.setRequestPath("/sia/query");
            SiaRequest siaRequest = new SiaRequest(job, 5.0);

            // select * from caom.SIAv1 where INTERSECTS(position_bounds, BOX"
            //             + "('ICRS',1.0,2.0,3.0,4.0)) = 1

            AdqlQueryGenerator instance = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = instance.getParameterMap();
            assertTrue(parameters.size() > 0);

            assertSingleValueEquals(parameters, "FORMAT", AdqlQueryGenerator.SIA_CONTENT_TYPE);
            assertSingleValueEquals(parameters, "REQUEST", "doQuery");
            assertSingleValueEquals(parameters, "LANG", "ADQL");

            String query = (String) parameters.get("QUERY");
            log.debug("QUERY: " + query);
            assertNotNull(query);
            assertTrue(query.contains("caom2.SIAv1"));
            //assertTrue(query.contains("INTERSECTS(position_bounds, BOX('ICRS',1.0,2.0,3.0,4.0)) = 1"));
            assertTrue(query.contains("INTERSECTS(position_bounds, CIRCLE('ICRS',1.0,2.0,2.0)) = 1"));

            assertTrue(query.contains("cutoutURL"));
            //assertSingleValueEquals(parameters, "cutout", "BOX(ICRS,1.0,2.0,3.0,4.0)");
            assertSingleValueEquals(parameters, "cutout", "CIRCLE(ICRS,1.0,2.0,4.0)");

            String maxRec = (String) parameters.get("MAXREC");
            assertNull("MAXREC should be null if not specified", maxRec);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

}