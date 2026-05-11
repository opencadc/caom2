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

package ca.nrc.cadc.sia.integration;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.net.ContentType;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.sia.AdqlQueryGenerator;
import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 *
 * @author jburke, zhangsa
 */
public class Sia1QueryTest
{
    private static Logger log = Logger.getLogger(Sia1QueryTest.class);
    
    protected static String serviceUrl;

    @BeforeClass
    public static void setUpClass()
        throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc.sia", Level.INFO);
        try
        {
            RegistryClient rc = new RegistryClient();
            URL serviceURL = rc.getServiceURL(
                URI.create("ivo://cadc.nrc.ca/sia"), Standards.SIA_10, AuthMethod.ANON);
            Sia1QueryTest.serviceUrl = serviceURL.toExternalForm();
            log.info("serviceURL: " + Sia1QueryTest.serviceUrl);
        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to init SIA URI and URL for tests", t);
        }
    }

    /**
     * Test Get operation which returns ERROR status.
     * 
     * @author zhangsa
     */
    @Test
    public void testGetError()
    {
        String[] queries = new String[]
        {
                "POS=a,b",
                "POS=180",
                "POS=180&SIZE=1",
                "POS=180&SIZE=0.5,0.5,3"
        };
        for (String query : queries)
        {
            doGet(query, "ERROR", AdqlQueryGenerator.SIA_CONTENT_TYPE);
        }
    }

    @Test
    public void testPostError()
    {
        String[] queries = new String[]
        {
              "POS=a"
        };
        for (String query : queries)
        {
            doPost(query, "ERROR", AdqlQueryGenerator.SIA_CONTENT_TYPE);
        }
    }
    
    /**
     * Test GET operation which returns OK status.
     * 
     * @author zhangsa
     */
    @Test
    public void testGetOK()
    {
        String[] queries = new String[]
        {
                "POS=180,5",
                "POS=180,5&SIZE=0.5",
                "POS=180,5&SIZE=0.5,0.5"
        };
        for (String query : queries)
        {
            doGet(query, "OK", AdqlQueryGenerator.SIA_CONTENT_TYPE);
        }
    }

    @Test
    public void testPostOK()
    {
        String[] queries = new String[]
        {
              "POS=180,5"
        };
        for (String query : queries)
        {
            doPost(query, "OK", AdqlQueryGenerator.SIA_CONTENT_TYPE);
        }
    }

    @Test
    public void testFormatMetadata()
    {
        VOTableDocument doc = doGet("FORMAT=METADATA", "OK", null);

        // TODO: verify that additional required metadata is present?
    }
    
    public VOTableDocument doGet(String query, String expectedStatus, String expectedContentType)
    {
        try
        {
            URL url = new URL(serviceUrl + "?" + query);
            log.info("GET url: " + url.toExternalForm());
            HttpGet get = new HttpGet(url,true);
            get.prepare();

            assertNull("Errors during get", get.getThrowable());
            assertEquals(200, get.getResponseCode());

            String contentType = get.getContentType();
            log.debug("Content-Type from getHeaderField: " + contentType);
            if (expectedContentType != null)
            {
                assertEquals(String.format("For Query string of [%s], content type ", query),
                    new ContentType(expectedContentType), new ContentType(contentType));
            }

            VOTableDocument vot = VOTableHandler.getVOTable(new InputStreamReader(get.getInputStream()));

            String queryStatus = VOTableHandler.getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals(expectedStatus, queryStatus);

            return vot;
        }
        catch (Exception e)
        {
            log.error("unexpected exception", e);
            fail("unexpected exception: " + e);
        }
        return null; // cannot be reached, but compiler does not know Assert.fail
    }
    
    public VOTableDocument doPost(String query, String expectedStatus, String expectedContentType)
    {
        try
        {
            Map<String, Object> parameters = new HashMap<String, Object>();
            String paras[] = query.split("&");
            String name;
            String value;
            for (String para : paras)
            {
                int eqLoc = para.indexOf("=");
                name = para.substring(0, eqLoc);
                value = para.substring(eqLoc + 1);
                parameters.put(name, value);
            }

            HttpPost post = new HttpPost(new URL(serviceUrl), parameters, true);
            post.prepare();

            assertNull("Errors during post", post.getThrowable());
            assertEquals(200, post.getResponseCode());

            String contentType = post.getContentType();
            log.debug("Content-Type from getHeaderField: " + contentType);
            if (expectedContentType != null)
            {
                assertEquals(String.format("For Query string of [%s], content type ", query),
                    new ContentType(expectedContentType), new ContentType(contentType));
            }

            VOTableDocument vot = VOTableHandler.getVOTable(new InputStreamReader(post.getInputStream()));

            String queryStatus = VOTableHandler.getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals(expectedStatus, queryStatus);

            return vot;
        }
        catch (Exception e)
        {
            log.error("unexpected exception", e);
            fail("unexpected exception: " + e);
        }
        return null; // cannot be reached, but compiler does not know Assert.fail
    }

}
