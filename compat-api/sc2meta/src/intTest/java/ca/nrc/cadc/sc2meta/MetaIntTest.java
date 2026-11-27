/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.sc2meta;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class MetaIntTest 
{
    private static final Logger log = Logger.getLogger(MetaIntTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.meta", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.sc2meta", Level.INFO);
    }
    
    private static final String TEST_URI = "caom:IRIS/f212h000";
    
    private static URI metaServiceURI = URI.create("ivo://cadc.nrc.ca/sc2meta");
    
    RegistryClient rc;
    
    public MetaIntTest() 
        throws Exception
    { 
        this.rc = new RegistryClient();
    }
    
    @Test
    public void testGet()
    {
        try
        {
            URL serviceURL = rc.getServiceURL(metaServiceURI, Standards.CAOM2_OBS_20, AuthMethod.ANON);
            URL url = new URL(serviceURL.toExternalForm() + "?ID="+TEST_URI);
            log.info("testGet: GET " + url.toExternalForm());
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();

            if (get.getThrowable() != null)
                throw (Exception) get.getThrowable();

            Assert.assertEquals("redirect", 200, get.getResponseCode());
            Assert.assertEquals("text/xml", get.getContentType());
            
            ObservationReader r = new ObservationReader(false); // CAOM-2.2 schema doesn't exist
            Observation o = r.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(o);
            Assert.assertEquals(TEST_URI, o.getURI().getURI().toASCIIString());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPost()
    {
        try
        {
            Map<String,Object> params = new HashMap<String,Object>();
            params.put("id", TEST_URI);
            URL url = rc.getServiceURL(metaServiceURI, Standards.CAOM2_OBS_20, AuthMethod.ANON);
            log.info("testPost: POST " + url.toExternalForm() + " params: " + + params.size());
            
            HttpPost post = new HttpPost(url, params, false);
            post.run();

            if (post.getThrowable() != null)
                throw (Exception) post.getThrowable();

            Assert.assertEquals("redirect", 303, post.getResponseCode());
            URL gurl = post.getRedirectURL();
            Assert.assertNotNull("redirect", gurl);

            log.debug("testPost: GET " + gurl.toExternalForm());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(gurl, bos);
            get.run();

            if (get.getThrowable() != null)
                throw (Exception) get.getThrowable();

            Assert.assertEquals("redirect", 200, get.getResponseCode());
            Assert.assertEquals("text/xml", get.getContentType());
            
            ObservationReader r = new ObservationReader(false); // CAOM-2.2 schema doesn't exist
            Observation o = r.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(o);
            Assert.assertEquals(TEST_URI, o.getURI().getURI().toASCIIString());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetJSON()
    {
        try
        {
            URL serviceURL = rc.getServiceURL(metaServiceURI, Standards.CAOM2_OBS_20, AuthMethod.ANON);
            URL url = new URL(serviceURL.toExternalForm() + "?RESPONSEFORMAT=application/json&ID="+TEST_URI);
            log.info("testGetJSON: GET " + url.toExternalForm());
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();

            if (get.getThrowable() != null)
                throw (Exception) get.getThrowable();

            Assert.assertEquals("redirect", 200, get.getResponseCode());
            Assert.assertEquals("application/json", get.getContentType());

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            InputStreamReader isr = new InputStreamReader(bis);
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[16384];
            int n = isr.read(buf);
            while ( n > 0 )
            {
                sb.append(buf, 0, n);
                n = isr.read(buf);
            }
            String str = sb.toString();
            log.debug(str);
            JSONObject doc = new JSONObject(str);
            JSONObject obs = doc.getJSONObject("caom2:Observation");
            
            String xmlns = obs.getString("@xmlns:caom2");
            Assert.assertNotNull(xmlns);
            Assert.assertEquals("http://www.opencadc.org/caom2/xml/v2.3", xmlns);
            
            String otype = obs.getString("@xsi:type");
            Assert.assertNotNull(otype);
            Assert.assertEquals("caom2:SimpleObservation", otype);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
