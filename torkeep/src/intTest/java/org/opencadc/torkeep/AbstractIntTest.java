/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package org.opencadc.torkeep;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.xml.ObservationReader;
import org.opencadc.caom2.xml.ObservationWriter;
import org.opencadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.UUID;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 * Base setup for other integration tests.
 *
 * @author majorb
 */
abstract class AbstractIntTest {

    private static final Logger log = Logger.getLogger(AbstractIntTest.class);

    static final URI RESOURCE_ID = URI.create("ivo://opencadc.org/caom25/torkeep");
            
    static final URI OBS_STANDARD_ID = URI.create("ivo://ivoa.net/std/CAOM-Repo#observations-1.0");
    static final URI DEL_STANDARD_ID = URI.create("ivo://ivoa.net/std/CAOM-Repo#deleted-observations-1.0");
    
    static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_5_NAMESPACE;
    
    static final String TEST_COLLECTION = "TEST";

    // subject1 has read/write privilege on the TEST collection
    String rwCertName = "torkeep-rw.pem";
    Subject subject1;

    // subject2 has read privilege on the TEST collection
    String roCertName = "torkeep-ro.pem";
    Subject subject2;

    final Subject subject3 = AuthenticationUtil.getAnonSubject();
    
    final String baseObsURL;
    final String baseDeletedObsURL;

    static final String SCHEME = "caom:";

    // service should be written to output documents with this version
    private static final String TEXT_XML = "text/xml";

    AbstractIntTest() {
        try {
            File sslCert1 = FileUtil.getFileFromResource(rwCertName, this.getClass());
            subject1 = SSLUtil.createSubject(sslCert1);
            
            File sslCert2 = FileUtil.getFileFromResource(roCertName, this.getClass());
            subject2 = SSLUtil.createSubject(sslCert2);

            RegistryClient rc = new RegistryClient();
            URL url = rc.getServiceURL(RESOURCE_ID, OBS_STANDARD_ID, AuthMethod.ANON);
            baseObsURL = url.toExternalForm();
            url = rc.getServiceURL(RESOURCE_ID, DEL_STANDARD_ID, AuthMethod.ANON);
            baseDeletedObsURL = url.toExternalForm();
            
            log.info("observations URL: " + baseObsURL);
            log.info("   deletions URL: " + baseDeletedObsURL);
        } catch (Throwable t) {
            String message = "Failed int-test initialization: " + t.getMessage();
            log.fatal(message, t);
            throw new ExceptionInInitializerError(message);
        }
    }

    protected String uriToPath(URI uri) {
        if ("caom".equals(uri.getScheme())) {
            return uri.getSchemeSpecificPart();
        }
        if ("ivo".equals(uri.getScheme())) {
            // strip preceeding / since baseURL has it
            return uri.getPath().substring(1) + "/" + uri.getQuery();
        }
        throw new RuntimeException("TEST SETUP: unexpected uriToPath for " + uri);
    }

    public String generateID(String base) {
        return base + "-" + UUID.randomUUID().toString();
    }

    private HttpURLConnection openConnection(Subject subject, URL url)
            throws Exception {
        
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        if (subject != null) {
            SSLSocketFactory sf = SSLUtil.getSocketFactory(subject);
            conn.setSSLSocketFactory(sf);
        }
        conn.setInstanceFollowRedirects(false);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    protected void updateObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage)
        throws Exception {
        sendObservation("POST", observation, subject, expectedResponse, expectedMessage, null);
    }

    protected void putObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage)
        throws Exception {
        sendObservation("PUT", observation, subject, expectedResponse, expectedMessage, null);
    }

    protected void putObservationCompat(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
        throws Exception {
        sendObservation("PUT", observation, subject, expectedResponse, expectedMessage, path);
    }

    protected void sendObservation(String method, final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage,
                                   String path)
        throws Exception {
        sendObservation(method, observation, subject, expectedResponse, expectedMessage, path, null);
    }
    
    protected void sendObservation(String method, final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage,
                                   String path, String httpIfMatchHeaderValue)
        throws Exception {
        log.debug("start " + method.toLowerCase() + " on " + observation.toString());

        String urlPath = path;
        if (urlPath == null) {
            urlPath = uriToPath(observation.getURI());
        }
        String surl = baseObsURL + "/" + urlPath;
        URL url = new URL(surl);
        log.info(method + " " + url.toString());
        HttpURLConnection conn = openConnection(subject, url);
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", TEXT_XML);
        if (httpIfMatchHeaderValue != null) {
            conn.setRequestProperty("If-Match", httpIfMatchHeaderValue);
        }

        OutputStream out = conn.getOutputStream();
        log.debug("write: " + observation);
        ByteCountOutputStream bcos = new ByteCountOutputStream(out);
        ObservationWriter writer = new ObservationWriter();
        writer.write(observation, bcos);
        log.debug(" wrote: " + bcos.getByteCount() + " bytes");

        int response = -1;
        try {
            response = conn.getResponseCode();
        } catch (IOException ex) {
            if (expectedResponse != null && expectedResponse == 413) {
                log.warn("expected 413 and getResponseCode() threw " + ex + ": known issue in JDK http lib");
            }
            return;
        }

        String message = conn.getResponseMessage();
        if (response != 200) {
            message = NetUtil.getErrorBody(conn).trim();
        }
        
        log.info(method + " " + response + " "  + message);

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (response != 200) {
            if (expectedMessage != null) {
                Assert.assertNotNull(message);
                if (expectedResponse != null 
                        && (expectedResponse == 400 || expectedResponse == 412)) {
                    // service provides extra info so check the start only
                    message = message.substring(0, expectedMessage.length());
                }
                Assert.assertEquals("Wrong response message", expectedMessage, message);
            }
        }

        conn.disconnect();
    }

    protected Observation getObservation(URI uri, Subject subject, Integer expectedResponse, String expectedMessage, String expectedCaomVersion)
        throws Exception {
        return getObservation(uri, subject, expectedResponse, expectedMessage, true, expectedCaomVersion);
    }

    protected Observation getObservation(URI uri, Subject subject, Integer expectedResponse, String expectedMessage, boolean exactMatch, String
        expectedCaomVersion)
        throws Exception {
        log.debug("start get on " + uri);

        // extract the path from the uri
        String surl = baseObsURL + "/" + uriToPath(uri);
        log.info("GET " + surl);
        URL url = new URL(surl);
        ObservationReader reader = new ObservationReader();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(url, bos);

        Subject.doAs(subject, new RunnableAction(get));

        int response = get.getResponseCode();

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (expectedMessage != null) {
            Assert.assertNotNull(get.getThrowable());
            String message = get.getThrowable().getMessage();
            Assert.assertNotNull(message);
            message = message.trim();
            if (exactMatch) {
                Assert.assertEquals("response message", expectedMessage, message);
            } else {
                Assert.assertTrue("response message prefix", message.startsWith(expectedMessage));
            }
        }

        if (response == 200) {
            //InputStream in = conn.getInputStream();
            if (expectedCaomVersion != null) {
                String doc = bos.toString();
                Assert.assertNotNull("document is null", doc);
                Assert.assertTrue("document namespace=" + expectedCaomVersion, doc.contains(expectedCaomVersion));
            }
            InputStream in = new ByteArrayInputStream(bos.toByteArray());
            Observation observation = reader.read(in);

            //conn.disconnect();

            return observation;

        }
        return null;
    }

    protected void deleteObservation(URI uri, Subject subject, Integer expectedResponse, String expectedMessage)
        throws Exception {
        log.debug("start delete on " + uri);

        String surl = baseObsURL + "/" + uriToPath(uri);
        log.info("DELETE " + surl);
        URL url = new URL(surl);
        HttpURLConnection conn = openConnection(subject, url);
        conn.setRequestMethod("DELETE");

        int response = conn.getResponseCode();
        String message = conn.getResponseMessage();
        if (response != 200) {
            message = NetUtil.getErrorBody(conn).trim();
        }

        log.info("DELETE " + response + " "  + message);

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (response != 200) {
            if (expectedMessage != null) {
                Assert.assertNotNull(message);
                Assert.assertEquals("Wrong response message", expectedMessage, message);
            }
        }
        
        conn.disconnect();
    }

}
