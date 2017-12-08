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

package ca.nrc.cadc.caom2.repo.integration;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.HttpDownload;
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
 * Integration tests for caom2repo_ws
 *
 * @author majorb
 */
class CaomRepoBaseIntTests {

    private static final Logger log = Logger.getLogger(CaomRepoBaseIntTests.class);

    static final String TEST_COLLECTION = "TEST";

    // subject1 has read/write privilege on the TEST collection
    Subject subject1;

    // subject2 has read privilege on the TEST collection
    Subject subject2;

    // subject3 has not read or write permission on the TEST collection
    Subject subject3;

    final String baseHTTPURL;
    final String baseHTTPSURL;

    static final String SCHEME = "caom:";

    // service should be written to output documents with this version
    private static final String TEXT_XML = "text/xml";

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    CaomRepoBaseIntTests() {
        subject1 = null;
        subject2 = null;
        subject3 = null;
        baseHTTPURL = null;
        baseHTTPSURL = null;
    }

    /**
     * @param resourceID resource identifier of service to test
     * @param pem1       PEM file for user with read-write permission
     * @param pem2       PEM file for user with read-only permission
     * @param pem3       PEM file for user with no permissions
     */
    public CaomRepoBaseIntTests(URI resourceID, URI repoStandardID, String pem1, String pem2, String pem3) {
        try {
            if (pem1 != null) {
                File sslCert1 = FileUtil.getFileFromResource(pem1, this.getClass());
                subject1 = SSLUtil.createSubject(sslCert1);
            }
            
            if (pem2 != null) {
                File sslCert2 = FileUtil.getFileFromResource(pem2, this.getClass());
                subject2 = SSLUtil.createSubject(sslCert2);
            }
            
            if (pem3 != null) {
                File sslCert3 = FileUtil.getFileFromResource(pem3, this.getClass());
                subject3 = SSLUtil.createSubject(sslCert3);
            }

            RegistryClient rc = new RegistryClient();

            URL serviceURL = rc.getServiceURL(resourceID, repoStandardID, AuthMethod.ANON);
            baseHTTPURL = serviceURL.toExternalForm();

            serviceURL = rc.getServiceURL(resourceID, repoStandardID, AuthMethod.CERT);
            baseHTTPSURL = serviceURL.toExternalForm();

            log.debug("test service URL: " + baseHTTPURL);
            log.debug("test service URL: " + baseHTTPSURL);
        } catch (Throwable t) {
            String message = "Failed int-test initialization: " + t.getMessage();
            log.fatal(message, t);
            throw new ExceptionInInitializerError(message);
        }
    }

    public String generateID(String base) {
        return base + "-" + UUID.randomUUID().toString();
    }

    private HttpURLConnection openConnection(Subject subject, String urlPath)
        throws Exception {
        HttpURLConnection conn;
        URL url;
        if (subject == null) {
            url = new URL(baseHTTPURL + "/" + urlPath);
            log.debug("opening connection to: " + url.toString());
            conn = (HttpURLConnection) url.openConnection();
        } else {
            url = new URL(baseHTTPSURL + "/" + urlPath);
            log.debug("opening connection to: " + url.toString());
            conn = (HttpsURLConnection) url.openConnection();
            SSLSocketFactory sf = SSLUtil.getSocketFactory(subject);
            ((HttpsURLConnection) conn).setSSLSocketFactory(sf);
        }
        conn.setInstanceFollowRedirects(false);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    protected void putObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
        throws Exception {
        sendObservation("PUT", observation, subject, expectedResponse, expectedMessage, path);
    }

    protected void sendObservation(String method, final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage,
                                   String path)
        throws Exception {
        log.debug("start " + method.toLowerCase() + " on " + observation.toString());

        String urlPath = path;
        if (urlPath == null) {
            // extract the path from the observation
            urlPath = observation.getURI().getURI().getSchemeSpecificPart();
        }

        HttpURLConnection conn = openConnection(subject, urlPath);
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", TEXT_XML);

        OutputStream out = conn.getOutputStream();
        log.debug("write: " + observation);
        ByteCountOutputStream bcos = new ByteCountOutputStream(out);
        ObservationWriter writer = new ObservationWriter();
        writer.write(observation, bcos);
        log.debug(" wrote: " + bcos.getByteCount() + " bytes");

        int response = -1;
        try {
            log.debug("getResponseCode()");
            response = conn.getResponseCode();
            log.debug("getResponseCode() returned " + response);
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

        log.debug(method.toLowerCase() + " response: " + message + " (" + response + ")");

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (expectedMessage != null) {
            Assert.assertNotNull(message);
            if (expectedResponse != null && expectedResponse == 400) {
                // service provides extra info so check the start only
                message = message.substring(0, expectedMessage.length());
            }
            Assert.assertEquals("Wrong response message", expectedMessage, message);
        }

        conn.disconnect();
    }

    protected Observation getObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage, String expectedCaomVersion)
        throws Exception {
        return getObservation(uri, subject, expectedResponse, expectedMessage, true, expectedCaomVersion);
    }

    protected Observation getObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage, boolean exactMatch, String
        expectedCaomVersion)
        throws Exception {
        log.debug("start get on " + uri);

        // extract the path from the uri
        URI ouri = new URI(uri);
        String surl = baseHTTPURL + "/" + ouri.getSchemeSpecificPart();
        if (subject != null) {
            surl = baseHTTPSURL + "/" + ouri.getSchemeSpecificPart();
        }
        URL url = new URL(surl);
        ObservationReader reader = new ObservationReader();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpDownload get = new HttpDownload(url, bos);

        Subject.doAs(subject, new RunnableAction(get));

        int response = get.getResponseCode();

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (expectedMessage != null) {
            String message = bos.toString().trim();
            Assert.assertNotNull(message);
            if (exactMatch) {
                Assert.assertEquals("Wrong response message", expectedMessage, message);
            } else {
                Assert.assertTrue("message long enough", (message.length() >= expectedMessage.length()));
                String cmpPart = message.substring(0, expectedMessage.length());
                Assert.assertEquals("Wrong response message (startsWith)", expectedMessage, cmpPart);
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

    protected void deleteObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage)
        throws Exception {
        log.debug("start delete on " + uri);

        // extract the path from the uri
        String urlPath = uri.substring(uri.indexOf(SCHEME) + SCHEME.length());

        HttpURLConnection conn = openConnection(subject, urlPath);
        conn.setRequestMethod("DELETE");

        int response = conn.getResponseCode();
        String message = conn.getResponseMessage();
        if (response != 200) {
            message = NetUtil.getErrorBody(conn).trim();
        }

        log.debug("delete response: " + message + " (" + response + ")");

        if (expectedResponse != null) {
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        }

        if (expectedMessage != null) {
            Assert.assertNotNull(message);
            Assert.assertEquals("Wrong response message", expectedMessage, message);
        }

        conn.disconnect();
    }

}
