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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.repo.integration;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.Subject;

import ca.nrc.cadc.reg.Standards;
import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.UUID;

/**
 * Integration tests for caom2repo_ws
 * 
 * @author majorb
 *
 */
public class CaomRepoIntTests
{
    
    private static final Logger log = Logger.getLogger(CaomRepoIntTests.class);
    
    private static final String TEST_COLLECTION = "TEST";
    
    // subject1 has read/write privilege on the TEST collection
    private Subject SUBJECT1;
    
    // subject2 has read privilege on the TEST collection
    private Subject SUBJECT2;
    
    // subject3 has not read or write permission on the TEST collection
    private Subject SUBJECT3;
    
    private URL AVAIL_URL;
    private String BASE_HTTP_URL;
    private String BASE_HTTPS_URL;
    
    private static final String SCHEME = "caom:";
    
    // service should be written to output documents with this version
    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_2_NAMESPACE;
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }
    
    public CaomRepoIntTests() 
    { 
        try
        {
            File SSL_CERT1 = FileUtil.getFileFromResource("proxy1.pem", CaomRepoIntTests.class);
            File SSL_CERT2 = FileUtil.getFileFromResource("proxy2.pem", CaomRepoIntTests.class);
            File SSL_CERT3 = FileUtil.getFileFromResource("proxy3.pem", CaomRepoIntTests.class);

            SUBJECT1 = SSLUtil.createSubject(SSL_CERT1);
            SUBJECT2 = SSLUtil.createSubject(SSL_CERT2);
            SUBJECT3 = SSLUtil.createSubject(SSL_CERT3);
            
            RegistryClient rc = new RegistryClient();

            URI serviceURI = new URI("ivo://cadc.nrc.ca/caom2repo");

            URL serviceURL = rc.getServiceURL(serviceURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
            AVAIL_URL = serviceURL;

            serviceURL = rc.getServiceURL(serviceURI, Standards.CAOM2REPO_OBS_20, AuthMethod.ANON);
            BASE_HTTP_URL = serviceURL.toExternalForm();

            serviceURL = rc.getServiceURL(serviceURI, Standards.CAOM2REPO_OBS_20, AuthMethod.CERT);
            BASE_HTTPS_URL = serviceURL.toExternalForm();

            log.debug("test service URL: " + BASE_HTTP_URL);
            log.debug("test service URL: " + BASE_HTTPS_URL);
        }
        catch (Throwable t)
        {
            String message = "Failed int-test initialization: " + t.getMessage();
            log.fatal(message, t);
            throw new ExceptionInInitializerError(message);
        }
    }
    
    public String generateObservationID(String base)
    {
        return base + "-" + UUID.randomUUID().toString();
    }
    
    @Test
    public void testAvailability()
    {
        try
        {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            HttpDownload dl = new HttpDownload(AVAIL_URL, buf);
            dl.run();
            Assert.assertEquals(200, dl.getResponseCode());
            Assert.assertEquals("text/xml", dl.getContentType());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCleanPutGetSuccess() throws Throwable
    {
        String observationID = generateObservationID("testCleanPutGetSuccess");
        
        String uri = SCHEME + TEST_COLLECTION + "/" + observationID;
        
        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane p = new Plane("foo");
        Artifact a = new Artifact(URI.create("ad:FOO/foo"), ProductType.SCIENCE, ReleaseType.DATA);
        Part pa = new Part(0);
        Chunk ch = new Chunk();
        ch.naxis = 0;
        pa.getChunks().add(ch);
        a.getParts().add(pa);
        p.getArtifacts().add(a);
        observation.getPlanes().add(p);
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // get the observation using subject2
        Observation ret = getObservation(uri, SUBJECT2, 200, null);
        Assert.assertEquals("wrong observation", observation, ret);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testGetNoReadPermission() throws Throwable
    {
        String observationID = generateObservationID("testGetNoReadPermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // get the observation using subject3
        getObservation(uri, SUBJECT3, 403, "permission denied: " + uri);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testGetNotFound() throws Throwable
    {
        String observationID = generateObservationID("testGetNotFound");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        getObservation(uri, SUBJECT2, 404, "not found: " + uri);
    }
    
    @Test
    public void testCollectionNotFound() throws Throwable
    {
        String collection = "NoSuchCollection";
        String observationID = generateObservationID("testGetNotFound");
        String path =  collection + "/" + observationID;
        String uri = SCHEME + path;
        
        getObservation(uri, SUBJECT2, 404, "collection not found: " + collection);
    }
    
    @Test
    public void testInvalidURI() throws Throwable
    {
        String collection = TEST_COLLECTION;
        String observationID = generateObservationID("testGetNotFound");
        String path =  collection + "/" + observationID + "/extraElementsInPath";
        String uri = SCHEME + path;
        
        getObservation(uri, SUBJECT2, 400, "invalid input: " + uri, false);
    }
    
    @Test
    public void testPutSuccessWCS() throws Throwable
    {
        String observationID = generateObservationID("testPutSuccessWCS");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // put an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        Artifact artifact = new Artifact(new URI("ad:TEST/foo"), ProductType.SCIENCE, ReleaseType.DATA);
        Part part = new Part(0);
        Chunk ch = new Chunk();
        ch.energy = new SpectralWCS(new CoordAxis1D(new Axis("FREQ", "Hz")), "TOPOCENT");
        ch.energy.getAxis().function = new CoordFunction1D(10L, 1.0, new RefCoord(0.5, 100.0e6)); // 100MHz
        part.getChunks().add(ch);
        artifact.getParts().add(part);
        plane.getArtifacts().add(artifact);
        observation.getPlanes().add(plane);
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }

    @Test
    public void testPutNoWritePermission() throws Throwable
    {
        String observationID = generateObservationID("testPutNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject2
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT2, 403, "permission denied: " + uri, null);
    }
    
    @Test
    public void testPutByteLimitExceeded() throws Throwable
    {
        String observationID = generateObservationID("testPutByteLimitExceeded");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1
        Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, 413, "too large: " + uri, null);
    }
    
    @Test
    public void testPutURIsDontMatch() throws Throwable
    {
        String observationID = generateObservationID("testPutURIsDontMatch");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1 but with a different path on the url
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, 400, "invalid input: " + uri +"-alt", path + "-alt");
    }
    
    @Test
    public void testPutURIAlreadyExists() throws Throwable
    {
        String observationID = generateObservationID("testPutURIAlreadyExists");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, null, null, null);
        
        // create it again to see the conflict
        putObservation(observation, SUBJECT1, 409, "already exists: " + uri, null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testPutValidationFails() throws Throwable
    {
        String observationID = generateObservationID("testPutValidationFails");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        Observation observation = createInvalidObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, 400, "invalid input: " + uri, null);
    }
    
    @Test
    public void testPostSuccess() throws Throwable
    {
        String observationID = generateObservationID("testPostSuccess");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        plane.calibrationLevel = CalibrationLevel.RAW_STANDARD;
        observation.getPlanes().add(plane);

        putObservation(observation, SUBJECT1, 200, "OK", null);

        // modify the plane since that also tweaks the Observation.maxLastModified
        plane.dataProductType = DataProductType.CUBE;
        
        // overwrite the observation with a post
        postObservation(observation, SUBJECT1, 200, "OK", null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testPostNoWritePermission() throws Throwable
    {
        String observationID = generateObservationID("testPostNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // overwrite the observation with a post
        postObservation(observation, SUBJECT2, 403, "permission denied: " + uri, null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testPostByteLimitExceeded() throws Throwable
    {
        String observationID = generateObservationID("testPostByteLimitExceeded");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // (byte limit exception will happen before 'doesnt exist'
        // exception)
        
        // create an observation using subject1
        Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
        postObservation(observation, SUBJECT1, 413, "too large: " + uri, null);
    }
    
    @Test
    public void testPostURIsDontMatch() throws Throwable
    {   
        String observationID = generateObservationID("testPostURIsDontMatch");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        
        // delete any previous run (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
        
        // create an observation using subject1
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // post an observation using subject1 but with a different path on the url
        postObservation(observation, SUBJECT1, 400, "invalid input: " + uri + "-alt", path + "-alt");
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testPostURIDoesntExist() throws Throwable
    {
        String observationID = generateObservationID("testPostURIDoesntExist");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // post an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        postObservation(observation, SUBJECT1, 404, "not found: " + uri, null);
    }
    
    @Test
    public void testPostValidationFails() throws Throwable
    {
        String observationID = generateObservationID("testPostValidationFails");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // Create one to overwrite with a post (ok to fail)
        SimpleObservation initialOb = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(initialOb, SUBJECT1, null, null, null);
        
        Observation observation = createInvalidObservation(TEST_COLLECTION, observationID);
        
        // create an observation using subject1
        postObservation(observation, SUBJECT1, 400, "invalid input: " + uri, null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testDeleteSuccess() throws Throwable
    {
        String observationID = generateObservationID("testDeleteSuccess");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        
        // delete any previous run (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
        
        // create an observation using subject1
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // delete the observation
        deleteObservation(uri, SUBJECT1, 200, "OK");
        
        // ensure we can't find it on a get
        getObservation(uri, SUBJECT2, 404, "not found: " + uri);
    }
    
    @Test
    public void testDeleteNoWritePermission() throws Throwable
    {   
        String observationID = generateObservationID("testDeleteNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        
        // create an observation using subject1
        putObservation(observation, SUBJECT1, 200, "OK", null);
        
        // delete the observation using subject 2 
        putObservation(observation, SUBJECT2, 403, "permission denied: " + uri, null);
        
        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }
    
    @Test
    public void testDeleteNotFound() throws Throwable
    {
        
        String observationID = "testDeleteNotFound";
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        
        // delete the non-existent observation
        deleteObservation(uri, SUBJECT1, 404, "not found: " + uri);
    }
    
    private HttpURLConnection openConnection(Subject subject, String urlPath)
            throws Exception
    {
        HttpURLConnection conn;
        URL url;
        if (subject == null)
        {
            url = new URL(BASE_HTTP_URL + "/" + urlPath);
            log.debug("opening connection to: " + url.toString());
            conn = (HttpURLConnection) url.openConnection();
        }
        else
        {
            url = new URL(BASE_HTTPS_URL + "/" + urlPath);
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
    
    private long DOCUMENT_SIZE_MAX = (long) 1.1*20971520L;
    private String KW_STR = "abcdefghijklmnopqrstuvwxyz0123456789";
    private Observation createVeryLargeObservation(String collection, String observationID)
    {
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        observation.instrument = new Instrument("FOO");
        long num = DOCUMENT_SIZE_MAX/KW_STR.length();
        log.debug("createVeryLargeObservation: " + num + " keywords");
        long len = 0L;
        for (long i=0; i<num; i++)
        {
            String s = KW_STR + i;
            len += s.length();
            observation.instrument.getKeywords().add(s);
        }
        log.debug("createVeryLargeObservation: " + observation.instrument.getKeywords().size() + " keywords, length = " + len);
        return observation;
    }
    
    private Observation createInvalidObservation(String collection, String observationID)
            throws Exception
    {
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        Plane plane = new Plane("plane");
        Artifact artifact = new Artifact(new URI(SCHEME + collection + "/artifact"), ProductType.SCIENCE, ReleaseType.DATA);
        Part part = new Part("part");
        Chunk chunk = new Chunk();
        
        String ctype = "STOKES";
        String cunit = "unit";
        
        Axis axis = new Axis(ctype, cunit);
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        PolarizationWCS polarization = new PolarizationWCS(coordAxis1D);
        Long naxis = 20L;
        Double delta = 1D;
        RefCoord refCoord = new RefCoord(0.5, 1);
        CoordFunction1D coordFunction1D = new CoordFunction1D(naxis, delta, refCoord);
        
        coordAxis1D.function = coordFunction1D;
        chunk.polarization = polarization;
        
        part.getChunks().add(chunk);
        artifact.getParts().add(part);
        plane.getArtifacts().add(artifact);
        observation.getPlanes().add(plane);
        
        // ensure we have an invalid observation
        try
        {
            plane.computeTransientState();
            throw new IllegalStateException("Test observation not invalid.");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
        
        return observation;
    }
    
    private void putObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
            throws Throwable
    {
        sendObservation("PUT", observation, subject, expectedResponse, expectedMessage, path);
    }
    
    private void postObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
            throws Throwable
    {
        sendObservation("POST", observation, subject, expectedResponse, expectedMessage, path);
    }
    
    private void sendObservation(String method, final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
            throws Throwable
    {
        log.debug("start " + method.toLowerCase() + " on " + observation.toString());
        
        String urlPath = path;
        if (urlPath == null)
        {
            // extract the path from the observation
            urlPath = observation.getURI().getURI().getSchemeSpecificPart();
        }
        
        ObservationWriter writer = new ObservationWriter();
        
        HttpURLConnection conn = openConnection(subject, urlPath);
        conn.setRequestMethod(method);
        
        OutputStream out = conn.getOutputStream();
        writer.write(observation, out);
        
        int response = conn.getResponseCode();
        String message = conn.getResponseMessage();
        if (response != 200)
            message = NetUtil.getErrorBody(conn).trim();
        
        log.debug(method.toLowerCase() + " response: " + message + " (" + response + ")");
        
        if (expectedResponse != null)
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        
        if (expectedMessage != null)
        {
            Assert.assertNotNull(message);
            if (expectedResponse != null && expectedResponse.intValue() == 400 )
            {
                // service provides extra info so check the start only
                message = message.substring(0, expectedMessage.length());
            }
            Assert.assertEquals("Wrong response message", expectedMessage, message);
        }
        
        conn.disconnect();
    }
    
    private Observation getObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage)
            throws Exception
    {
        return getObservation(uri, subject, expectedResponse, expectedMessage, true);
    }
    
    private Observation getObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage, boolean exactMatch)
            throws Exception
    {
        log.debug("start get on " + uri);
        
        // extract the path from the uri
        URI ouri = new URI(uri);
        String surl = BASE_HTTP_URL + "/" + ouri.getSchemeSpecificPart();
        if (subject != null)
            surl = BASE_HTTPS_URL + "/" + ouri.getSchemeSpecificPart();
        URL url = new URL(surl);
        ObservationReader reader = new ObservationReader();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpDownload get = new HttpDownload(url, bos);
        //HttpURLConnection conn = openConnection(subject, urlPath);
        //conn.setRequestMethod("GET");
        
        Subject.doAs(subject, new RunnableAction(get));
        
        int response = get.getResponseCode();
        
        if (expectedResponse != null)
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        
        if (expectedMessage != null)
        {
            String message = bos.toString().trim();
            Assert.assertNotNull(message);
            if (exactMatch)
                Assert.assertEquals("Wrong response message", expectedMessage, message);
            else
                Assert.assertTrue("Wrong response message (startsWith)", message.startsWith(expectedMessage));
        }
        
        if (response == 200)
        {
            //InputStream in = conn.getInputStream();
            if (EXPECTED_CAOM_VERSION != null)
            {
                String doc = bos.toString();
                Assert.assertTrue("document namespace="+EXPECTED_CAOM_VERSION, doc.contains(EXPECTED_CAOM_VERSION));
            }
            InputStream in = new ByteArrayInputStream(bos.toByteArray());
            Observation observation = reader.read(in);
            
            //conn.disconnect();
            
            return observation;
            
        }
        return null;
    }
    
    private void deleteObservation(String uri, Subject subject, Integer expectedResponse, String expectedMessage)
            throws Throwable
    {
        log.debug("start delete on " + uri);
        
        // extract the path from the uri
        String urlPath = uri.substring(uri.indexOf(SCHEME) + SCHEME.length());
        
        HttpURLConnection conn = openConnection(subject, urlPath);
        conn.setRequestMethod("DELETE");
        
        int response = conn.getResponseCode();
        String message = conn.getResponseMessage();
        if (response != 200)
            message = NetUtil.getErrorBody(conn).trim();
        
        log.debug("delete response: " + message + " (" + response + ")");
        
        if (expectedResponse != null)
            Assert.assertEquals("Wrong response", expectedResponse.intValue(), response);
        
        if (expectedMessage != null)
        {
            Assert.assertNotNull(message);
            Assert.assertEquals("Wrong response message", expectedMessage, message);
        }
        
        conn.disconnect();
    }

}
