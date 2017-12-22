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
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for caom2repo_ws
 * TODO - This file is very inconsistent with how Exceptions are handled.  Each method should throw all unexpected Exceptions, and
 * TODO - not attempt to try/catch them unless there is good reason to.
 * TODO - jenkinsd 2017.10.03
 *
 * @author majorb
 */
public class CaomRepoIntTests extends CaomRepoBaseIntTests {

    private static final Logger log = Logger.getLogger(CaomRepoIntTests.class);

    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_3_NAMESPACE;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    private CaomRepoIntTests() {
    }

    /**
     * @param resourceID resource identifier of service to test
     * @param pem1       PEM file for user with read-write permission
     * @param pem2       PEM file for user with read-only permission
     * @param pem3       PEM file for user with no permissions
     */
    public CaomRepoIntTests(URI resourceID, String pem1, String pem2, String pem3) {
        super(resourceID, Standards.CAOM2REPO_OBS_23, pem1, pem2, pem3);
    }

    @Test
    public void testCleanPutGetSuccess() throws Throwable {
        String observationID = generateID("testCleanPutGetSuccess");

        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane p = new Plane("foo");
        observation.getPlanes().add(p);

        Artifact a = new Artifact(URI.create("ad:FOO/foo"), ProductType.SCIENCE, ReleaseType.DATA);
        p.getArtifacts().add(a);

        Part pa = new Part(0);
        a.getParts().add(pa);

        Chunk ch = new Chunk();
        pa.getChunks().add(ch);

        ch.naxis = 0;
        putObservation(observation, subject1, 200, "OK", null);

        String uri = SCHEME + TEST_COLLECTION + "/" + observationID;

        // get the observation using subject2
        Observation ret = getObservation(uri, subject2, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertEquals("wrong observation", observation, ret);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testGetNoReadPermission() throws Throwable {
        String observationID = generateID("testGetNoReadPermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, 200, "OK", null);

        // get the observation using subject3
        getObservation(uri, subject3, 403, "permission denied: " + uri, EXPECTED_CAOM_VERSION);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testGetNotFound() throws Throwable {
        String observationID = generateID("testGetNotFound");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        getObservation(uri, subject2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testCollectionNotFound() throws Throwable {
        String collection = "NoSuchCollection";
        String observationID = generateID("testCollectionNotFound");
        String path = collection + "/" + observationID;
        String uri = SCHEME + path;

        getObservation(uri, subject2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testInvalidURI() throws Throwable {
        String observationID = generateID("testInvalidURI");
        String path = TEST_COLLECTION + "/" + observationID + "/extraElementsInPath";
        String uri = SCHEME + path;

        super.getObservation(uri, subject2, 400, "invalid input: " + uri, false, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testPutSuccessWCS() throws Throwable {
        String observationID = generateID("testPutSuccessWCS");

        // put an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        observation.getPlanes().add(plane);

        Artifact artifact = new Artifact(new URI("ad:TEST/foo"), ProductType.SCIENCE, ReleaseType.DATA);
        plane.getArtifacts().add(artifact);

        Part part = new Part(0);
        artifact.getParts().add(part);

        Chunk ch = new Chunk();
        part.getChunks().add(ch);

        ch.energy = new SpectralWCS(new CoordAxis1D(new Axis("FREQ", "Hz")), "TOPOCENT");
        ch.energy.getAxis().function = new CoordFunction1D(10L, 1.0, new RefCoord(0.5, 100.0e6)); // 100MHz

        putObservation(observation, subject1, 200, "OK", null);

        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testPutInvalidWCS() throws Throwable {
        String observationID = generateID("testPostInvalidWCS");

        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        Plane plane = new Plane("foo");
        observation.getPlanes().add(plane);

        // computation test looks at science artifacts; we want to test that complete WCS validation works
        plane.getArtifacts().add(new Artifact(new URI("ad:TEST/foo"), ProductType.SCIENCE, ReleaseType.DATA));
        
        Artifact invalid = new Artifact(new URI("ad:TEST/bar"), ProductType.AUXILIARY, ReleaseType.DATA);
        plane.getArtifacts().add(invalid);
        
        Part part = new Part(0);
        invalid.getParts().add(part);

        Chunk ch = new Chunk();
        part.getChunks().add(ch);

        // Use invalid cunit
        ch.energy = new SpectralWCS(new CoordAxis1D(new Axis("FREQ", "Fred")), "TOPOCENT");

        //set delta to 0
        ch.energy.getAxis().function = new CoordFunction1D(10L, 0.0, new RefCoord(0.5, 100.0e6)); // 100MHz

        observation.getPlanes().add(plane);

        putObservation(observation, subject1, 400, "invalid input: ", null);
    }

    @Test
    public void testPutNoWritePermission() throws Throwable {
        String observationID = generateID("testPutNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject2
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject2, 403, "permission denied: " + uri, null);
    }

    @Test
    public void testPutByteLimitExceeded() {
        try {
            String observationID = generateID("testPutByteLimitExceeded");

            // create an observation using subject1
            Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
            putObservation(observation, subject1, 413, "too large:", null);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPutURIsDontMatch() throws Throwable {
        String observationID = generateID("testPutURIsDontMatch");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1 but with a different path on the url
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, 400, "invalid input: " + uri + "-alt", path + "-alt");
    }

    @Test
    public void testPutURIAlreadyExists() throws Throwable {
        String observationID = generateID("testPutURIAlreadyExists");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, null, null, null);

        // create it again to see the conflict
        putObservation(observation, subject1, 409, "already exists: " + uri, null);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testPutValidationFails() throws Throwable {
        String observationID = generateID("testPutValidationFails");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        Observation observation = createInvalidObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, 400, "invalid input: " + uri, null);
    }

    @Test
    public void testPostSuccess() throws Throwable {
        String observationID = generateID("testPostSuccess");

        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        plane.calibrationLevel = CalibrationLevel.RAW_STANDARD;
        observation.getPlanes().add(plane);

        putObservation(observation, subject1, 200, "OK", null);
        
        Observation po = getObservation(observation.getURI().getURI().toASCIIString(), subject1, 200, null, null);
        Plane pp = po.getPlanes().iterator().next();
        pp.dataProductType = DataProductType.CUBE;
        postObservation(po, subject1, 200, "OK", null);

        // cleanup (ok to fail)
        deleteObservation(po.getURI().getURI().toASCIIString(), subject1, null, null);
    }

    @Test
    public void testPostNoWritePermission() throws Throwable {
        String observationID = generateID("testPostNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, 200, "OK", null);

        // overwrite the observation with a post
        postObservation(observation, subject2, 403, "permission denied: " + uri, null);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testPostByteLimitExceeded() {
        try {
            String observationID = generateID("testPostByteLimitExceeded");

            // (byte limit exception will happen before 'doesnt exist'
            // exception)

            // create an observation using subject1
            Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
            postObservation(observation, subject1, 413, "too large:", null);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPostURIsDontMatch() throws Throwable {
        String observationID = generateID("testPostURIsDontMatch");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        // delete any previous run (ok to fail)
        deleteObservation(uri, subject1, null, null);

        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK", null);

        // post an observation using subject1 but with a different path on the url
        postObservation(observation, subject1, 400, "invalid input: " + uri + "-alt", path + "-alt");

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testPostURIDoesntExist() throws Throwable {
        String observationID = generateID("testPostURIDoesntExist");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // post an observation using subject1
        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        postObservation(observation, subject1, 404, "not found: " + uri, null);
    }

    @Test
    public void testPostValidationFails() throws Throwable {
        String observationID = generateID("testPostValidationFails");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // Create one to overwrite with a post (ok to fail)
        SimpleObservation initialOb = new SimpleObservation(TEST_COLLECTION, observationID);
        putObservation(initialOb, subject1, null, null, null);

        Observation observation = createInvalidObservation(TEST_COLLECTION, observationID);

        // create an observation using subject1
        postObservation(observation, subject1, 400, "invalid input: " + uri, null);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testDeleteSuccess() throws Throwable {
        String observationID = generateID("testDeleteSuccess");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        // delete any previous run (ok to fail)
        deleteObservation(uri, subject1, null, null);

        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK", null);

        // delete the observation
        deleteObservation(uri, subject1, 200, "OK");

        // ensure we can't find it on a get
        getObservation(uri, subject2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testDeleteNoWritePermission() throws Throwable {
        String observationID = generateID("testDeleteNoWritePermission");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK", null);

        // delete the observation using subject 2
        putObservation(observation, subject2, 403, "permission denied: " + uri, null);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testDeleteNotFound() throws Throwable {

        String observationID = "testDeleteNotFound";
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // delete the non-existent observation
        deleteObservation(uri, subject1, 404, "not found: " + uri);
    }

    @Test
    public void testPostMultipartSingleParamSuccess() {
        try {
            String observationID = generateID("testPostMultipartSingleParamSuccess");
            final SimpleObservation observation = this.generateObservation(observationID);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("file", convertToFile(observation));

            testPostMultipartWithParamsSuccess(observationID, params);
        } catch (Exception unexpected) {
            log.error("unexpected", unexpected);
            Assert.fail("unexpected: " + unexpected);
        }
    }

    @Test
    public void testPostMultipartMultipleParamSuccess() throws Throwable {
        String observationID = generateID("testPostMultipartMultipleParamSuccess");
        final SimpleObservation observation = this.generateObservation(observationID);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("fooKey", "fooValue");
        params.put("file", convertToFile(observation));
        params.put("barKey", "barValue");

        testPostMultipartWithParamsSuccess(observationID, params);
    }


    private SimpleObservation generateObservation(String observationID) throws Exception {
        // create an observation using subject1
        final SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        plane.calibrationLevel = CalibrationLevel.RAW_STANDARD;
        observation.getPlanes().add(plane);

        putObservation(observation, subject1, 200, "OK", null);

        return (SimpleObservation) getObservation(observation.getURI().getURI().toASCIIString(), 
                subject1, 200, null, null);
    }

    private File convertToFile(SimpleObservation observation) throws IOException {
        StringBuilder sb = new StringBuilder();
        ObservationWriter writer = new ObservationWriter();
        writer.write(observation, sb);
        log.debug(sb.toString());

        File file = new File("build/tmp/testPostMultipartSuccess.xml");
        BufferedWriter bwr = new BufferedWriter(new FileWriter(file));
        bwr.write(sb.toString());
        bwr.flush();
        bwr.close();
        log.info("created: " + file);
        return file;
    }

    private void testPostMultipartWithParamsSuccess(String observationID, final Map<String, Object> params)
        throws Exception {
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        final URL url = new URL(baseHTTPSURL + "/" + path);

        PrivilegedExceptionAction<Object> p = new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                HttpPost httpPost = new HttpPost(url, params, outputStream);
                httpPost.setMaxRetries(4);
                httpPost.run();
                log.debug("throwable: " + httpPost.getThrowable());
                Assert.assertNull("Wrong throwable", httpPost.getThrowable());
                return null;
            }
        };
        Subject.doAs(subject1, p);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    private Observation createVeryLargeObservation(String collection, String observationID) {
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        observation.instrument = new Instrument("FOO");
        String kwStr = "abcdefghijklmnopqrstuvwxyz0123456789";
        long documentSizeMax = (long) 20 * 1024 * 1024;
        long num = (long) (1.5 * documentSizeMax) / kwStr.length();
        log.debug("createVeryLargeObservation: " + num + " keywords");
        long len = 0L;
        for (long i = 0; i < num; i++) {
            String s = kwStr + i;
            len += s.length();
            observation.instrument.getKeywords().add(s);
        }
        log.debug("createVeryLargeObservation: " + observation.instrument.getKeywords().size() + " keywords, length = " + len);
        return observation;
    }

    private Observation createInvalidObservation(String collection, String observationID)
        throws Exception {
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        observation.instrument = new Instrument("INSTR");
        observation.instrument.getKeywords().add("FOO|BAR"); // reserved character

        // ensure we have an invalid observation
        try {
            CaomValidator.validate(observation);
            throw new IllegalStateException("BUG: Test setup - observation not invalid.");
        } catch (IllegalArgumentException e) {
            // expected
        }

        return observation;
    }

    private void postObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
        throws Exception {
        super.sendObservation("POST", observation, subject, expectedResponse, expectedMessage, path);
    }
}
