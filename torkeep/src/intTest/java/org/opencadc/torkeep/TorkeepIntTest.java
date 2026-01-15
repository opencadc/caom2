/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2026.                            (c) 2026.
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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.Log4jInit;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.Instrument;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.ObservationIntentType;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.SimpleObservation;
import org.opencadc.caom2.Telescope;
import org.opencadc.caom2.util.CaomValidator;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.xml.ObservationWriter;

/**
 *
 * @author pdowler
 */
public class TorkeepIntTest extends AbstractIntTest {
    private static final Logger log = Logger.getLogger(TorkeepIntTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.torkeep", Level.INFO);
    }

    public TorkeepIntTest() {
    }

    @Test
    public void testPutGetUpdateXML() throws Exception {
        doPutGetUpdateDelete("testPutGetUpdateXML", RepoAction.CAOM_XML_MIMETYPE);
        doPutGetUpdateDelete("testPutGetUpdateXML", RepoAction.XML_MIMETYPE);
    }
    
    @Test
    public void testPutGetUpdateJSON() throws Exception {
        doPutGetUpdateDelete("testPutGetUpdateJSON", RepoAction.CAOM_JSON_MIMETYPE);
        doPutGetUpdateDelete("testPutGetUpdateJSON", RepoAction.JSON_MIMETYPE);
    }

    private void doPutGetUpdateDelete(String name, String contentType) throws Exception {
        // create an observation using subject1
        URI ouri = URI.create("caom:" + TEST_COLLECTION + "/" + name);
        Observation observation = new SimpleObservation(TEST_COLLECTION, ouri, SimpleObservation.EXPOSURE);
        Plane p = new Plane(URI.create(observation.getURI().toASCIIString() + "/bar"));
        observation.getPlanes().add(p);

        Artifact a = new Artifact(URI.create("cadc:FOO/foo"), DataLinkSemantics.THIS, ReleaseType.DATA);
        p.getArtifacts().add(a);

        deleteObservation(observation.getURI(), subject1, null, null);
        
        final URI accMetaChecksum1 = observation.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
        log.info("PUT " + observation.getURI() + " aka " + observation.getID() + " as " + contentType);
        sendObservation("PUT", contentType, observation, subject1, 200, "OK", null, null);
        
        // GET
        Observation ret = getObservation(observation.getURI(), contentType, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertEquals("wrong URI", observation.getURI(), ret.getURI());
        Assert.assertEquals("wrong ID", observation.getID(), ret.getID());
        Assert.assertEquals("wrong checksum", accMetaChecksum1, ret.getAccMetaChecksum());

        // HEAD done after so we have the server side checksum and timestamp
        String surl = baseObsURL + "/" + uriToPath(ret.getURI());
        URL url = new URL(surl);
        log.info("HEAD: " + surl);
        final HttpGet head = new HttpGet(url, false);
        head.setHeadOnly(true);
        Subject.doAs(subject1, new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                head.prepare();
                return null;
            }
        });
        Assert.assertEquals(200, head.getResponseCode());
        Assert.assertEquals(ret.getID().toString(), head.getResponseHeader("x-entity-id"));
        Assert.assertEquals(ret.getAccMetaChecksum().toString(), head.getResponseHeader("etag"));
        // HTTP date format has no milliseconds so we need to compare strings
        DateFormat df = DateUtil.getDateFormat(DateUtil.HTTP_DATE_FORMAT, DateUtil.GMT);
        String maxLastMod = df.format(ret.getMaxLastModified());
        Assert.assertEquals(maxLastMod, head.getResponseHeader("last-modified"));
        
        observation.telescope = new Telescope("FOOSCOPE");
        final URI accMetaChecksum2 = observation.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
        Assert.assertNotEquals(accMetaChecksum1, accMetaChecksum2);
        
        log.info("POST " + observation.getURI() + " as " + contentType);
        sendObservation("POST", contentType, observation, subject1, 200, "OK", null, null);
        ret = getObservation(observation.getURI(), contentType, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertEquals("wrong URI", observation.getURI(), ret.getURI());
        Assert.assertEquals("wrong ID", observation.getID(), ret.getID());
        Assert.assertEquals("wrong checksum", accMetaChecksum2, ret.getAccMetaChecksum());
        
        // DELETE
        deleteObservation(observation.getURI(), subject1, 200, null);
        final HttpGet headNotFound = new HttpGet(url, false);
        headNotFound.setHeadOnly(true);
        try {
            Subject.doAs(subject1, new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    headNotFound.prepare();
                    return null;
                }
            });
        } catch (PrivilegedActionException pev) {
            Exception cause = pev.getException();
            Assert.assertTrue(cause instanceof ResourceNotFoundException);
            log.info("caught expected: " + cause);
        }
        
        // TEMPORARY HACK: repeat the PUT with the same Entity.id to make sure the
        // DeletedObservationEvent was removed and doesn't block subsequent PUT
        log.info("PUT " + observation.getURI() + " aka " + observation.getID() + " as " + contentType);
        sendObservation("PUT", contentType, observation, subject1, 200, "OK", null, null);
    }
    
    @Test
    public void testPutGetUpdateIVO() throws Exception {
        
        
        // create an observation using subject1
        String ivoTestCollection = "IVOTEST";
        URI ouri = URI.create("ivo://opencadc.org/" + ivoTestCollection + "?testCleanPutGetSuccessIVO");
        Observation observation = new SimpleObservation(ivoTestCollection, ouri, SimpleObservation.EXPOSURE);
        Plane p = new Plane(URI.create(observation.getURI().toASCIIString() + "/raw"));
        observation.getPlanes().add(p);

        Artifact a = new Artifact(URI.create("cadc:FOO/foo"), DataLinkSemantics.THIS, ReleaseType.DATA);
        p.getArtifacts().add(a);

        deleteObservation(observation.getURI(), subject1, null, null);
        
        final URI accMetaChecksum1 = observation.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));

        putObservation(observation, subject1, 200, "OK");

        Observation ret = getObservation(observation.getURI(), DEFAULT_CONTENT_TYPE, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertEquals("wrong observation", observation.getURI(), ret.getURI());
        Assert.assertEquals("wrong observation", observation.getID(), ret.getID());
        Assert.assertEquals("wrong checksum", accMetaChecksum1, ret.getAccMetaChecksum());

        observation.telescope = new Telescope("FOOSCOPE");
        final URI accMetaChecksum2 = observation.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
        Assert.assertNotEquals(accMetaChecksum1, accMetaChecksum2);
        
        updateObservation(observation, subject1, 200, "OK");
        ret = getObservation(observation.getURI(), DEFAULT_CONTENT_TYPE, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertEquals("wrong URI", observation.getURI(), ret.getURI());
        Assert.assertEquals("wrong ID", observation.getID(), ret.getID());
        Assert.assertEquals("wrong checksum", accMetaChecksum2, ret.getAccMetaChecksum());

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }
    
    @Test
    public void testConditonalUpdate() throws Throwable {
        
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        
        Observation orig = new SimpleObservation(TEST_COLLECTION, "testConditionalUpdate");
        final URI origAcc = orig.computeAccMetaChecksum(md5);
        deleteObservation(orig.getURI(), subject1, null, null);
         
        log.info("put: orig");
        sendObservation("PUT", orig, subject1, 200, "OK", null);

        // get the observation using subject2
        log.info("get: o1");
        Observation o1 = getObservation(orig.getURI(), DEFAULT_CONTENT_TYPE, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertNotNull(o1);
        final URI acc1 = o1.computeAccMetaChecksum(md5);
        
        // modify
        orig.intent = ObservationIntentType.SCIENCE;
        
        // update
        log.info("update: orig");
        sendObservation("POST", RepoAction.CAOM_XML_MIMETYPE, orig, subject1, 200, "OK", null, acc1.toASCIIString());
        
        // get and verify
        log.info("get: updated");
        Observation o2 = getObservation(orig.getURI(), DEFAULT_CONTENT_TYPE, subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertNotNull(o2);
        final URI acc2 = o2.computeAccMetaChecksum(md5);
        
        // attempt second update from orig: rejected
        log.info("update: orig [race loser]");
        sendObservation("POST", RepoAction.CAOM_XML_MIMETYPE, orig, subject1, 412, "update blocked", null, origAcc.toASCIIString());
        
        // attempt update from current: accepted
        log.info("update: o2");
        sendObservation("POST", RepoAction.CAOM_XML_MIMETYPE, orig, subject1, 200, "OK", null, acc2.toASCIIString());
        
        // cleanup (ok to fail)
        deleteObservation(orig.getURI(), subject1, null, null);
    }

    @Test
    public void testGetNoReadPermission() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testGetNoReadPermission"), SimpleObservation.EXPOSURE);
        
        deleteObservation(observation.getURI(), subject1, null, null);
        
        putObservation(observation, subject1, 200, "OK");

        // get the observation using subject3
        getObservation(observation.getURI(), DEFAULT_CONTENT_TYPE, subject3, 403, "permission denied", false, EXPECTED_CAOM_VERSION);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }
    
    @Test
    public void testGetAnonPublic() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testGetAnonPublic"), SimpleObservation.EXPOSURE);
        observation.metaRelease = new Date(System.currentTimeMillis() - 10000L); // 10 sec ago

        deleteObservation(observation.getURI(), subject1, null, null);
        
        putObservation(observation, subject1, 200, "OK");

        Subject anon = null; // clear intent
        getObservation(observation.getURI(), DEFAULT_CONTENT_TYPE, anon, 200, null, EXPECTED_CAOM_VERSION);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testGetNotFound() throws Exception {
        URI uri = URI.create("caom:TEST/testGetNotFound");

        getObservation(uri, DEFAULT_CONTENT_TYPE, subject1, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testCollectionNotFound() throws Exception {
        URI uri = URI.create("caom:testCollectionNotFound/testGetNotFound");
        getObservation(uri, DEFAULT_CONTENT_TYPE, subject2, 404, "not found: testCollectionNotFound", EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testPutNoWritePermission() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPutNoWritePermission"), SimpleObservation.EXPOSURE);
        putObservation(observation, subject2, 403, "permission denied");
    }

    @Test
    public void testPutByteLimitExceeded() throws Exception {
        // create an observation using subject1
        Observation observation = createVeryLargeObservation(TEST_COLLECTION, "testPutByteLimitExceeded");
        putObservation(observation, subject1, 413, null);
    }

    @Test
    public void testPutURIsDontMatch() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPutURIsDontMatch"), SimpleObservation.EXPOSURE);
        String suri = observation.getURI().toASCIIString() + "-alt";
        String altPath = observation.getURI().getSchemeSpecificPart() + "-alt";
        putObservationCompat(observation, subject1, 400, "invalid input: " + suri , altPath);
    }

    @Test
    public void testPutAlreadyExists() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPutAlreadyExists"), SimpleObservation.EXPOSURE);
        putObservation(observation, subject1, null, null);

        // TODO: reconsider PUT to replace where IDs and URIs match??
        
        // create it again to get different UUID
        Observation dupe = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPutAlreadyExists"), SimpleObservation.EXPOSURE);
        putObservation(dupe, subject1, 409, "already exists: " + observation.getURI().toASCIIString());

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testPutValidationFails() throws Exception {
        String observationID = generateID("testPutValidationFails");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        // create an observation using subject1
        Observation observation = createInvalidObservation(TEST_COLLECTION, observationID);
        putObservation(observation, subject1, 400, "invalid input: " + uri);
    }

    @Test
    public void testUpdatePermissionDenied() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testUpdatePermissionDenied"), SimpleObservation.EXPOSURE);
        
        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
        
        putObservation(observation, subject1, 200, "OK");

        // overwrite the observation with a post
        postObservation(observation, subject2, 403, "permission denied", null);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testPostByteLimitExceeded() {
        try {
            String observationID = generateID("testPostByteLimitExceeded");

            // (byte limit exception will happen before 'doesnt exist'
            // exception)

            // create an observation using subject1
            Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
            postObservation(observation, subject1, 413, null, null);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPostURIsDontMatch() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostURIsDontMatch"), SimpleObservation.EXPOSURE);

        // delete any previous run (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);

        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK");

        // post an observation using subject1 but with a different path on the url
        String suri = observation.getURI().toASCIIString() + "-alt";
        String altPath = observation.getURI().getSchemeSpecificPart() + "-alt";
        postObservation(observation, subject1, 400, "invalid input: " + suri, altPath);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testPostNotFound() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostNotFound"), SimpleObservation.EXPOSURE);
        deleteObservation(observation.getURI(), subject1, null, null);
        
        postObservation(observation, subject1, 404, "not found: " + observation.getURI(), null);
    }

    @Test
    public void testPostValidationFails() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostValidationFails"), SimpleObservation.EXPOSURE);
        putObservation(observation, subject1, null, null);

        // make invalid
        observation.instrument = new Instrument("INSTR");
        observation.instrument.getKeywords().add("FOO|BAR"); // reserved character

        // try to update 
        postObservation(observation, subject1, 400, "invalid input: " + observation.getURI(), null);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testDeleteSuccess() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostValidationFails"), SimpleObservation.EXPOSURE);

        // delete any previous run (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);

        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK");

        // delete the observation
        deleteObservation(observation.getURI(), subject1, 200, "OK");

        // ensure we can't find it on a get
        getObservation(observation.getURI(), DEFAULT_CONTENT_TYPE, subject1, 404, "not found: " + observation.getURI(), EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testDeleteNoWritePermission() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostValidationFails"), SimpleObservation.EXPOSURE);

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
        
        // create an observation using subject1
        putObservation(observation, subject1, 200, "OK");

        // delete the observation using subject 2
        deleteObservation(observation.getURI(), subject2, 403, "permission denied");

        // cleanup (ok to fail)
        deleteObservation(observation.getURI(), subject1, null, null);
    }

    @Test
    public void testDeleteNotFound() throws Exception {
        URI uri = URI.create("caom:" + TEST_COLLECTION + "/testDeleteNotFound");

        // delete the non-existent observation
        deleteObservation(uri, subject1, 404, "not found: " + uri);
    }

    // TODO: figure out what use case this is for... currently fails with 404
    //@Test
    public void testPostMultipartSingleParamSuccess() {
        try {
            Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostMultipartSingleParamSuccess"), SimpleObservation.EXPOSURE);
            Map<String, Object> params = new HashMap<>();
            params.put("file", convertToFile(observation));

            testPostMultipartWithParamsSuccess(observation.getURI(), params);
        } catch (Exception unexpected) {
            log.error("unexpected", unexpected);
            Assert.fail("unexpected: " + unexpected);
        }
    }

    // TODO: figure out what use case this is for... currently fails with 404
    //@Test
    public void testPostMultipartMultipleParamSuccess() throws Exception {
        Observation observation = new SimpleObservation(TEST_COLLECTION, 
                URI.create("caom:" + TEST_COLLECTION + "/testPostMultipartMultipleParamSuccess"), SimpleObservation.EXPOSURE);
        Map<String, Object> params = new HashMap<>();
        params.put("fooKey", "fooValue");
        params.put("file", convertToFile(observation));
        params.put("barKey", "barValue");

        testPostMultipartWithParamsSuccess(observation.getURI(), params);
    }

    private File convertToFile(Observation observation) throws IOException {
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

    private void testPostMultipartWithParamsSuccess(URI uri, final Map<String, Object> params)
        throws Exception {
        String path = uri.getSchemeSpecificPart();
        final URL url = new URL(baseObsURL + "/" + path);

        PrivilegedExceptionAction<Object> p = new PrivilegedExceptionAction<>() {
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
