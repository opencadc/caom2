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
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;

/**
 * Integration tests for caom2repo_ws
 *
 * @author majorb
 *
 */
public class CaomRepoIntTests extends CaomRepoBaseIntTests
{

    private static final Logger log = Logger.getLogger(CaomRepoIntTests.class);

    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_3_NAMESPACE;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    private CaomRepoIntTests() { }

    /**
     * @param resourceID resource identifier of service to test
     * @param pem1 PEM file for user with read-write permission
     * @param pem2 PEM file for user with read-only permission
     * @param pem3 PEM file for user with no permissions
     */
    public CaomRepoIntTests(URI resourceID, String pem1, String pem2, String pem3)
    {
        super(resourceID, Standards.CAOM2REPO_OBS_23, pem1, pem2, pem3);
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
        Observation ret = getObservation(uri, SUBJECT2, 200, null, EXPECTED_CAOM_VERSION);
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
        getObservation(uri, SUBJECT3, 403, "permission denied: " + uri, EXPECTED_CAOM_VERSION);

        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
    }

    @Test
    public void testGetNotFound() throws Throwable
    {
        String observationID = generateObservationID("testGetNotFound");
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;

        getObservation(uri, SUBJECT2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testCollectionNotFound() throws Throwable
    {
        String collection = "NoSuchCollection";
        String observationID = generateObservationID("testCollectionNotFound");
        String path =  collection + "/" + observationID;
        String uri = SCHEME + path;

        getObservation(uri, SUBJECT2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
    }

    @Test
    public void testInvalidURI() throws Throwable
    {
        String collection = TEST_COLLECTION;
        String observationID = generateObservationID("testInvalidURI");
        String path =  collection + "/" + observationID + "/extraElementsInPath";
        String uri = SCHEME + path;

        super.getObservation(uri, SUBJECT2, 400, "invalid input: " + uri, false, EXPECTED_CAOM_VERSION);
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
    public void testPutByteLimitExceeded()
    {
        try
        {
            String observationID = generateObservationID("testPutByteLimitExceeded");
            String path = TEST_COLLECTION + "/" + observationID;
            String uri = SCHEME + path;

            // create an observation using subject1
            Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
            putObservation(observation, SUBJECT1, 413, "too large:", null);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
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
    public void testPostByteLimitExceeded()
    {
        try
        {
            String observationID = generateObservationID("testPostByteLimitExceeded");
            String path = TEST_COLLECTION + "/" + observationID;
            String uri = SCHEME + path;

            // (byte limit exception will happen before 'doesnt exist'
            // exception)

            // create an observation using subject1
            Observation observation = createVeryLargeObservation(TEST_COLLECTION, observationID);
            postObservation(observation, SUBJECT1, 413, "too large:", null);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
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
        getObservation(uri, SUBJECT2, 404, "not found: " + uri, EXPECTED_CAOM_VERSION);
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

    @Test
    public void testPostMultipartSingleParamSuccess()
    {
        try
        {
            String observationID = generateObservationID("testPostMultipartSingleParamSuccess");
            final SimpleObservation observation = this.generateObservation(observationID);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("file", convertToFile(observation));

            testPostMultipartWithParamsSuccess(observationID, params);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected", unexpected);
            Assert.fail("unexpected: " + unexpected);
        }
    }

    @Test
    public void testPostMultipartMultipleParamSuccess() throws Throwable
    {
	    String observationID = generateObservationID("testPostMultipartMultipleParamSuccess");
        final SimpleObservation observation = this.generateObservation(observationID);
		Map<String, Object> params = new HashMap<String, Object>();
   	    params.put("fooKey", "fooValue");
   	    params.put("file", convertToFile(observation));
   	    params.put("barKey", "barValue");

	    testPostMultipartWithParamsSuccess(observationID, params);
    }

    private long DOCUMENT_SIZE_MAX = (long) 20*1024*1024; // 20MB limit in caom2-repo-server
    private String KW_STR = "abcdefghijklmnopqrstuvwxyz0123456789";

    private SimpleObservation generateObservation(String observationID) throws Exception
    {
        // create an observation using subject1
        final SimpleObservation observation = new SimpleObservation(TEST_COLLECTION, observationID);
        Plane plane = new Plane("foo");
        plane.calibrationLevel = CalibrationLevel.RAW_STANDARD;
        observation.getPlanes().add(plane);

        putObservation(observation, SUBJECT1, 200, "OK", null);

        // modify the plane since that also tweaks the Observation.maxLastModified
        plane.dataProductType = DataProductType.CUBE;
        return observation;
    }

    private File convertToFile(SimpleObservation observation) throws IOException
    {
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
            throws Exception
	{
        String path = TEST_COLLECTION + "/" + observationID;
        String uri = SCHEME + path;
        final URL url = new URL(BASE_HTTPS_URL + "/" + path);

        PrivilegedExceptionAction<Object> p = new PrivilegedExceptionAction<Object>()
        {
            @Override
            public Object run() throws Exception
            {
        	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        	    HttpPost httpPost = new HttpPost(url, params, outputStream);
        	    httpPost.setMaxRetries(4);
        	    httpPost.run();
        	    log.debug("throwable: " + httpPost.getThrowable());
        	    Assert.assertNull("Wrong throwable", httpPost.getThrowable());
                return null;
            }
        };
        Subject.doAs(SUBJECT1, p);

        // cleanup (ok to fail)
        deleteObservation(uri, SUBJECT1, null, null);
	}

    private Observation createVeryLargeObservation(String collection, String observationID)
    {
        SimpleObservation observation = new SimpleObservation(collection, observationID);
        observation.instrument = new Instrument("FOO");
        long num = (long) (1.5 * DOCUMENT_SIZE_MAX)/KW_STR.length();
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
            ComputeUtil.computeTransientState(observation, plane);
            throw new IllegalStateException("BUG: Test setup - observation not invalid.");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        return observation;
    }

    private void postObservation(final Observation observation, final Subject subject, Integer expectedResponse, String expectedMessage, String path)
            throws Exception
    {
        super.sendObservation("POST", observation, subject, expectedResponse, expectedMessage, path);
    }
}
