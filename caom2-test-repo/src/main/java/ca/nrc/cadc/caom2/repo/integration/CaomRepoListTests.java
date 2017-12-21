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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


/**
 * Integration tests for caom2repo_ws
 *
 * @author majorb
 */
public class CaomRepoListTests extends CaomRepoBaseIntTests {

    private static final Logger log = Logger.getLogger(CaomRepoListTests.class);

    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_3_NAMESPACE;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    private CaomRepoListTests() {
    }

    /**
     * @param resourceID resource identifier of service to test
     * @param pem1       PEM file for user with read-write permission
     * @param pem2       PEM file for user with read-only permission
     * @param pem3       PEM file for user with no permissions
     */
    public CaomRepoListTests(URI resourceID, String pem1, String pem2, String pem3) {
        super(resourceID, Standards.CAOM2REPO_OBS_23, pem1, pem2, pem3);
    }

    @Test
    public void testListNoReadPermission() throws Throwable {
        Date start = new Date(System.currentTimeMillis());

        // Add a list of observations
        List<String> baseIDs = new ArrayList<>(Arrays.asList("testListNoReadPermission1", "testListNoReadPermission2",
            "testListNoReadPermission3"));
        List<Observation> observations = this.putObservations(baseIDs);

        int maxRec = 10;

        // Check that we have no permission to list the observations
        checkObservationList(0, SCHEME + TEST_COLLECTION, maxRec, start, null,
            subject3, null, null, 403, "permission denied: ", false);

        // cleanup (ok to fail)
        for (Observation obs : observations) {
            deleteObservation(obs.getURI().toString(), subject1, null, null);
        }
    }

    /**
     * TODO - Can the try/catch be removed?  It's completely useless.
     * TODO - jenkinsd 2017.10.03
     *
     * @throws Throwable Any errors
     */
    @Test
    public void testListSuccess() throws Throwable {
        try {
            // Add a list of observations
            List<String> baseIDs = new ArrayList<>(Arrays.asList("testListSuccess1", "testListSuccess2", "testListSuccess3"));
            List<Observation> observations = this.putObservations(baseIDs);
            Assert.assertTrue("failed to put observations", observations.size() == 3);
            Assert.assertNotNull("failed to get first observation maxLastModified date",
                observations.get(0).getLastModified());
            Assert.assertNotNull("failed to get first observation maxLastModified date",
                observations.get(1).getLastModified());
            Assert.assertNotNull("failed to get first observation maxLastModified date",
                observations.get(2).getLastModified());
            final Date start = getTime(observations.get(0).getLastModified());
            final Date mid = getTime(observations.get(1).getLastModified());
            final Date end = getTime(observations.get(2).getLastModified());
            Integer maxRec = 3;

            // Check that we have maxRec of the observations, use default (ascending) order
            checkObservationList(baseIDs.size(), SCHEME + TEST_COLLECTION, maxRec, start,
                null, subject2, null, observations, 200, null, true);

            observations.remove(0);
            // Check that we only have the last two observations
            checkObservationList((baseIDs.size() - 1), SCHEME + TEST_COLLECTION, maxRec, mid,
                null, subject2, null, observations, 200, null, true);

            
            observations.remove(0);
            // Check that we only have the last observation
            checkObservationList((baseIDs.size() - 2), SCHEME + TEST_COLLECTION, maxRec, end,
                null, subject2, null, observations, 200, null, true);

            // cleanup (ok to fail)
            for (Observation obs : observations) {
                deleteObservation(obs.getURI().toString(), subject1, null, null);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testListLessThanMaxRecSuccess() throws Throwable {
        // Add a list of observations
        List<String> baseIDs = new ArrayList<>(Arrays.asList("testListLessThanMaxRecSuccess1",
            "testListLessThanMaxRecSuccess2", "testListLessThanMaxRecSuccess3"));
        List<Observation> observations = this.putObservations(baseIDs);
        Assert.assertTrue("failed to put observations", observations.size() == 3);
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(0).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(1).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(2).getLastModified());
        Date start = getTime(observations.get(0).getLastModified());
        Date end = null;
        Integer maxRec = 10;

        // Check that we only have 3 observations, use ascending order
        checkObservationList(baseIDs.size(), SCHEME + TEST_COLLECTION, maxRec, start,
            end, subject2, "asc", observations, 200, null, true);

        // cleanup (ok to fail)
        for (Observation obs : observations) {
            deleteObservation(obs.getURI().toString(), subject1, null, null);
        }
    }

    @Test
    public void testListMoreThanMaxRecSuccess() throws Throwable {
        // Add a list of observations
        List<String> baseIDs = new ArrayList<>(Arrays.asList("testListMoreThanMaxRecSuccess1",
            "testListMoreThanMaxRecSuccess2", "testListMoreThanMaxRecSuccess3"));
        List<Observation> observations = this.putObservations(baseIDs);
        Assert.assertTrue("failed to put observations", observations.size() == 3);
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(0).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(1).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(2).getLastModified());
        Date start = getTime(observations.get(0).getLastModified());
        Integer maxRec = 2;

        // Check that we only have 2 observations, use descending order
        checkObservationList(maxRec, SCHEME + TEST_COLLECTION, maxRec, start,
            null, subject2, "desc", observations, 200, null, true);

        // cleanup (ok to fail)
        for (Observation obs : observations) {
            deleteObservation(obs.getURI().toString(), subject1, null, null);
        }
    }

    @Test
    public void testListAllSuccess() throws Throwable {
        // Add a list of observations
        List<String> baseIDs = new ArrayList<>(Arrays.asList("testListAllSuccess1",
            "testListAllSuccess2", "testListAllSuccess3"));
        List<Observation> observations = this.putObservations(baseIDs);
        Assert.assertTrue("failed to put observations", observations.size() == 3);
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(0).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(1).getLastModified());
        Assert.assertNotNull("failed to get first observation maxLastModified date",
            observations.get(2).getLastModified());
        Date start = null;
        Date end = null;
        Integer maxRec = null;

        // Check that we have all of the observations, use default (ascending) order
        checkObservationList(null, SCHEME + TEST_COLLECTION, maxRec, start,
            end, subject2, null, observations, 200, null, true);

        // cleanup (ok to fail)
        for (Observation obs : observations) {
            deleteObservation(obs.getURI().toString(), subject1, null, null);
        }
    }

    @Test
    public void testListCollections() throws Throwable {
        try {
            listCollections(false, subject1);
            listCollections(true, null);
        } catch (Throwable t) {
            log.error("unexpected", t);
            Assert.fail();
        }
    }


    private List<Observation> putObservations(final List<String> baseIDs) throws Throwable {
        List<Observation> retObs = new ArrayList<>();
        for (String baseID : baseIDs) {
            String observationID = generateID(baseID);

            // create an observation using subject1
            SimpleObservation observation = new SimpleObservation(
                TEST_COLLECTION, observationID);
            Plane p = new Plane("foo");
            observation.getPlanes().add(p);

            Artifact a = new Artifact(URI.create("ad:FOO/foo"),
                ProductType.SCIENCE, ReleaseType.DATA);
            p.getArtifacts().add(a);

            Part pa = new Part(0);
            Chunk ch = new Chunk();
            ch.naxis = 0;
            pa.getChunks().add(ch);
            a.getParts().add(pa);
            putObservation(observation, subject1, 200, "OK", null);

            // verify the observation using subject2
            Observation ret = getObservation(observation.getURI().toString(), subject2, 200, null, EXPECTED_CAOM_VERSION);
            Assert.assertEquals("wrong observation", observation, ret);
            retObs.add(ret);

            // separate the puts by 10 ms so that we can pick out each observations easier
            TimeUnit.MILLISECONDS.sleep(10);
        }

        return retObs;
    }

    private URL buildURL(String uri, Integer maxRec, Date start, Date end, Subject subject, String order)
        throws URISyntaxException, MalformedURLException {
        // extract the path from the uri
        URI ouri = new URI(uri);
        String surl = baseHTTPURL + "/" + ouri.getSchemeSpecificPart();
        if (subject != null) {
            surl = baseHTTPSURL + "/" + ouri.getSchemeSpecificPart();
        }

        if (maxRec != null) {
            surl = surl + "?maxRec=" + maxRec;
        }
        if (start != null) {
            surl = surl + "&start=" + df.format(start);
        }
        if (end != null) {
            surl = surl + "&end=" + df.format(end);
        }
        if (order != null) {
            surl = surl + "&order=" + order;
        }

        return new URL(surl);
    }

    private Map<String, Date> listObservationIDs(String uri, Integer maxRec,
                                                 Date start, Date end, Subject subject, String order, List<Observation> observations,
                                                 Integer expectedResponse, String expectedMessage, boolean exactMatch) throws Exception {
        log.debug("start list on " + uri);

        Map<String, Date> retMap = new Hashtable<String, Date>();
        URL url = this.buildURL(uri, maxRec, start, end, subject, order);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpDownload get = new HttpDownload(url, bos);
        Subject.doAs(subject, new RunnableAction(get));

        int response = get.getResponseCode();

        if (expectedResponse != null) {
            Assert.assertEquals("wrong response", expectedResponse.intValue(), response);
        }

        if (expectedMessage != null) {
            String message = bos.toString().trim();
            Assert.assertNotNull(message);
            if (exactMatch) {
                Assert.assertEquals("Wrong response message", expectedMessage, message);
            } else {
                Assert.assertTrue("Wrong response message (startsWith)", message.startsWith(expectedMessage));
            }
        } else if (response == 200 && observations != null && observations.size() > 0) {
            long matchSize = observations.size();
            if (maxRec != null && observations.size() > maxRec) {
                matchSize = Long.valueOf(maxRec);
            }

            String message = bos.toString().trim();
            Assert.assertNotNull(message);
            String[] lines = message.split("\\r?\\n");
            if (start == null) {
                Assert.assertTrue("too few observation states", lines.length >= matchSize);
            } else {
                Assert.assertEquals("wrong number of observation states", matchSize, lines.length);
            }

            boolean revert = false;
            if ((order != null) && (order.equals("desc"))) {
                revert = true;
            }

            for (int i = 0; i < lines.length; i++) {
                String[] fields = lines[i].split("\t");
                String actualDate = fields[2];
                int j = i;

                if (start != null) {
                    if (revert) {
                        j = observations.size() - 1 - i;
                    }

                    Observation obs = observations.get(j);
                    String expectedDate = df.format(obs.getLastModified());
                    Assert.assertEquals("wrong date", expectedDate, actualDate);
                }

                retMap.put(fields[1], df.parse(actualDate));
            }
        }

        return retMap;
    }

    private void checkObservationList(final Integer expectedSize,
                                      final String collection, final Integer maxRec, final Date start,
                                      final Date end, Subject subject, String order,
                                      List<Observation> observations, Integer expectedCode,
                                      String expectedMessage, boolean exactMatch) throws Throwable {
        Map<String, Date> observationIDMap = listObservationIDs(
            SCHEME + TEST_COLLECTION, maxRec, start, end, subject, order,
            observations, expectedCode, expectedMessage, exactMatch);

        // expectedSize == null means no size limits
        if (expectedSize == null) {
            Assert.assertTrue("wrong number of observationIDs", observationIDMap.size() > 0);
        } else {
            Assert.assertEquals("wrong number of observationIDs",
                expectedSize.intValue(), observationIDMap.size());
        }

        if (observationIDMap.size() > 0) {
            Collection<Date> dates = observationIDMap.values();
            for (Date date : dates) {
                if (start != null) {
                    // start date should not be after the date of an observationID
                    Assert.assertTrue("wrong timestamp", start.compareTo(date) <= 0);
                }

                if (end != null) {
                    // end date should not be before the date of an observationID
                    Assert.assertTrue("wrong timestamp", end.compareTo(date) >= 0);
                }
            }
        }
    }

    private List<String> listCollections(boolean testAnon, Subject subject) throws Exception {
        List<String> retList = new ArrayList<>();

        String surl = baseHTTPURL;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        if (!testAnon) {
            surl = baseHTTPSURL;
        }

        URL url = new URL(surl);
        HttpDownload get = new HttpDownload(url, bos);


        if (!testAnon) {
            Subject.doAs(subject, new RunnableAction(get));
        } else {
            get.run();
        }

        int response = get.getResponseCode();

        log.debug("response code: " + response);

        // Check content type & reponse code. Can't count on content or size of list returned.
        Assert.assertTrue("bad content returned", get.getContentType().equals("text/tab-separated-values"));
        Assert.assertTrue("bad reponse", response == 200);

        // read return, parse collection list
        String message = bos.toString().trim();
        Assert.assertNotNull(message);
        String[] lines = message.split("\\r?\\n");

        log.debug("lines returned: " + lines.length);

        for (int i = 0; i < lines.length; i++) {
            log.debug("line " + i + ": " + lines[i]);
            retList.add(lines[i]);
        }

        return retList;
    }


    private Date getTime(Date time) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(time);
        cal.add(Calendar.MILLISECOND, -5);
        return cal.getTime();
    }

}
