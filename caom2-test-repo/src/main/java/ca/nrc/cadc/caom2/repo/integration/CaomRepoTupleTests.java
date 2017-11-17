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
 *
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo.integration;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.reg.Standards;

import java.net.URI;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

/**
 * Integration tests to ensure no exceptions are thrown in the process of tuple generation for seemingly invalid dates.
 * Note - No content checking is done for tuple generation.
 * <p/>
 * This class is intended to be extended by concrete service implementations.  It exists here solely to provide a
 * standard by which this library holds to.
 * <p/>
 * jenkinsd 2017.11.15
 */
public abstract class CaomRepoTupleTests extends CaomRepoBaseIntTests {
    static final String TEST_PRODUCT_ID_PREFIX = "TEST_PRODUCT_ID";


    public CaomRepoTupleTests() {
    }

    /**
     * Default constructor.
     *
     * @param resourceID resource identifier of service to test
     * @param pem1       PEM file for user with read-write permission
     * @param pem2       PEM file for user with read-only permission
     * @param pem3       PEM file for user with no permissions
     */
    public CaomRepoTupleTests(URI resourceID, String pem1, String pem2, String pem3) {
        super(resourceID, Standards.CAOM2REPO_OBS_23, pem1, pem2, pem3);
    }

    private void ensurePutAndDelete(final Observation observation, final String uri) throws Exception {
        putObservation(observation, subject1, 200, "OK", null);

        // cleanup (ok to fail)
        deleteObservation(uri, subject1, null, null);
    }

    @Test
    public void testNullDates() throws Exception {
        final String observationID = generateID("testNullDates");
        final String path = TEST_COLLECTION + "/" + observationID;
        final String productID = generateID(TEST_PRODUCT_ID_PREFIX);
        final String uri = SCHEME + path;

        final Observation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        final Plane plane = new Plane(productID);

        // Test 1
        // Null data release and null meta release.
        plane.dataRelease = null;
        plane.metaRelease = null;

        observation.getPlanes().add(plane);

        ensurePutAndDelete(observation, uri);

        // TEST 2
        // Null data release and set meta release to now.
        plane.metaRelease = new Date();
        ensurePutAndDelete(observation, uri);

        // TEST 3
        // Set data release to now and null meta release.
        plane.dataRelease = new Date();
        plane.metaRelease = null;
        ensurePutAndDelete(observation, uri);
    }

    @Test
    public void testPastDates() throws Exception {
        final Calendar pastCal = Calendar.getInstance();

        // November 25th, 1977.
        pastCal.set(1977, Calendar.NOVEMBER, 25, 1, 51, 0);
        pastCal.set(Calendar.MILLISECOND, 0);

        final String observationID = generateID("testPastDates");
        final String path = TEST_COLLECTION + "/" + observationID;
        final String productID = generateID(TEST_PRODUCT_ID_PREFIX);
        final String uri = SCHEME + path;

        final Observation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        final Plane plane = new Plane(productID);

        // Test 1
        // Past data release and current meta release.
        plane.dataRelease = pastCal.getTime();
        plane.metaRelease = new Date();

        observation.getPlanes().add(plane);

        ensurePutAndDelete(observation, uri);

        // TEST 2
        // Current data release and set meta release to past.
        plane.dataRelease = new Date();
        plane.metaRelease = pastCal.getTime();
        ensurePutAndDelete(observation, uri);

        // TEST 3
        // Set data release to past and meta release to past.
        plane.dataRelease = pastCal.getTime();
        plane.metaRelease = pastCal.getTime();
        ensurePutAndDelete(observation, uri);
    }

    @Test
    public void testFutureDates() throws Exception {
        final Calendar futureCal = Calendar.getInstance();

        // Thirty years, nice round number.
        futureCal.add(Calendar.YEAR, 30);

        final String observationID = generateID("testFutureDates");
        final String path = TEST_COLLECTION + "/" + observationID;
        final String productID = generateID(TEST_PRODUCT_ID_PREFIX);
        final String uri = SCHEME + path;

        final Observation observation = new SimpleObservation(TEST_COLLECTION, observationID);

        final Plane plane = new Plane(productID);

        // Test 1
        // Future data release and current meta release.
        plane.dataRelease = futureCal.getTime();
        plane.metaRelease = new Date();

        observation.getPlanes().add(plane);

        ensurePutAndDelete(observation, uri);

        // TEST 2
        // Current data release and set meta release to future.
        plane.dataRelease = new Date();
        plane.metaRelease = futureCal.getTime();
        ensurePutAndDelete(observation, uri);

        // TEST 3
        // Set data release to future and meta release to future.
        plane.dataRelease = futureCal.getTime();
        plane.metaRelease = futureCal.getTime();
        ensurePutAndDelete(observation, uri);
    }
}
