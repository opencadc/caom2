/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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
 ************************************************************************
 */

package org.opencadc.caom2.download;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.util.BucketSelector;
import ca.nrc.cadc.util.Log4jInit;

import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class FileSyncTest extends AbstractFileSyncTest {
    private static final Logger log = Logger.getLogger(FileSyncTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.caom2.download", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    public FileSyncTest() throws Exception {
        super();
    }

    private List<Artifact> makeSmallDataset() {
        log.info("making 4 element dataset");
        // set up 4 IRIS artifacts
        List<Artifact> artifacts = new ArrayList<>();
        artifacts.add(makeArtifact("cadc:IRIS/I429B4H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/f429h000_preview_1024.png", null, 471897L));
        artifacts.add(makeArtifact("cadc:IRIS/I426B4H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/f426h000_preview_256.png", null, 113116L));
        return artifacts;
    }

    private List<Artifact> makeLargeDataset() {
        log.info("making 16 element dataset");
        // Used in the different test suites to pass to the function that populates the test database.
        // generate a 4 item list
        List<Artifact> artifacts = makeSmallDataset();
        // Build on the small dataset
        artifacts.add(makeArtifact("cadc:IRIS/I422B4H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I422B1H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I422B2H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I422B3H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I421B4H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I421B1H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I421B2H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I421B3H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I420B4H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I420B1H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I420B2H0.fits", null, 1008000L));
        artifacts.add(makeArtifact("cadc:IRIS/I420B3H0.fits", null, 1008000L));
        log.info("done making 16 element dataset");
        return artifacts;
    }

    private void createTestMetadata(List<Artifact> artifacts) {
        for(Artifact artifact : artifacts) {
            Observation observation = makeObservation(artifact);
            observationDAO.put(observation);
            log.debug("put test Artifact " + artifact.getURI());
            HarvestSkipURI harvestSkipURI = makeHarvestSkipURI(artifact);
            harvestSkipURIDAO.put(harvestSkipURI);
            log.debug("put test HarvestSkipURI " + harvestSkipURI.getSkipID());
        }
    }

    public void fileSyncTestBody(List<Artifact> artifacts, int threads)
        throws Exception {

        //File certificateFile = FileUtil.getFileFromResource(TestUtil.CERTIFICATE_FILE, FileSyncJobTest.class);
        //Subject subject = SSLUtil.createSubject(certificateFile);
        final Subject subject = AuthenticationUtil.getAnonSubject();

        // instantiate ArtifactStore when it's config is in place.
        final ArtifactStore artifactStore = TestUtil.getArtifactStore();

        createTestMetadata(artifacts);
        log.info("test metadata put to database");
        log.debug("threads: " + threads);

        // bucket to cover all ranges
        String namespace = null; // all
        BucketSelector buckets = new BucketSelector("0-f");
        log.debug("buckets: " + buckets);

        log.info("FileSync: START");
        FileSync fs = new FileSync(daoConfig, cc, artifactStore, namespace, buckets, threads, 2);
        fs.testRunLoops = 1;
        fs.doit(subject);
        log.info("FileSync: DONE");


        // Check the artifact store for artifacts.
        for (Artifact artifact : artifacts) {
            ArtifactMetadata am = artifactStore.get(artifact.getURI());
            log.info("check: " + artifact.getURI() + " found: " + am);
            Assert.assertNotNull(artifact.getURI().toASCIIString(), am);
        }
    }

    @Test
    public void testValidFileSyncSmallSet1Thread() throws Exception {
        log.info("testValidFileSyncSmallSet1Thread - START");
        try {
            System.setProperty("user.home", TestUtil.TMP_DIR);

            List<Artifact> artifacts = makeSmallDataset();
            fileSyncTestBody(artifacts, 1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testValidFileSyncSmallSet1Thread - DONE");
    }

    @Test
    public void testValidFileSyncSmallSet4Threads() throws Exception {
        log.info("testValidFileSyncSmallSet4Threads - START");
        try {
            System.setProperty("user.home", TestUtil.TMP_DIR);

            List<Artifact> artifacts = makeSmallDataset();
            fileSyncTestBody(artifacts, 4);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testValidFileSyncSmallSet4Threads - DONE");
    }

    @Test
    public void testValidFileSyncLargeSet2Threads() {
        log.info("testValidFileSyncLargeSet2Threads - START");
        try {
            System.setProperty("user.home", TestUtil.TMP_DIR);

            List<Artifact> artifacts = makeLargeDataset();
            fileSyncTestBody(artifacts, 2);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testValidFileSyncLargeSet2Threads - DONE");
    }

    @Test
    public void testRetryOnRepeatedQuery() {
        log.info("testRetryOnRepeatedQuery - START");
        try {
            //System.setProperty("user.home", TestUtil.TMP_DIR);
            //File certificateFile = FileUtil.getFileFromResource(TestUtil.CERTIFICATE_FILE, FileSyncJobTest.class);
            //Subject subject = SSLUtil.createSubject(certificateFile);
            final Subject subject = AuthenticationUtil.getAnonSubject();

            // instantiate ArtifactStore when it's config is in place.
            final ArtifactStore artifactStore = TestUtil.getArtifactStore();


            Artifact testArtifact = makeArtifact("cadc:IRIS/no-such-file.fits");
            Observation observation = makeObservation(testArtifact);
            observationDAO.put(observation);
            HarvestSkipURI skip = makeHarvestSkipURI(testArtifact);
            harvestSkipURIDAO.put(skip);

            String namespace = null; // all
            BucketSelector buckets = new BucketSelector("0-f");

            // make sure FileSyncJob actually fails to update
            log.info("FileSync: START");
            final FileSync fs = new FileSync(daoConfig, cc, artifactStore, namespace, buckets, 1, 2);
            fs.testRunLoops = 1;
            fs.doit(subject);
            log.info("FileSync: DONE");

            skip = harvestSkipURIDAO.get(skip.getSource(), skip.getName(), skip.getSkipID());
            Assert.assertNotNull(skip);

            // now with loops
            log.info("FileSync: START");
            final FileSync fs2 = new FileSync(daoConfig, cc, artifactStore, namespace, buckets, 1, 2);
            fs2.testRunLoops = 4;
            fs.doit(subject);
            log.info("FileSync: DONE");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testRetryOnRepeatedQuery - DONE");
    }

}
