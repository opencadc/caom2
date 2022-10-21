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
import ca.nrc.cadc.util.Log4jInit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class FileSyncJobTest extends AbstractFileSyncTest {
    private static final Logger log = Logger.getLogger(FileSyncJobTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.caom2.download", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.db", Level.INFO);
    }

    private static final String ARTIFACT_URI =  "cadc:IRIS/I212B2H0.fits";
    private static final String ARTIFACT_CONTENT_CHECKSUM = "md5:646d3c548ffb98244a0fc52b60556082";
    private static final long ARTIFACT_CONTENT_LENGTH = 1008000;

    public FileSyncJobTest() throws Exception {
        super();
    }

    @Test
    public void testMissingSourceArtifact() {
        log.info("testMissingSourceArtifact - START");
        try {
            //System.setProperty("user.home", TestUtil.TMP_DIR);
            //File certificateFile = FileUtil.getFileFromResource(TestUtil.CERTIFICATE_FILE, FileSyncJobTest.class);
            //Subject subject = SSLUtil.createSubject(certificateFile);
            final Subject subject = AuthenticationUtil.getAnonSubject();

            // instantiate ArtifactStore when it's config is in place.
            final ArtifactStore artifactStore = TestUtil.getArtifactStore();

            // Source with a HarvestSkipURI but no Artifact.
            Artifact artifact = makeArtifact(ARTIFACT_URI, ARTIFACT_CONTENT_CHECKSUM, ARTIFACT_CONTENT_LENGTH);

            HarvestSkipURI skip = makeHarvestSkipURI(artifact);
            this.harvestSkipURIDAO.put(skip);

            int retryAfter = 2;

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, harvestSkipURIDAO, artifactDAO, artifactStore,
                                            retryAfter, subject);
            job.run();
            log.info("FileSyncJob: DONE");
            
            // artifact should not be stored
            ArtifactMetadata am = artifactStore.get(skip.getSkipID());
            Assert.assertNull(am);

            // Skip record should be deleted
            skip = this.harvestSkipURIDAO.get(skip.getSource(), skip.getName(), artifact.getURI());
            Assert.assertNull("skip record should been deleted", skip);
        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        } finally {
            //System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testMissingSourceArtifact - DONE");
    }

    @Test
    public void testTolerateNullChecksum() {
        log.info("testTolerateNullChecksum - START");
        try {
            //System.setProperty("user.home", TestUtil.TMP_DIR);
            //File certificateFile = FileUtil.getFileFromResource(TestUtil.CERTIFICATE_FILE, FileSyncJobTest.class);
            //Subject subject = SSLUtil.createSubject(certificateFile);
            final Subject subject = AuthenticationUtil.getAnonSubject();

            // instantiate ArtifactStore when it's config is in place.
            final ArtifactStore artifactStore = TestUtil.getArtifactStore();

            // Source Artifact without a checksum and a HarvestSkipURI
            Artifact artifact = makeArtifact(ARTIFACT_URI, null, ARTIFACT_CONTENT_LENGTH);
            Observation observation = makeObservation(artifact);
            this.observationDAO.put(observation);

            HarvestSkipURI skip = makeHarvestSkipURI(artifact);
            this.harvestSkipURIDAO.put(skip);

            // Test tolerate  null checksum
            int retryAfter = 2;

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, harvestSkipURIDAO, artifactDAO, artifactStore,
                                            retryAfter, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // artifact should be stored
            ArtifactMetadata am = artifactStore.get(skip.getSkipID());
            Assert.assertNotNull(am);
            
            // Skip record should be deleted
            skip = harvestSkipURIDAO.get(skip.getSource(), skip.getName(), skip.getSkipID());
            Assert.assertNull("skip record should've been deleted", skip);

        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testTolerateNullChecksum - DONE");
    }

    @Test
    public void testValidJob() {
        try {
            //System.setProperty("user.home", TestUtil.TMP_DIR);
            //File certificateFile = FileUtil.getFileFromResource(TestUtil.CERTIFICATE_FILE, FileSyncJobTest.class);
            //Subject subject = SSLUtil.createSubject(certificateFile);
            final Subject subject = AuthenticationUtil.getAnonSubject();

            // instantiate ArtifactStore when it's config is in place.
            final ArtifactStore artifactStore = TestUtil.getArtifactStore();

            // Artifact & HarvestSkipURI in the database to start.
            Artifact artifact = makeArtifact(ARTIFACT_URI, ARTIFACT_CONTENT_CHECKSUM, ARTIFACT_CONTENT_LENGTH);
            Observation observation = makeObservation(artifact);
            HarvestSkipURI skip = makeHarvestSkipURI(artifact);

            log.debug("putting test artifact to database");
            this.observationDAO.put(observation);
            this.harvestSkipURIDAO.put(skip);

            int retryAfter = 2;

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, harvestSkipURIDAO, artifactDAO, artifactStore,
                                            retryAfter, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // artifact should be stored
            ArtifactMetadata am = artifactStore.get(skip.getSkipID());
            Assert.assertNotNull(am);
            
            // Skip record should be deleted
            skip = this.harvestSkipURIDAO.get(skip.getSource(), skip.getName(), artifact.getURI());
            Assert.assertNull("skip record should been deleted", skip);
        } catch (Exception unexpected) {
            log.error("unexpedcted exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
            
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testValidJob - DONE");
    }

}
