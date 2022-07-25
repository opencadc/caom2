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

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
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

    private static final String ARTIFACT_URI =  "ad:IRIS/I212B2H0.fits";
    private static final String ARTIFACT_CONTENT_CHECKSUM = "md5:646d3c548ffb98244a0fc52b60556082";
    private static final long ARTIFACT_CONTENT_LENGTH = 1008000;

    public FileSyncJobTest() throws Exception {
        super();
    }

    private Date getRetryAfterDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.roll(Calendar.HOUR, 6);
        return calendar.getTime();
    }

    @Test
    public void testMissingSourceArtifact() {
        log.info("testMissingSourceArtifact - START");
        try {
            System.setProperty("user.home", TestUtil.TMP_DIR);
            Subject subject = SSLUtil.createSubject(new File(FileSync.CERTIFICATE_FILE_LOCATION));

            // Source with a HarvestSkipURI but no Artifact.
            Artifact artifact = makeArtifact(ARTIFACT_URI, ARTIFACT_CONTENT_CHECKSUM, ARTIFACT_CONTENT_LENGTH);

            HarvestSkipURI skip = makeHarvestSkipURI(artifact);
            this.harvestSkipURIDAO.put(skip);

            boolean tolerateNullChecksum = true;
            Date retryAfterDate = getRetryAfterDate();

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, this.harvestSkipURIDAO, this.artifactDAO, this.artifactStore,
                                              tolerateNullChecksum, retryAfterDate, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // Skip record should be deleted
            skip = this.harvestSkipURIDAO.get(skip.getSource(), skip.getName(), artifact.getURI());
            Assert.assertNull("skip record should've been deleted", skip);
        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testMissingSourceArtifact - DONE");
    }

    @Test
    public void testTolerateNullChecksum() {
        log.info("testTolerateNullChecksum - START");
        try {
            System.setProperty("user.home", TestUtil.TMP_DIR);
            Subject subject = SSLUtil.createSubject(new File(FileSync.CERTIFICATE_FILE_LOCATION));

            // Source Artifact without a checksum and a HarvestSkipURI
            Artifact artifact = makeArtifact(ARTIFACT_URI, null, ARTIFACT_CONTENT_LENGTH);
            Observation observation = makeObservation(artifact);
            this.observationDAO.put(observation);

            HarvestSkipURI skip = makeHarvestSkipURI(artifact);
            this.harvestSkipURIDAO.put(skip);

            // Test tolerate  null checksum
            boolean tolerateNullChecksum = true;
            Date retryAfterDate = getRetryAfterDate();

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, this.harvestSkipURIDAO, this.artifactDAO, this.artifactStore,
                                              tolerateNullChecksum, retryAfterDate, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // Loop until the job has updated the artifact store.
            Connection con = this.artifactStoreDataSource.getConnection();
            String sql = String.format("select uri from %s.artifact", TestUtil.ARTIFACT_STORE_SCHEMA);
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            try {
                while (!rs.next()) {
                    log.debug("waiting for file sync jobs to update database");
                    Thread.sleep(1000);
                    rs = ps.executeQuery();
                }
                String uri = rs.getString(1);
                Assert.assertNotNull(uri);
            } catch (SQLException e) {
                log.error(String.format("Artifact %s not found in ArtifactStore", ARTIFACT_URI));
                Assert.fail(e.getMessage());
            }

            // Skip record should be deleted
            skip = this.harvestSkipURIDAO.get(skip.getSource(), skip.getName(), skip.getSkipID());
            Assert.assertNull("skip record should've been deleted", skip);

            // truncate source databases
            cleanTestEnvironment();

            // Source Artifact without a checksum & HarvestSkipURI
            this.observationDAO.put(observation);
            skip = makeHarvestSkipURI(artifact);
            this.harvestSkipURIDAO.put(skip);

            // Test do not tolerate null checksum
            tolerateNullChecksum = false;

            log.info("FileSyncJob: START");
            job = new FileSyncJob(skip, this.harvestSkipURIDAO, this.artifactDAO, this.artifactStore,
                                  tolerateNullChecksum, retryAfterDate, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // Skip record should exist and contain the errorMessage
            skip = this.harvestSkipURIDAO.get(skip.getSource(), skip.getName(), skip.getSkipID());
            Assert.assertNotNull("skip record should've been deleted", skip);
            Assert.assertEquals("artifact content checksum is null", skip.errorMessage);
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
            System.setProperty("user.home", TestUtil.TMP_DIR);
            Subject subject = SSLUtil.createSubject(new File(FileSync.CERTIFICATE_FILE_LOCATION));

            // Artifact & HarvestSkipURI in the database to start.
            Artifact artifact = makeArtifact(ARTIFACT_URI, ARTIFACT_CONTENT_CHECKSUM, ARTIFACT_CONTENT_LENGTH);
            Observation observation = makeObservation(artifact);
            HarvestSkipURI skip = makeHarvestSkipURI(artifact);

            log.debug("putting test artifact to database");
            this.observationDAO.put(observation);
            this.harvestSkipURIDAO.put(skip);

            boolean tolerateNullChecksum = true;
            Date retryAfterDate = getRetryAfterDate();

            log.info("FileSyncJob: START");
            FileSyncJob job = new FileSyncJob(skip, this.harvestSkipURIDAO, this.artifactDAO, this.artifactStore,
                                              tolerateNullChecksum, retryAfterDate, subject);
            job.run();
            log.info("FileSyncJob: DONE");

            // Loop until the job has updated the artifact store.
            Connection con = this.artifactStoreDataSource.getConnection();
            String sql = String.format("select uri from %s.artifact", TestUtil.ARTIFACT_STORE_SCHEMA);
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            try {
                while (!rs.next()) {
                    log.debug("waiting for file sync jobs to update database");
                    Thread.sleep(1000);
                    rs = ps.executeQuery();
                }
                String uri = rs.getString(1);
                Assert.assertNotNull(uri);
            } catch (SQLException e) {
                log.error(String.format("Artifact %s not found in ArtifactStore", ARTIFACT_URI));
                Assert.fail(e.getMessage());
            }
        } catch (Exception unexpected) {
            Assert.fail("unexpected exception: " + unexpected);
            log.debug(unexpected);
        } finally {
            System.setProperty("user.home", TestUtil.USER_HOME);
        }
        log.info("testValidJob - DONE");
    }

}
