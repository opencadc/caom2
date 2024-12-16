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

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractFileSyncTest {

    private static final Logger log = Logger.getLogger(AbstractFileSyncTest.class);

    static Map<String,Object> daoConfig;
    static ConnectionConfig cc;

    final ArtifactDAO artifactDAO;
    final ObservationDAO observationDAO;
    final HarvestSkipURIDAO harvestSkipURIDAO;
    final DataSource caom2DataSource;

    public AbstractFileSyncTest() throws Exception {
        daoConfig = new TreeMap<>();
        try {
            // source caom2 database
            DBConfig dbrc = new DBConfig();
            cc = dbrc.getConnectionConfig(TestUtil.CAOM2_SERVER, TestUtil.CAOM2_DATABASE);
            this.caom2DataSource = DBUtil.getDataSource(cc);

            daoConfig.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            daoConfig.put("basePublisherID", "ivo://opencadc.org/");
            daoConfig.put("server", TestUtil.CAOM2_SERVER);
            daoConfig.put("database", TestUtil.CAOM2_DATABASE);
            daoConfig.put("schema", TestUtil.CAOM2_SCHEMA);

            DataSource dataSource = DBUtil.getDataSource(cc, true, true);
            this.harvestSkipURIDAO = new HarvestSkipURIDAO(dataSource, TestUtil.CAOM2_DATABASE, TestUtil.CAOM2_SCHEMA);

            this.artifactDAO = new ArtifactDAO();
            this.artifactDAO.setConfig(daoConfig);

            this.observationDAO = new ObservationDAO();
            this.observationDAO.setConfig(daoConfig);
            
            InitDatabase init = new InitDatabase(caom2DataSource, TestUtil.CAOM2_DATABASE, TestUtil.CAOM2_SCHEMA);
            init.doInit();

        } catch (Exception ex) {
            log.error("setup failed", ex);
            throw ex;
        }

    }

    @Before
    public void cleanTestEnvironment() throws Exception {
        log.debug("cleaning caom2 HarvestSkipURI");
        String sql = String.format("delete from %s.harvestSkipURI", TestUtil.CAOM2_SCHEMA);
        this.caom2DataSource.getConnection().createStatement().execute(sql);

        log.debug("cleaning caom2 observations...");
        sql = String.format("truncate table %s.observation cascade", TestUtil.CAOM2_SCHEMA);
        this.caom2DataSource.getConnection().createStatement().execute(sql);
    }

    Artifact makeArtifact(final String uri) {
        return makeArtifact(uri, null, null);
    }

    Artifact makeArtifact(final String uri, String contentChecksum, Long contentLength) {
        Artifact artifact = new Artifact(URI.create(uri), ProductType.SCIENCE, ReleaseType.DATA);
        if (contentChecksum != null) {
            artifact.contentChecksum = URI.create(contentChecksum);
        }
        if (contentLength != null) {
            artifact.contentLength = contentLength;
        }
        return artifact;
    }

    Observation makeObservation(final Artifact artifact) {
        String id = String.valueOf(System.currentTimeMillis());
        Plane plane = new Plane("plane-" + id);
        plane.getArtifacts().add(artifact);
        Observation observation = new SimpleObservation("IRIS", "obs-" + id);
        observation.getPlanes().add(plane);
        return observation;
    }

    HarvestSkipURI makeHarvestSkipURI(final Artifact artifact) {
        return new HarvestSkipURI("cadctest.caom2?IRIS", "Artifact", artifact.getURI(), new Date());
    }
}
