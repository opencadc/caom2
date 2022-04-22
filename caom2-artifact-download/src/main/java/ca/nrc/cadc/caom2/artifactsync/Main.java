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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Entry point for running the caom2-artifact-download process.
 *
 * @author jburke
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";
    private static final String CONFIG_FILE_NAME = "caom2-artifact-download.properties";
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String LOGGING_CONFIG_KEY = CONFIG_PREFIX + ".logging";
    private static final String PROFILE_CONFIG_KEY = CONFIG_PREFIX + ".profile";
    private static final String DB_SERVER_CONFIG_KEY = CONFIG_PREFIX + ".server";
    private static final String DB_DATABASE_CONFIG_KEY = CONFIG_PREFIX + ".database";
    private static final String DB_SCHEMA_CONFIG_KEY = CONFIG_PREFIX + ".schema";
    private static final String DB_USERNAME_CONFIG_KEY = CONFIG_PREFIX + ".username";
    private static final String DB_PASSWORD_CONFIG_KEY = CONFIG_PREFIX + ".password";
    private static final String COLLECTION_CONFIG_KEY = CONFIG_PREFIX + ".collection";
    private static final String ARTIFACT_STORE_CONFIG_KEY = CONFIG_PREFIX + ".artifactStore";
    private static final String CONTINUE_CONFIG_KEY = CONFIG_PREFIX + ".continue";
    private static final String BATCH_SIZE_CONFIG_KEY = CONFIG_PREFIX + ".batchSize";

    private static final String THREADS_CONFIG_KEY = CONFIG_PREFIX + ".threads";
    private static final String RETRY_AFTER_CONFIG_KEY = CONFIG_PREFIX + ".retryAfter";
    private static final String TOLERATE_NULL_CHECKSUM_CONFIG_KEY = CONFIG_PREFIX + ".tolerateNullChecksum";
    private static final String DOWNLOAD_THRESHOLD_CONFIG_KEY = CONFIG_PREFIX + ".downloadThreshold";

    private static final String JNDI_DATA_SOURCE_NAME = "jdbc/artifact-download";

    // Used to verify configuration items.  See the README for descriptions.
    private static final String[] MANDATORY_PROPERTY_KEYS = {
        LOGGING_CONFIG_KEY,
        PROFILE_CONFIG_KEY,
        DB_SERVER_CONFIG_KEY,
        DB_DATABASE_CONFIG_KEY,
        DB_SCHEMA_CONFIG_KEY,
        DB_USERNAME_CONFIG_KEY,
        DB_PASSWORD_CONFIG_KEY,
        COLLECTION_CONFIG_KEY,
        ARTIFACT_STORE_CONFIG_KEY,
        CONTINUE_CONFIG_KEY,
        THREADS_CONFIG_KEY,
        RETRY_AFTER_CONFIG_KEY,
        TOLERATE_NULL_CHECKSUM_CONFIG_KEY,
        DOWNLOAD_THRESHOLD_CONFIG_KEY
    };

    public static void main(final String[] args) {
        Log4jInit.setLevel(CONFIG_PREFIX, Level.INFO);

        try {
            final PropertiesReader propertiesReader = new PropertiesReader(CONFIG_FILE_NAME);
            final MultiValuedProperties props = propertiesReader.getAllProperties();
            if (props == null) {
                log.fatal(String.format("Configuration file not found: %s\n", CONFIG_FILE_NAME));
                System.exit(2);
            }

            final String[] missingKeys = Main.verifyConfiguration(props);
            if (missingKeys.length > 0) {
                log.fatal(String.format("Configuration file %s missing one or more values: %s.\n", CONFIG_FILE_NAME,
                                        Arrays.toString(missingKeys)));
                System.exit(2);
            }

            final String configuredLogging = props.getFirstPropertyValue(LOGGING_CONFIG_KEY);
            Level loggingLevel = Level.toLevel(configuredLogging.toUpperCase());
            Log4jInit.setLevel(CONFIG_PREFIX, loggingLevel);
            Log4jInit.setLevel("ca.nrc.cadc.caom2", loggingLevel);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", loggingLevel);
            if (loggingLevel.equals(Level.DEBUG)) {
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", loggingLevel);
                Log4jInit.setLevel("ca.nrc.cadc.net", loggingLevel);
            }

            final String configuredProfile = props.getFirstPropertyValue(PROFILE_CONFIG_KEY);
            final boolean profile = Boolean.parseBoolean(configuredProfile);
            if (profile) {
                Log4jInit.setLevel("ca.nrc.cadc.profiler", Level.INFO);
            }

            final String configuredServer = props.getFirstPropertyValue(DB_SERVER_CONFIG_KEY);
            final String configuredDatabase = props.getFirstPropertyValue(DB_DATABASE_CONFIG_KEY);
            final String configuredSchema = props.getFirstPropertyValue(DB_SCHEMA_CONFIG_KEY);
            final String configuredUsername = props.getFirstPropertyValue(DB_USERNAME_CONFIG_KEY);
            final String configuredPassword = props.getFirstPropertyValue(DB_PASSWORD_CONFIG_KEY);

            final String dbUrl = String.format("jdbc:postgresql://%s/%s", configuredServer, configuredDatabase);
            final ConnectionConfig connectionConfig = new ConnectionConfig(null, null,
                                                                           configuredUsername, configuredPassword,
                                                                           "org.postgresql.Driver", dbUrl);
            try {
                DBUtil.createJNDIDataSource(JNDI_DATA_SOURCE_NAME, connectionConfig);
            } catch (NamingException ne) {
                throw new IllegalStateException(String.format("Unable to access database: %s",
                                                              connectionConfig.getURL()), ne);
            }

            final Map<String, Object> daoConfig = new TreeMap<>();
            daoConfig.put("server", configuredServer);
            daoConfig.put("database", configuredDatabase);
            daoConfig.put("schema", configuredSchema);
            daoConfig.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            daoConfig.put("jndiDataSourceName", JNDI_DATA_SOURCE_NAME);

            ArtifactDAO artifactDAO = new ArtifactDAO();
            artifactDAO.setConfig(daoConfig);

            final String configuredCollection = props.getFirstPropertyValue(COLLECTION_CONFIG_KEY);
            HarvestResource harvestResource = new HarvestResource(configuredServer, configuredDatabase,
                                                                  configuredSchema, configuredCollection);

            final String configuredArtifactStore = props.getFirstPropertyValue(ARTIFACT_STORE_CONFIG_KEY);
            Class<?> asClass = Class.forName(configuredArtifactStore);
            ArtifactStore artifactStore = (ArtifactStore) asClass.getDeclaredConstructor().newInstance();
            Log4jInit.setLevel(asClass.getPackage().getName(), loggingLevel);

            final String configuredBatchSize = props.getFirstPropertyValue(BATCH_SIZE_CONFIG_KEY);
            final int batchSize = Integer.parseInt(configuredBatchSize);

            final String configuredContinue = props.getFirstPropertyValue(CONTINUE_CONFIG_KEY);
            final boolean loop = Boolean.parseBoolean(configuredContinue);

            final String configuredThreads = props.getFirstPropertyValue(THREADS_CONFIG_KEY);
            final int threads = Integer.parseInt(configuredThreads);

            final String configuredRetryAfter = props.getFirstPropertyValue(RETRY_AFTER_CONFIG_KEY);
            final int retryAfter = Integer.parseInt(configuredRetryAfter);

            final String configuredTolerateNullChecksum = props.getFirstPropertyValue(TOLERATE_NULL_CHECKSUM_CONFIG_KEY);
            final boolean tolerateNullChecksum = Boolean.parseBoolean(configuredTolerateNullChecksum);

            final String configuredDownloadThreshold = props.getFirstPropertyValue(DOWNLOAD_THRESHOLD_CONFIG_KEY);
            final int downloadThreshold = Integer.parseInt(configuredDownloadThreshold);

            final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));

            DownloadArtifactFiles downloader = new DownloadArtifactFiles(artifactDAO, harvestResource, artifactStore,
                                                                         threads, batchSize, loop, retryAfter,
                                                                         tolerateNullChecksum, downloadThreshold);
            Subject.doAs(subject, downloader);
        } catch (Throwable unexpected) {
            log.fatal("Unexpected failure", unexpected);
            System.exit(-1);
        }
    }

    /**
     * Verify all mandatory properties.
     * @param properties    The properties to check the mandatory keys against.
     * @return  An array of missing String keys, or empty array.  Never null.
     */
    private static String[] verifyConfiguration(final MultiValuedProperties properties) {
        final Set<String> keySet = properties.keySet();
        return Arrays.stream(MANDATORY_PROPERTY_KEYS).filter(k -> !keySet.contains(k)).toArray(String[]::new);
    }

    private Main() {
    }

}
