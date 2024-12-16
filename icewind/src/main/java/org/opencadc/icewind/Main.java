/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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

package org.opencadc.icewind;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Entry point for running the caom2-meta-sync process.
 *
 * @author jburke
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    private static final String CONFIG_FILE_NAME = "icewind.properties";
    private static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String LOGGING_CONFIG_KEY = CONFIG_PREFIX + ".logging";
    
    private static final String RETRY_MODE_CONFIG_KEY = CONFIG_PREFIX + ".retrySkipped";
    private static final String VALIDATE_MODE_CONFIG_KEY = CONFIG_PREFIX + ".validate";
    private static final String RETRY_ERROR_PATTERN = CONFIG_PREFIX + ".retryErrorPattern";
    private static final String REPO_SERVICE_CONFIG_KEY = CONFIG_PREFIX + ".repoService";
    private static final String COLLECTION_CONFIG_KEY = CONFIG_PREFIX + ".collection";
    private static final String MAX_IDLE_CONFIG_KEY = CONFIG_PREFIX + ".maxIdle";
    private static final String BATCH_SIZE_CONFIG_KEY = CONFIG_PREFIX + ".batchSize";
    private static final String NUM_THREADS_CONFIG_KEY = CONFIG_PREFIX + ".numThreads";
    private static final String DB_URL_CONFIG_KEY = CONFIG_PREFIX + ".caom.url";
    private static final String DB_SCHEMA_CONFIG_KEY = CONFIG_PREFIX + ".caom.schema";
    private static final String DB_USERNAME_CONFIG_KEY = CONFIG_PREFIX + ".caom.username";
    private static final String DB_PASSWORD_CONFIG_KEY = CONFIG_PREFIX + ".caom.password";
    private static final String BASE_PUBLISHER_ID_CONFIG_KEY = CONFIG_PREFIX + ".basePublisherID";
    private static final String EXIT_WHEN_COMPLETE_CONFIG_KEY = CONFIG_PREFIX + ".exitWhenComplete";

    private static final boolean DEFAULT_EXIT_WHEN_COMPLETE = false;
    private static final int DEFAULT_BATCH_SIZE = 100;


    // Used to verify configuration items.  See the README for descriptions.
    private static final String[] MANDATORY_PROPERTY_KEYS = {
        LOGGING_CONFIG_KEY, REPO_SERVICE_CONFIG_KEY, COLLECTION_CONFIG_KEY, MAX_IDLE_CONFIG_KEY,
        DB_SCHEMA_CONFIG_KEY, DB_USERNAME_CONFIG_KEY, DB_PASSWORD_CONFIG_KEY, DB_URL_CONFIG_KEY,
        BASE_PUBLISHER_ID_CONFIG_KEY
    };

    public static void main(final String[] args) {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
        Log4jInit.setLevel("org.opencadc.caom2", Level.INFO);
        Log4jInit.setLevel("org.opencadc.icewind", Level.INFO);

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
            Log4jInit.setLevel("ca.nrc.cadc.db.version", loggingLevel);
            if (loggingLevel.equals(Level.DEBUG)) {
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", loggingLevel);
            }

            // source
            final List<String> configuredCollections = props.getProperty(COLLECTION_CONFIG_KEY);
            if (configuredCollections.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format("%s must be configured with a minimum of one collection", COLLECTION_CONFIG_KEY));
            }
            final URI configuredSourceRepoService;
            String s = props.getFirstPropertyValue(REPO_SERVICE_CONFIG_KEY);
            try {
                configuredSourceRepoService = new URI(s);
            } catch (URISyntaxException ex) {
                throw new IllegalArgumentException(
                        String.format("%s - invalid URI: %s because: %s", REPO_SERVICE_CONFIG_KEY, s, ex.getMessage()));
            }
            final HarvestSource sourceHarvestResource =
                new HarvestSource(configuredSourceRepoService, configuredCollections);

            // destination
            final String configuredDestinationUrl = props.getFirstPropertyValue(DB_URL_CONFIG_KEY);
            final String configuredDestinationSchema = props.getFirstPropertyValue(DB_SCHEMA_CONFIG_KEY);
            final String configuredDestinationUsername = props.getFirstPropertyValue(DB_USERNAME_CONFIG_KEY);
            final String configuredDestinationPassword = props.getFirstPropertyValue(DB_PASSWORD_CONFIG_KEY);
            final HarvestDestination destinationHarvestResource = new HarvestDestination(configuredDestinationUrl,
                    configuredDestinationUsername, configuredDestinationPassword, configuredDestinationSchema);

            // publisherID generation
            String configuredBasePublisherIDUrl = props.getFirstPropertyValue(BASE_PUBLISHER_ID_CONFIG_KEY);
            if (!StringUtil.hasText(configuredBasePublisherIDUrl)) {
                throw new IllegalArgumentException(String.format("%s - has no value", BASE_PUBLISHER_ID_CONFIG_KEY));
            }
            if (!configuredBasePublisherIDUrl.endsWith("/")) {
                configuredBasePublisherIDUrl += "/";
            }
            URI basePublisherID;
            try {
                basePublisherID = URI.create(configuredBasePublisherIDUrl);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format("%s - invalid URI: %s because: %s", BASE_PUBLISHER_ID_CONFIG_KEY,
                                  configuredBasePublisherIDUrl, e.getMessage()));
            }
            if (!"ivo".equals(basePublisherID.getScheme()) || !StringUtil.hasText(basePublisherID.getAuthority())) {
                throw new IllegalArgumentException(
                    String.format("%s - invalid basePublisherID: %s expected: ivo://<authority> or ivo://<authority>/<path>",
                                                                 BASE_PUBLISHER_ID_CONFIG_KEY, configuredBasePublisherIDUrl));
            }

            // modes
            final boolean retrySkipped;
            final String errorMessagePattern;
            final String configuredRetrySkipped = props.getFirstPropertyValue(RETRY_MODE_CONFIG_KEY);
            if (configuredRetrySkipped == null) {
                retrySkipped = false;
                errorMessagePattern = null;
            } else {
                retrySkipped = Boolean.parseBoolean(configuredRetrySkipped);
                errorMessagePattern = props.getFirstPropertyValue(RETRY_ERROR_PATTERN);
            }
            
            final boolean validateMode;
            String configuredValidateMode = props.getFirstPropertyValue(VALIDATE_MODE_CONFIG_KEY);
            if (configuredValidateMode == null) {
                validateMode = false;
            } else {
                validateMode = Boolean.parseBoolean(configuredValidateMode);
            }
            
            final boolean exitWhenComplete;
            final String configuredExitWhenComplete = props.getFirstPropertyValue(EXIT_WHEN_COMPLETE_CONFIG_KEY);
            if (retrySkipped || validateMode) {
                exitWhenComplete = true;
            } else if (configuredExitWhenComplete == null) {
                exitWhenComplete = DEFAULT_EXIT_WHEN_COMPLETE;
            } else {
                exitWhenComplete = Boolean.parseBoolean(configuredExitWhenComplete);
            }

            final String configuredMaxSleep = props.getFirstPropertyValue(MAX_IDLE_CONFIG_KEY);
            final long maxSleep = Long.parseLong(configuredMaxSleep);
            
            String configBatchSize = props.getFirstPropertyValue(BATCH_SIZE_CONFIG_KEY);
            int batchSize = DEFAULT_BATCH_SIZE;
            if (configBatchSize != null) {
                batchSize = Integer.parseInt(configBatchSize);
                if (batchSize < 1) {
                    throw new IllegalArgumentException("invalid batchSize: " + batchSize);
                }
            }
            String configNumThreads = props.getFirstPropertyValue(BATCH_SIZE_CONFIG_KEY);
            int numThreads = 1 + batchSize / 10;
            if (configNumThreads != null) {
                numThreads = Integer.parseInt(configNumThreads);
                if (numThreads < 1) {
                    throw new IllegalArgumentException("invalid numThreads: " + numThreads);
                }
            }

            CaomHarvester harvester = new CaomHarvester(sourceHarvestResource, configuredCollections, 
                    destinationHarvestResource, basePublisherID);
            harvester.batchSize = batchSize;
            harvester.numThreads = numThreads;
            harvester.maxSleep = maxSleep;
            harvester.skipMode = retrySkipped;
            harvester.validateMode = validateMode;
            harvester.retryErrorMessagePattern = errorMessagePattern;
            harvester.exitWhenComplete = exitWhenComplete;

            Subject subject = AuthenticationUtil.getAnonSubject();
            File cert = new File(CERTIFICATE_FILE_LOCATION);
            if (cert.exists()) {
                subject = SSLUtil.createSubject(cert);
            }
            Subject.doAs(subject, new RunnableAction(harvester));
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

    private static String[] parseServerDatabase(final String dbUrl) {
        try {
            String[] parts = dbUrl.split("/+");
            String server = parts[1].split(":")[0];
            String database = parts[2];
            return new String[] {server, database};
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Unable to parse server/database from url %s because %s",
                    dbUrl, e.getMessage()));
        }
    }

    private Main() {
    }

}

