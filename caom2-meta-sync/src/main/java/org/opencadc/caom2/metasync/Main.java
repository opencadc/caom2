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

package org.opencadc.caom2.metasync;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.net.URI;
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

    private static final String CONFIG_FILE_NAME = "caom2-meta-sync.properties";
    private static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";
    private static final String CONFIG_PREFIX = Main.class.getPackage().getName();
    private static final String LOGGING_CONFIG_KEY = CONFIG_PREFIX + ".logging";

    private static final String REPO_SERVICE_CONFIG_KEY = CONFIG_PREFIX + ".repoService";
    private static final String COLLECTION_CONFIG_KEY = CONFIG_PREFIX + ".collection";
    private static final String MAX_IDLE_CONFIG_KEY = CONFIG_PREFIX + ".maxIdle";
    private static final String DB_URL_CONFIG_KEY = CONFIG_PREFIX + ".db.url";
    private static final String DB_SCHEMA_CONFIG_KEY = CONFIG_PREFIX + ".db.schema";
    private static final String DB_USERNAME_CONFIG_KEY = CONFIG_PREFIX + ".db.username";
    private static final String DB_PASSWORD_CONFIG_KEY = CONFIG_PREFIX + ".db.password";
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

            final String configuredSourceRepoService = props.getFirstPropertyValue(REPO_SERVICE_CONFIG_KEY);
            final HarvesterResource sourceHarvestResource;
            try {
                sourceHarvestResource = new HarvesterResource(URI.create(configuredSourceRepoService));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format("%s - invalid repository service URI: %s reason: %s", REPO_SERVICE_CONFIG_KEY,
                                  configuredSourceRepoService, e.getMessage()));
            }

            final String configuredDestinationUrl = props.getFirstPropertyValue(DB_URL_CONFIG_KEY);
            String [] destinationServerDatabase = parseServerDatabase(configuredDestinationUrl);
            final String destinationServer = destinationServerDatabase[0];
            final String destinationDatabase = destinationServerDatabase[1];

            final String configuredDestinationSchema = props.getFirstPropertyValue(DB_SCHEMA_CONFIG_KEY);
            final String configuredDestinationUsername = props.getFirstPropertyValue(DB_USERNAME_CONFIG_KEY);
            final String configuredDestinationPassword = props.getFirstPropertyValue(DB_PASSWORD_CONFIG_KEY);

            final HarvesterResource destinationHarvestResource = new HarvesterResource(configuredDestinationUrl,
                    destinationServer, destinationDatabase, configuredDestinationUsername,
                    configuredDestinationPassword, configuredDestinationSchema);

            final List<String> configuredCollections = props.getProperty(COLLECTION_CONFIG_KEY);
            if (configuredCollections.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format("%s must be configured with a minimum of one collection", COLLECTION_CONFIG_KEY));
            }

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

            final boolean exitWhenComplete;
            final String configuredExitWhenComplete = props.getFirstPropertyValue(EXIT_WHEN_COMPLETE_CONFIG_KEY);
            if (configuredExitWhenComplete == null) {
                exitWhenComplete = DEFAULT_EXIT_WHEN_COMPLETE;
            } else {
                exitWhenComplete = Boolean.parseBoolean(configuredExitWhenComplete);
            }

            final String configuredMaxSleep = props.getFirstPropertyValue(MAX_IDLE_CONFIG_KEY);
            final long maxSleep = Long.parseLong(configuredMaxSleep);

            // full=false, skip=false: incremental harvest
            final boolean full = false;
            final boolean skip = false;
            final boolean noChecksum = false;
            CaomHarvester harvester = new CaomHarvester(sourceHarvestResource, destinationHarvestResource,
                    configuredCollections, basePublisherID, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_SIZE / 10,
                                                        full, skip, noChecksum, exitWhenComplete, maxSleep);

            final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
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

