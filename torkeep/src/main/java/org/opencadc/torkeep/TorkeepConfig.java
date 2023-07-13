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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.torkeep;

import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

public class TorkeepConfig {

    private static final Logger log = Logger.getLogger(TorkeepConfig.class);

    public static final String TORKEEP_PROPERTIES = "torkeep.properties";

    private static final String GRANT_PROVIDER_KEY = "org.opencadc.torkeep.grantProvider";
    private static final String COLLECTION_KEY = "org.opencadc.torkeep.collection";
    private static final String BASE_PUBLISHER_ID_KEY = ".basePublisherID";
    private static final String COMPUTE_METADATA_KEY = ".computeMetadata";
    private static final String PROPOSAL_GROUP_KEY = ".proposalGroup";
    private final List<URI> grantProviders = new ArrayList<>();
    private final List<CollectionEntry> configs = new ArrayList<>();

    public TorkeepConfig() {
        init();
    }

    public List<URI> getGrantProviders() {
        return this.grantProviders;
    }

    public List<CollectionEntry> getConfigs() {
        return this.configs;
    }

    public CollectionEntry getConfig(String collection) {
        if (!StringUtil.hasText(collection)) {
            throw new IllegalArgumentException("collection can not be null");
        }
        for (CollectionEntry entry : configs) {
            if (collection.equals(entry.getCollection())){
                return entry;
            }
        }
        return null;
    }

    void init() {
        PropertiesReader propertiesReader = new PropertiesReader(TORKEEP_PROPERTIES);
        MultiValuedProperties properties = propertiesReader.getAllProperties();
        if (properties == null) {
            throw new IllegalStateException("CONFIG: failed to read config from " + TORKEEP_PROPERTIES);
        }
        if (properties.isEmpty()) {
            throw new IllegalStateException("CONFIG: properties not found in " + TORKEEP_PROPERTIES);
        }

        // config errors
        StringBuilder errors = new StringBuilder();

        // grant providers
        this.grantProviders.addAll(getGrantProviders(properties, errors));

        // get the collection keys
        List<String> collections = getCollections(properties, errors);
        for (String collection : collections) {
            log.debug("reading collection: " + collection);

            URI basePublisherID = getBasePublisherID(properties, collection + BASE_PUBLISHER_ID_KEY, errors);

            String computeMetadataValue = getProperty(properties, collection + COMPUTE_METADATA_KEY, errors);
            boolean computeMetadata = Boolean.parseBoolean(computeMetadataValue);

            String proposalGroupValue = getProperty(properties, collection + PROPOSAL_GROUP_KEY, errors);
            boolean proposalGroup = Boolean.parseBoolean(proposalGroupValue);

            // if a config errors found, read and check the rest of the properties
            if (errors.length() > 0) {
                continue;
            }

            CollectionEntry cc = new CollectionEntry(collection, basePublisherID, computeMetadata, proposalGroup);
            this.configs.add(cc);
            log.debug("added " + cc);
        }

        if (errors.length() > 0) {
            throw new InvalidConfigException(String.format("\nCONFIG: configuration contains errors\n\n%s",
                    errors));
        }
    }

    private List<URI> getGrantProviders(MultiValuedProperties properties, StringBuilder errors) {
        List<String> values = properties.getProperty(GRANT_PROVIDER_KEY);
        if (values.isEmpty()) {
            throw new InvalidConfigException(String.format("CONFIG: configured %s not found", GRANT_PROVIDER_KEY));
        }
        List<URI> providers = new ArrayList<>();
        for (String value : values) {
            try {
                providers.add(new URI(value));
                log.debug("grant provider - " + value);
            } catch (URISyntaxException e) {
                errors.append(String.format("%s: invalid URI %s - %s\n", GRANT_PROVIDER_KEY, value, e.getMessage()));
            }
        }
        return providers;
    }

    private List<String> getCollections(MultiValuedProperties properties, StringBuilder errors) {
        List<String> collections = properties.getProperty(COLLECTION_KEY);
        if (collections.isEmpty()) {
            throw new InvalidConfigException(String.format("CONFIG: configured %s not found", COLLECTION_KEY));
        }
        log.debug("reading collections config with " + collections.size() + " collections.");

        // check for duplicate collection names
        List<String> duplicates = new ArrayList<>();
        Set<String> collectionSet = new HashSet<>();
        for (String key : collections) {
            if (!collectionSet.add(key)) {
                duplicates.add(key);
            }
        }
        if (!duplicates.isEmpty()) {
            errors.append(String.format("%s: duplicate collections found - %s\n",
                    COLLECTION_KEY, Arrays.toString(duplicates.toArray())));
        }
        return collections;
    }

    private String getProperty(MultiValuedProperties properties, String key, StringBuilder errors) {
        String value = null;
        List<String> values = properties.getProperty(key);
        if (values.isEmpty()) {
            errors.append(String.format("%s: missing required property\n", key));
        } else if (values.size() > 1) {
            errors.append(String.format("%s: property has multiple values\n", key));
        } else {
            value = values.get(0);
            if (!StringUtil.hasText(value)) {
                errors.append(String.format("%s: property value is empty\n", key));
            }
        }
        return value;
    }

    private URI getBasePublisherID(MultiValuedProperties properties, String key, StringBuilder errors) {
        URI basePublisherID = null;
        String basePublisherIDValue = getProperty(properties, key, errors);
        if (basePublisherIDValue != null) {
            try {
                basePublisherID = new URI(basePublisherIDValue);
                if (!"ivo".equals(basePublisherID.getScheme()) || basePublisherID.getAuthority() == null) {
                    errors.append(String.format("%s: invalid basePublisherID %s, expected ivo://<authority>[/<path>]\n",
                            key, basePublisherIDValue));
                }
            } catch (URISyntaxException e) {
                errors.append(String.format("%s: invalid basePublisherID URI %s - %s\n",
                        key, basePublisherIDValue, e.getMessage()));
            }
        }
        return basePublisherID;
    }

}
