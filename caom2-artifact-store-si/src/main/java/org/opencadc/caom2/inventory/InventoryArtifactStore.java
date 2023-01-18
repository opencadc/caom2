/*
************************************************************************
M*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
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

package org.opencadc.caom2.inventory;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.StoragePolicy;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileMetadata;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

/**
 * CADC Implementation of the ArtifactStore interface that uses Storage Inventory.
 *
 * <p>This class interacts with the Storage Inventory services to perform the
 * artifact operations defined in ArtifactStore.
 *
 * @author yeunga
 */
public class InventoryArtifactStore implements ArtifactStore {

    private static final Logger log = Logger.getLogger(InventoryArtifactStore.class);

    private static final String COLLECTION_CONFIG = "collection-prefix.properties";
    private static final String CONFIG_FILE_NAME = "caom2-artifact-store-si.properties";

    private static final String CONFIG_PREFIX = InventoryArtifactStore.class.getPackage().getName();
    private static final String QUERY_SERVICE_CONFIG_KEY = CONFIG_PREFIX + ".queryService";
    private static final String LOCATE_SERVICE_CONFIG_KEY = CONFIG_PREFIX + ".locateService";

    public static final int DEFAULT_TIMEOUT = 600000;  // 10 minutes

    // Used to verify configuration items.  See the README for descriptions.
    private static final String[] MANDATORY_PROPERTY_KEYS = {
        LOCATE_SERVICE_CONFIG_KEY,
        QUERY_SERVICE_CONFIG_KEY
    };

    private URI locatorService;
    private URI queryService;
    private URL storageInventoryTapURL;
    private URL locateServicesFilesURL;
    private List<Protocol> storeProtocolList = new ArrayList<>();

    public InventoryArtifactStore() {
        initConfig();
    }
    
    // ctor for intTest
    InventoryArtifactStore(URI locatorService, URI queryService) {
        this.locatorService = locatorService;
        this.queryService = queryService;
    }

    /**
     * Get the ArtifactMetadata for the given Artifact URI.
     *
     * @param artifactURI the Artifact URI
     * @return the ArtifactMetadata for the artifactURI, or null if the Artifact is not found.
     */
    public ArtifactMetadata get(URI artifactURI) {
        URL url = getLocateFilesURL(artifactURI);
        HttpGet head = new HttpGet(url, true);
        head.setHeadOnly(true);
        head.setConnectionTimeout(6000);
        head.setReadTimeout(12000);

        long start = System.currentTimeMillis();
        try {
            head.prepare();
        } catch (ResourceAlreadyExistsException e) {
            throw new RuntimeException("BUG: ResourceAlreadyExistsException thrown for HEAD request", e);
        } catch (ResourceNotFoundException ignore) {
            return null;
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
        log.debug("Finished physical get query in " + (System.currentTimeMillis() - start) + " ms");

        if (head.getResponseCode() == 200) {
            URI digest = head.getDigest();
            String checksum = digest != null ? digest.getSchemeSpecificPart() : null;
            ArtifactMetadata artifactMetadata = new ArtifactMetadata(artifactURI, checksum);
            artifactMetadata.contentLength = head.getContentLength();
            artifactMetadata.contentType = head.getContentType();
            return artifactMetadata;
        }
        return null;
    }

    public StoragePolicy getStoragePolicy(String collection) {
        PropertiesReader pr = new PropertiesReader(COLLECTION_CONFIG);
        final MultiValuedProperties props = pr.getAllProperties();
        if (props == null || props.keySet().isEmpty()) {
            String msg = "Missing " + COLLECTION_CONFIG
                    + " or the config file is empty.";
            throw new IllegalArgumentException(msg);
        } else {
            String policyKey = collection.toUpperCase() + ".policy";
            final String value = props.getFirstPropertyValue(policyKey);
            if (value == null) {
                String msg = "Missing an entry in the config file for collection " + policyKey + ".";
                throw new IllegalArgumentException(msg);
            } else {
                return StoragePolicy.toValue(value);
            }
        }
    }

    /**
     * Add an Artifact to a Storage Inventory.
     *
     * @param artifactURI the Artifact URI to store
     * @param src URl to retrieve file from
     * @param metadata Artifact metadata
     * @throws TransientException if an unexpected, temporary exception occurred
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws ca.nrc.cadc.net.ResourceNotFoundException
     */
    @Override
    public void store(URI artifactURI, URL src, FileMetadata metadata) throws TransientException, InterruptedException,
            IOException, ResourceNotFoundException {

        // request all protocols that can be used
        if (storeProtocolList.isEmpty()) {
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            storeProtocolList.add(new Protocol(VOS.PROTOCOL_HTTPS_PUT));
            if (!AuthMethod.ANON.equals(authMethod)) {
                Protocol httpsAuth = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
                httpsAuth.setSecurityMethod(Standards.getSecurityMethod(authMethod));
                storeProtocolList.add(httpsAuth);
            }
        }

        Direction direction = Direction.pushToVoSpace;
        InventoryClient storageInventoryClient = new InventoryClient(locatorService);
        Transfer transfer = storageInventoryClient.createTransferSync(artifactURI, direction, storeProtocolList);
        if (transfer.getAllEndpoints().isEmpty()) {
            throw new RuntimeException("No transfer endpoint available.");
        }

        storageInventoryClient.upload(transfer, src, metadata);
    }

    /**
     * Get a list of ArtifactMetadata for artifacts matching the given namespace.
     *
     * @param namespace artifact uri prefix to match
     * @return Set of ArtifactMetadata with matching namespace, or an empty set if no
     *         matching Artifacts found, never null.
     * @throws TransientException if an unexpected, temporary exception occurred
     * @throws UnsupportedOperationException if an unsupported operation occurs
     * @throws AccessControlException if the caller doesn't have permission to access the resource
     */
    @Override
    public Set<ArtifactMetadata> list(String namespace)
            throws IOException, InterruptedException,
            ResourceNotFoundException, TransientException, AccessControlException {
        
        ResourceIterator<ArtifactMetadata> iter = iterator(namespace);
        TreeSet<ArtifactMetadata> result = new TreeSet<>(ArtifactMetadata.getComparator());
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        return result;
    }
    
    // soon: list() will be replaced by iterator()
    public ResourceIterator<ArtifactMetadata> iterator(String namespace)
            throws IOException, InterruptedException,
            ResourceNotFoundException, TransientException, AccessControlException {
        String adql = ArtifactRowMapper.SELECT + " WHERE uri LIKE '" + namespace + "%'";
        log.warn("query: " + adql);
        try {
            long start = System.currentTimeMillis();
            TapClient tap = new TapClient(queryService);
            tap.setConnectionTimeout(6000);
            tap.setReadTimeout(12000);
            ResourceIterator<ArtifactMetadata> ret = tap.query(adql, new ArtifactRowMapper(), true);
            log.debug("Finished query in " + (System.currentTimeMillis() - start) + " ms");
            return ret;
        } catch (ByteLimitExceededException ex) {
            throw new RuntimeException("UNEXPECTED fail: " + ex, ex);
        }
    }

    class ArtifactRowMapper implements TapRowMapper<ArtifactMetadata>  {
        public static final String SELECT = "SELECT uri, contentChecksum, contentLength, contentType"
                + " FROM inventory.Artifact";

        @Override
        public ArtifactMetadata mapRow(final List<Object> row) {
            int index = 0;
            final URI uri = (URI) row.get(index++);
            final URI contentChecksum = (URI) row.get(index++);

            final ArtifactMetadata artifact = new ArtifactMetadata(uri, contentChecksum.getSchemeSpecificPart());
            artifact.contentLength = (Long) row.get(index++);
            artifact.contentType = (String) row.get(index++);
            return artifact;
        }
    }
    
    @Override
    public String toStorageID(String artifactURI) throws IllegalArgumentException {
        throw new UnsupportedOperationException("toStorageID(...) is no longer supported.");
    }

    /**
     * Verify all of the mandatory properties.
     *
     * @param properties The properties to check the mandatory keys against.
     * @return An array of missing String keys, or empty array. Never null.
     */
    private static String[] verifyConfiguration(final MultiValuedProperties properties) {
        if (properties != null) {
            final Set<String> keySet = properties.keySet();
            return Arrays.stream(MANDATORY_PROPERTY_KEYS).filter(k -> !keySet.contains(k)).toArray(String[]::new);
        } else {
            return null;
        }
    }

    private void initConfig() {
        final PropertiesReader propertiesReader = new PropertiesReader(CONFIG_FILE_NAME);
        final MultiValuedProperties props = propertiesReader.getAllProperties();
        final String[] missingKeys = verifyConfiguration(props);

        if (missingKeys != null && missingKeys.length > 0) {
            String message = String.format("Configuration file %s missing one or more values: %s.",
                    CONFIG_FILE_NAME, Arrays.toString(missingKeys));
            log.fatal(message);
            throw new RuntimeException(message);
        }

        final String configuredQueryService = props.getFirstPropertyValue(QUERY_SERVICE_CONFIG_KEY);
        final String configuredLocateService = props.getFirstPropertyValue(LOCATE_SERVICE_CONFIG_KEY);
        this.locatorService = URI.create(configuredLocateService);
        this.queryService = URI.create(configuredQueryService);
    }

    private String getPrefixes(String collection) {
        PropertiesReader pr = new PropertiesReader(COLLECTION_CONFIG);
        final MultiValuedProperties props = pr.getAllProperties();
        if (props == null || props.keySet().isEmpty()) {
            String msg = "Missing " + COLLECTION_CONFIG
                    + " or the config file is empty.";
            throw new IllegalArgumentException(msg);
        } else {
            final List<String> values = props.getProperty(collection.toUpperCase());
            if (values == null || values.isEmpty()) {
                String msg = "Missing an entry in the config file for collection " + collection + ".";
                throw new IllegalArgumentException(msg);
            } else {
                String prefixes = "";
                for (String value : values) {
                    prefixes = prefixes + "'" + value + "',";
                }

                return prefixes.substring(0, prefixes.length() - 1);
            }
        }
    }

    private URL getLocateFilesURL(URI artifactURI) {
        try {
            RegistryClient rc = new RegistryClient();
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            if (authMethod == null) {
                authMethod = AuthMethod.ANON;
            }

            URL serviceURL = rc.getServiceURL(locatorService, Standards.SI_FILES, authMethod);
            return new URL(serviceURL.toExternalForm() + "/" + artifactURI.toASCIIString());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        } catch (Throwable t) {
            String message = "Failed to initialize storage inventory URLs";
            throw new RuntimeException(message, t);
        }
    }

    @Override
    public void processResults(long total, long successes, long totalElapsedTime, long totalBytes, int threads) {
        // do nothing - this method was requested by ESAC on the ArtifactStore interface,
        //              this method does not apply to the cadc-artifact-sync service.
    }

}
