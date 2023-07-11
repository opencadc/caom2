/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package org.opencadc.torkeep;

import ca.nrc.cadc.ac.UserNotFoundException;
import ca.nrc.cadc.ac.client.GMSClient;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.compute.CaomWCSValidator;
import ca.nrc.cadc.caom2.compute.ComputeUtil;
import ca.nrc.cadc.caom2.persistence.DeletedEntityDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.caom2.xml.ObservationParsingException;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import com.csvreader.CsvWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.permissions.client.PermissionsClient;

/**
 * @author pdowler
 */
public abstract class RepoAction extends RestAction {
    private static final Logger log = Logger.getLogger(RepoAction.class);

    public static final int MAX_LIST_SIZE = 100000;

    protected ObservationURI observationURI;
    protected boolean computeMetadata;
    private String collection;
    private transient CollectionsConfig collectionsConfig;
    private transient ObservationDAO observationDAO;
    private transient DeletedEntityDAO deletedEntityDAO;

    protected RepoAction() {
    }

    private void initTarget() {
        log.debug("initTarget collection: " + this.collection);
        if (this.collection == null) {
            String path = syncInput.getPath();
            log.debug("syncInput path: " + path);
            if (path != null) {
                String[] parts = path.split("/");
                this.collection = parts[0];
                log.debug("path collection: " + parts[0]);
                if (parts.length > 1) {
                    String suri = "caom:" + path;
                    log.debug("path uri: " + suri);
                    try {
                        this.observationURI = new ObservationURI(new URI(suri));
                    } catch (URISyntaxException | IllegalArgumentException ex) {
                        throw new IllegalArgumentException("invalid input: " + suri, ex);
                    }
                }
            }
        }
    }

    // used by GetAction and GetDeletedAction
    protected void doGetCollectionList() throws Exception {
        log.debug("START: (collection list)");
        syncOutput.setHeader("Content-Type", "text/tab-separated-values");

        // Write out a single column as one entry per row
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        OutputStreamWriter out = new OutputStreamWriter(bc, StandardCharsets.US_ASCII);
        CsvWriter writer = new CsvWriter(out, '\t');
        CollectionsConfig cc = getConfigs();
        for (CollectionEntry entry : cc.getConfigs()) {
            writer.write(entry.getCollection());
            writer.endRecord();
        }
        writer.flush();
        logInfo.setBytes(bc.getByteCount());
    }

    protected class InputParams {
        // default values if called with no params
        public Integer maxrec = MAX_LIST_SIZE;
        public boolean ascending = true;
        public Date start = null;
        public Date end = null;
    }
    
    /**
     * Get InputParams object with listing output control parameters.
     * 
     * @return 
     */
    protected InputParams getInputParams() {
        InputParams ret = new InputParams();

        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        String str = null;
        try {
            str = syncInput.getParameter("maxrec");
            if (str != null) {
                int m = Integer.parseInt(str);
                if (m <= 0) {
                    throw new IllegalArgumentException("invalid maxrec value: " + m + ", maxrec must be > 0");
                }
                if (m < ret.maxrec) {
                    ret.maxrec = m;
                }
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid maxrec value: " + str, ex);
        }
        
        try {
            str = syncInput.getParameter("start");
            if (str != null) {
                ret.start = df.parse(str);
            }

            str = syncInput.getParameter("end");
            if (str != null) {
                ret.end = df.parse(str);
            }
        } catch (ParseException ex) {
            throw new IllegalArgumentException("invalid date format: " + str, ex);
        }
        
        str = syncInput.getParameter("order");
        if (str != null) {
            if (str.equals("desc")) {
                ret.ascending = false;
            } else if (!str.equals("asc")) {
                throw new IllegalArgumentException("invalid order value: " + str);
            }
        }
            
        return ret;
    }
    
    // return uri for get-observation, null for get-list, and throw for invalid
    protected ObservationURI getObservationURI() {
        initTarget();
        return this.observationURI;
    }

    // return the specified collection or throw for invalid
    protected String getCollection() {
        initTarget();
        return this.collection;
    }

    private Map<String, Object> getDAOConfig(String collection) throws IOException {
        CollectionsConfig collectionsConfig = getConfigs();
        CollectionEntry collectionEntry = collectionsConfig.getConfig(collection);
        if (collectionEntry != null) {
            this.computeMetadata = collectionEntry.isComputeMetadata();
        }
        return TorkeepInitAction.getDAOConfig();
    }
    
    protected ObservationDAO getDAO() throws IOException {
        if (this.observationDAO == null) {
            this.observationDAO = new ObservationDAO();
            this.observationDAO.setConfig(getDAOConfig(getCollection()));
        }
        return this.observationDAO;
    }

    protected DeletedEntityDAO getDeletedEntityDAO() throws IOException {
        if (this.deletedEntityDAO == null) {
            this.deletedEntityDAO = new DeletedEntityDAO();
            this.deletedEntityDAO.setConfig(getDAOConfig(getCollection()));
        }
        return this.deletedEntityDAO;
    }
    
    // read the input stream (POST and PUT) and extract the observation from the XML
    // document
    protected Observation getInputObservation() throws ObservationParsingException {
        Object o = syncInput.getContent(ObservationInlineContentHandler.ERROR_KEY);
        if (o != null) {
            ObservationParsingException ex = (ObservationParsingException) o;
            throw new IllegalArgumentException("invalid input: " + getObservationURI() +
                    " reason: " + ex.getMessage(), ex);
        }
        Object obs = this.syncInput.getContent(ObservationInlineContentHandler.CONTENT_KEY);
        if (obs != null) {
            return (Observation) obs;
        }
        return null;
    }

    /**
     * Check if the caller can read the resource.
     *
     * @throws AccessControlException
     * @throws java.security.cert.CertificateException
     * @throws ca.nrc.cadc.net.ResourceNotFoundException
     * @throws java.io.IOException
     */
    protected void checkReadPermission() throws AccessControlException,
        CertificateException, ResourceNotFoundException, IOException {
        if (!readable) {
            if (!writable) {
                throw new IllegalStateException(STATE_OFFLINE_MSG);
            }
            throw new IllegalStateException(STATE_READ_ONLY_MSG);
        }

        // config for the collection
        CollectionsConfig collectionsConfig = getConfigs();
        CollectionEntry collectionEntry = collectionsConfig.getConfig(getCollection());
        if (collectionEntry == null) {
            throw new ResourceNotFoundException("not found: " + getObservationURI());
        }

        URI grantURI;
        if (getObservationURI() != null) {
            grantURI = getObservationURI().getURI();
        } else {
            grantURI = URI.create("caom://" + getCollection());
        }

        for (URI grantProvider : collectionsConfig.getGrantProviders()) {
            log.debug("checking grant provider: " + grantProvider);
            PermissionsClient permissionsClient = new PermissionsClient(grantProvider);
            ReadGrant readGrant = permissionsClient.getReadGrant(grantURI);
            if (readGrant != null) {
                if (readGrant.isAnonymousAccess()) {
                    return;
                }
            }
        }

        try {
            if (CredUtil.checkCredentials()) {
                for (URI grantProvider : collectionsConfig.getGrantProviders()) {
                    PermissionsClient pc = new PermissionsClient(grantProvider);
                    WriteGrant writeGrant = pc.getWriteGrant(getObservationURI().getURI());
                    if (writeGrant != null) {
                        for (GroupURI groupURI : writeGrant.getGroups()) {
                            GMSClient gmsClient = new GMSClient(groupURI.getServiceID());
                            if (gmsClient.isMember(groupURI.getName())) {
                                return;
                            }
                        }
                    }
                }

                for (URI grantProvider : collectionsConfig.getGrantProviders()) {
                    PermissionsClient pc = new PermissionsClient(grantProvider);
                    ReadGrant readGrant = pc.getReadGrant(getObservationURI().getURI());
                    if (readGrant != null) {
                        for (GroupURI groupURI : readGrant.getGroups()) {
                            GMSClient gmsClient = new GMSClient(groupURI.getServiceID());
                            if (gmsClient.isMember(groupURI.getName())) {
                                return;
                            }
                        }
                    }
                }
            }
        } catch (AccessControlException ex) {
            throw new AccessControlException(
                "permission denied (credentials not found): " + getCollection());
        } catch (UserNotFoundException ex) {
            throw new AccessControlException(
                "permission denied (user not found): " + getCollection());
        }
        throw new AccessControlException("permission denied: " + getCollection());
    }

    /**
     * Check if the caller can create or modify the resource.
     *
     * @throws AccessControlException
     * @throws java.security.cert.CertificateException
     * @throws ca.nrc.cadc.net.ResourceNotFoundException
     * @throws java.io.IOException
     */
    protected void checkWritePermission() throws AccessControlException,
        CertificateException, ResourceNotFoundException, IOException {
        if (!writable) {
            if (readable) {
                throw new IllegalStateException(STATE_READ_ONLY_MSG);
            }
            throw new IllegalStateException(STATE_OFFLINE_MSG);
        }

        CollectionsConfig cc = getConfigs();
        CollectionEntry entry = cc.getConfig(getCollection());
        if (entry == null) {
            throw new ResourceNotFoundException("not found: " + getObservationURI());
        }

        URI grantURI;
        if (getObservationURI() != null) {
            grantURI = getObservationURI().getURI();
        } else {
            grantURI = URI.create("caom://" + getCollection());
        }

        try {
            if (CredUtil.checkCredentials()) {
                for (URI grantProvider : cc.getGrantProviders()) {
                    PermissionsClient pc = new PermissionsClient(grantProvider);
                    WriteGrant writeGrant = pc.getWriteGrant(grantURI);
                    if (writeGrant != null) {
                        for (GroupURI groupURI : writeGrant.getGroups()) {
                            GMSClient gms = new GMSClient(groupURI.getServiceID());
                            if (gms.isMember(groupURI.getName())) {
                                return;
                            }
                        }
                    }
                }
            }
        } catch (AccessControlException ex) {
            throw new AccessControlException(
                "permission denied (credentials not found): " + getCollection());
        } catch (UserNotFoundException ex) {
            throw new AccessControlException(
                "permission denied (user not found): " + getCollection());
        }

        throw new AccessControlException("permission denied: " + getCollection());
    }

    protected void validate(Observation obs) 
        throws AccessControlException, IOException, TransientException {
        try {
            if (this.computeMetadata) {
                for (Plane p : obs.getPlanes()) {
                    ComputeUtil.clearTransientState(p);
                }
            }

            CaomValidator.validate(obs);
            for (Plane pl : obs.getPlanes()) {
                for (Artifact a : pl.getArtifacts()) {
                    CaomWCSValidator.validate(a);
                }
            }

            if (this.computeMetadata) {
                String ostr = obs.getCollection() + "/" + obs.getObservationID();
                String cur = ostr;
                try {
                    for (Plane p : obs.getPlanes()) {
                        cur = ostr + "/" + p.getProductID();
                        ComputeUtil.computeTransientState(obs, p);
                    }
                } catch (Error er) {
                    throw new RuntimeException("failed to compute metadata for plane " + cur, er);
                } catch (Exception ex) {
                    throw new IllegalArgumentException(
                        "failed to compute metadata for plane " + cur, ex);
                }
            }
            
//            ReadAccessGenerator ratGenerator = getReadAccessTuplesGenerator(getCollection(), raGroupConfig);
//            if (ratGenerator != null) {
//                ratGenerator.generateTuples(obs);
//            }
        } catch (IllegalArgumentException ex) {
            log.debug(ex.getMessage(), ex);
            // build complete error cause message because rest api only outputs the message,
            // not the stack trace
            StringBuilder sb = new StringBuilder();
            Throwable cause = ex;
            while (cause != null) {
                sb.append("|").append("cause: ").append(cause.getMessage());
                cause = cause.getCause();
            }
            throw new IllegalArgumentException("invalid input: " + getObservationURI() + " " + sb.toString(), ex);
        }
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

//    /**
//     * Returns an instance of ReadAccessTuplesGenerator if the read access group are configured.
//     * Returns null otherwise.
//     *
//     * @param collection
//     * @param raGroupConfig read access group data from configuration file
//     * @return read access generator plugin or null if not configured
//     */
//    protected ReadAccessGenerator getReadAccessTuplesGenerator(String collection, Map<String, Object> raGroupConfig) {
//        ReadAccessGenerator ratGenerator = null;
//
//        if (raGroupConfig.get(ReadAccessGenerator.STAFF_GROUP_KEY) != null
//                || raGroupConfig.get(ReadAccessGenerator.OPERATOR_GROUP_KEY) != null) {
//            ratGenerator = new ReadAccessGenerator(collection, raGroupConfig);
//        }
//
//        return ratGenerator;
//    }

    protected CollectionsConfig getConfigs() {
        if (this.collectionsConfig == null) {
            String jndiKey = TorkeepInitAction.JNDI_CONFIG_KEY;
            log.debug("jndiKey: " + jndiKey);
            try {
                log.debug("retrieving config via JNDI: " + jndiKey);
                javax.naming.Context initContext = new javax.naming.InitialContext();
                this.collectionsConfig =  (CollectionsConfig) initContext.lookup(jndiKey);
            } catch (Exception ex) {
                throw new IllegalStateException("failed to find config via JNDI: lookup failed", ex);
            }
        }
        return this.collectionsConfig;
    }
    
}
