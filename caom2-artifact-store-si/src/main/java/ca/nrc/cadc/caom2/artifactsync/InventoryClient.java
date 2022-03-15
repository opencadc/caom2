/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileMetadata;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

public class InventoryClient {

    private static final Logger log = Logger.getLogger(InventoryClient.class);

    private URL baseTransferURL;

    public InventoryClient(URI locateServiceResourceID) {
        try {
            RegistryClient rc = new RegistryClient();
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            if (authMethod == null) {
                authMethod = AuthMethod.ANON;
            }

            baseTransferURL = rc.getServiceURL(locateServiceResourceID, Standards.SI_LOCATE, authMethod);
        } catch (Throwable t) {
            String message = "Failed to initialize storage inventory URLs";
            throw new RuntimeException(message, t);
        }
    }

    public Transfer createTransferSync(URI target, Direction direction, List<Protocol> protocolList) throws TransientException {
        Transfer transfer = new Transfer(target, direction);
        transfer.getProtocols().addAll(protocolList);
        transfer.version = VOS.VOSPACE_21;

        TransferWriter tw = new TransferWriter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            tw.write(transfer, out);
        } catch (IOException ex) {
            String msg = "Failed to write transfer to output stream. ";
            throw new RuntimeException(msg + ex.getMessage());
        }

        FileContent content = new FileContent(out.toByteArray(), "text/xml");
        HttpPost post = new HttpPost(baseTransferURL, content, false);
        post.setConnectionTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);
        post.setReadTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);
        post.run();
        if (post.getThrowable() != null) {
            if (post.getThrowable() instanceof Exception) {
                if (post.getThrowable() instanceof AccessControlException) {
                    throw (AccessControlException) post.getThrowable();
                } else {
                    throw new IllegalStateException(post.getThrowable().getMessage());
                }
            } else {
                throw new RuntimeException(post.getThrowable());
            }
        }

        int respCode = post.getResponseCode();
        if (respCode != 200 && respCode != 201) {
            String msg = "Failed to create a transfer, received response code " + respCode;
            throw new RuntimeException(msg);
        }

        try {
            TransferReader tr = new TransferReader();
            Transfer result = tr.read(post.getInputStream(), target.getScheme());
            return result;
        } catch (TransferParsingException ex) {
            throw new TransientException(ex.getMessage());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to negotiate transfer, " + ex.getMessage());
        }
    }

    public void upload(Transfer transfer, InputStream inputStream, FileMetadata metadata) throws TransientException {
        List<Protocol> protocols = transfer.getProtocols();
        if (transfer.getProtocols().isEmpty()) {
            throw new IllegalArgumentException("No tranfer protocols");
        }
        if (transfer.getTargets().size() != 1) {
            throw new IllegalArgumentException("invalid number of targets: expected 1, got: " + transfer.getTargets().size());
        }
        final URI targetURI = transfer.getTargets().get(0);

        // TODO: An improvement here would be to try the other protocols
        // in the list when one fails. This could be tricky to implement
        // while not breaking the inputStream.
        Protocol p = protocols.get(0);
        URL putEndpoint;
        try {
            putEndpoint = new URL(p.getEndpoint());
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Received bad endpoint URL", e);
        }

        log.debug("Put endpoint: " + putEndpoint);
        HttpUpload put = new HttpUpload(inputStream, putEndpoint);
        put.setConnectionTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);
        put.setReadTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);
        upload(targetURI, put, metadata);
    }

    public void upload(URI target, HttpUpload put, FileMetadata metadata) throws TransientException {
        put.setBufferSize(64 * 1024); // 64KB
        put.setLogIO(true);

        if (metadata != null) {
            if (metadata.getMd5Sum() != null) {
                put.setRequestProperty(HttpTransfer.CONTENT_MD5, metadata.getMd5Sum());
            }
            if (metadata.getContentLength() != null) {
                put.setRequestProperty(HttpTransfer.CONTENT_LENGTH, Long.toString((long) metadata.getContentLength()));
            }
            if (metadata.getContentType() != null) {
                put.setRequestProperty(HttpTransfer.CONTENT_TYPE, metadata.getContentType());
            }
            if (metadata.getContentEncoding() != null) {
                put.setRequestProperty(HttpTransfer.CONTENT_ENCODING, metadata.getContentEncoding());
            }
        }

        put.run();
        if (put.getThrowable() != null) {
            if (put.getThrowable() instanceof Exception) {
                if (put.getThrowable() instanceof AccessControlException) {
                    throw (AccessControlException) put.getThrowable();
                } else {
                    throw new IllegalStateException(put.getThrowable().getMessage());
                }
            } else {
                throw new RuntimeException(put.getThrowable());
            }
        }

        int respCode = put.getResponseCode();
        if (respCode != 200 && respCode != 201) {
            String msg = "Failed to upload, received response code " + respCode;
            throw new RuntimeException(msg);
        }

        return;
    }

}
