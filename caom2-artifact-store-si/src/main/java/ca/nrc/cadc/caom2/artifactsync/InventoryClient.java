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
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

public class InventoryClient {

    private static final Logger log = Logger.getLogger(InventoryClient.class);

    static final String PUT_TXN_ID = "x-put-txn-id"; // request/response header
    static final String PUT_TXN_OP = "x-put-txn-op"; // request header
    static final String PUT_TXN_TOTAL_SIZE = "x-total-length"; // request header
    static final String PUT_TXN_MIN_SIZE = "x-put-segment-minbytes"; // response header
    static final String PUT_TXN_MAX_SIZE = "x-put-segment-maxbytes"; // response header

    // PUT_TXN_OP values
    static final String PUT_TXN_OP_ABORT = "abort"; // request header
    static final String PUT_TXN_OP_COMMIT = "commit"; // request header
    static final String PUT_TXN_OP_REVERT = "revert"; // request header
    static final String PUT_TXN_OP_START = "start"; // request header
    private long bytesTransferred = 0;

    private URL baseTransferURL;
    // files larger that MIN_SEGMENT_FILE_SIZE require segments, the others don't
    static long MIN_SEGMENT_FILE_SIZE = 5 * 1024L * 1024L * 1024L; // 5 GiB
    // adjustable by test code
    static long SEGMENT_SIZE_PREF = 2 * 1024L * 1024L * 1024L; // 2 GiB

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

    public void upload(Transfer transfer, URL src, FileMetadata metadata) throws TransientException,
            InterruptedException, ResourceNotFoundException, IOException {
        List<Protocol> protocols = transfer.getProtocols();
        if (transfer.getProtocols().isEmpty()) {
            throw new IllegalArgumentException("No tranfer protocols");
        }
        if (transfer.getTargets().size() != 1) {
            throw new IllegalArgumentException("invalid number of targets: expected 1, got: " + transfer.getTargets().size());
        }
        final URI targetURI = transfer.getTargets().get(0);

        // TODO: An improvement here would be to try the other protocols
        Protocol p = protocols.get(0);
        URL putEndpoint;
        try {
            putEndpoint = new URL(p.getEndpoint());
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Received bad endpoint URL", e);
        }

        log.debug("Put endpoint: " + putEndpoint);
        upload(src, metadata, putEndpoint);
    }

    /**
     * Upload file content from src to SI dest. For large files, it takes advantage of the PUT Transactions feature
     * in SI (https://github.com/opencadc/storage-inventory/blob/master/minoc/PutTransaction)
     * @param src - URL pointed to the src file. Must support HTTP Range requests
     * @param metadata - file metadata
     * @param dest - SI destination to put the file to.
     * @throws TransientException
     * @throws InterruptedException
     * @throws IOException
     * @throws ResourceNotFoundException
     */
    private static void upload(URL src, FileMetadata metadata, URL dest) throws TransientException,
            InterruptedException, IOException, ResourceNotFoundException {
        HttpGet srcInfo = new HttpGet(src, true);
        srcInfo.setHeadOnly(true);
        srcInfo.run();
        long contentLength = srcInfo.getContentLength();
        boolean acceptRages = "bytes".equalsIgnoreCase(srcInfo.getResponseHeader("Accept-Ranges"));
        if (contentLength <= MIN_SEGMENT_FILE_SIZE) {
            // no file segmentation required
            if (srcInfo.getContentMD5() != null) {
                // no transaction required
                HttpGet srcDownload = new HttpGet(src, true);
                doPrepare(srcDownload);
                runTxnRequest(dest, null, srcDownload.getInputStream(), metadata);
                return;
            }
            TxnMetadata transMeta = new TxnMetadata(PUT_TXN_OP_START, null, contentLength);
            HttpGet srcDownload = new HttpGet(src, true);
            doPrepare(srcDownload);
            HttpTransfer rsp = runTxnRequest(dest, transMeta, srcDownload.getInputStream(), metadata);
            // check the md5?
            String txnID = rsp.getResponseHeader(PUT_TXN_ID);
            TxnMetadata commitMeta = new TxnMetadata(PUT_TXN_OP_COMMIT, txnID, 0);
            runTxnRequest(dest, commitMeta, new ByteArrayInputStream(new byte[0]), metadata);
            return;
        }

        // large files that require segmentation
        if (!acceptRages) {
            throw new RuntimeException("File source must support HTTP Range requests: " + src.toString());
        }
        TxnMetadata txnMetadata = new TxnMetadata(PUT_TXN_OP_START, null, 0);
        txnMetadata.totalSize = contentLength;
        HttpTransfer rsp = runTxnRequest(dest, txnMetadata, new ByteArrayInputStream(new byte[0]), metadata);
        String txnID = rsp.getResponseHeader(PUT_TXN_ID);
        long txnMinBytes = 1;
        if (rsp.getResponseHeader(PUT_TXN_MIN_SIZE) != null) {
            txnMinBytes = Long.parseLong(rsp.getResponseHeader(PUT_TXN_MIN_SIZE));
        }
        long txnMaxBytes = contentLength;
        if (rsp.getResponseHeader(PUT_TXN_MAX_SIZE) != null) {
            txnMaxBytes = Long.parseLong(rsp.getResponseHeader(PUT_TXN_MAX_SIZE));
        }
        if (txnMaxBytes < txnMinBytes) {
            throw new RuntimeException(
                    "BUG: PUT txn start returned minBytes>maxBytes: " + txnMinBytes + " vs " + txnMaxBytes);
        }
        List<PutSegment> segments = getSegmentPlan(metadata, txnMinBytes, txnMaxBytes);
        boolean success = false;
        HttpGet srcSegment = null;
        try {
            for (PutSegment seg : segments) {
                log.debug("Sending segment " + seg.toString());
                srcSegment = new HttpGet(src, true);
                srcSegment.setRequestProperty("Range", seg.getRangeHeaderVal());
                doPrepare(srcSegment);
                TxnMetadata sendTnx = new TxnMetadata(null, txnID, seg.contentLength);
                runTxnRequest(dest, sendTnx, srcSegment.getInputStream(), metadata);
                success = true;
            }
        } catch (TransientException | IOException | InterruptedException | ResourceNotFoundException | AccessControlException pass) {
            throw pass;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error ", ex);
        } finally {
            if ((srcSegment != null) && (srcSegment.getInputStream() != null)) {
                srcSegment.getInputStream().close();
            }
            if (!success) {
                TxnMetadata abortTxn = new TxnMetadata(PUT_TXN_OP_ABORT, txnID, 0);
                runTxnRequest(dest, abortTxn, null, metadata);
            }
        }
        // commit trans
        TxnMetadata commitTxn = new TxnMetadata(PUT_TXN_OP_COMMIT, txnID, 0);
        runTxnRequest(dest, commitTxn, new ByteArrayInputStream(new byte[0]), metadata);
    }

    static class PutSegment {
        long start;
        long end;
        long contentLength;

        public String getRangeHeaderVal() {
            return "bytes=" + start + "-" + end;
        }

        @Override
        public String toString() {
            return PutSegment.class.getSimpleName() + "[" + start + "," + end + "," + contentLength + "]";
        }
    }

    static class TxnMetadata {
        // txn related headers
        String txnOperation;
        String txnID;
        long size;
        long totalSize;

        public TxnMetadata(String txnOperation, String txnID, long size) {
            this.txnOperation = txnOperation;
            this.txnID = txnID;
            this.size = size;
            this.totalSize = -1;
        }

        Map<String, Object> toHttpHeaders() {
            Map<String, Object> headers = new HashMap<>();
            if (txnOperation != null) {
                headers.put(PUT_TXN_OP, txnOperation);
            }
            if (txnID != null) {
                headers.put(PUT_TXN_ID, txnID);
            }
            if (totalSize != -1) {
                headers.put(PUT_TXN_TOTAL_SIZE, totalSize);
            }
            if (size != -1) {
                headers.put(HttpTransfer.CONTENT_LENGTH, String.valueOf(size));
            }
            return headers;
        }

    }

    static List<PutSegment> getSegmentPlan(FileMetadata metadata, Long minSegmentSize, Long maxSegmentSize) {
        List<PutSegment> segs = new ArrayList<>();

        long segmentSize = Math.min(SEGMENT_SIZE_PREF, metadata.getContentLength()); // client preference
        if (minSegmentSize != null) {
            segmentSize = Math.max(segmentSize, minSegmentSize);
        }
        if (maxSegmentSize != null) {
            segmentSize = Math.min(segmentSize, maxSegmentSize);
        }

        long numWholeSegments = metadata.getContentLength() / segmentSize;
        long lastSegment = metadata.getContentLength() - (segmentSize * numWholeSegments);
        long numSegments = (lastSegment > 0L ? numWholeSegments + 1 : numWholeSegments);

        for (int i = 0; i < numSegments; i++) {
            PutSegment s = new PutSegment();

            s.start = i * segmentSize;
            s.end = s.start + segmentSize - 1;
            s.contentLength = segmentSize;
            if (i + 1 == numSegments && lastSegment > 0L) {
                s.end = s.start + lastSegment - 1;
                s.contentLength = lastSegment;
            }
            segs.add(s);
        }
        return segs;
    }

    private static void doPrepare(HttpGet get) throws InterruptedException, ResourceNotFoundException, IOException {
        try {
            get.prepare();
        } catch (ResourceAlreadyExistsException ex) {
            throw new RuntimeException("BUG: unexpected failure from HttpGet: " + ex, ex);
        }
    }

    private static HttpTransfer runTxnRequest(
            URL target, TxnMetadata txnMetadata, InputStream inputStream, FileMetadata fileMetadata)
            throws InterruptedException, ResourceNotFoundException, IOException {
        if ((txnMetadata == null) && (inputStream == null)) {
            throw new IllegalArgumentException("No operation specified to run");
        }
        HttpTransfer httpOp;
        if (PUT_TXN_OP_ABORT.equals(txnMetadata.txnOperation) || PUT_TXN_OP_REVERT.equals(txnMetadata.txnOperation)) {
            httpOp = new HttpPost(target, txnMetadata.toHttpHeaders(), true);
            httpOp.setRequestProperty(PUT_TXN_ID, txnMetadata.txnID);
            httpOp.setRequestProperty(PUT_TXN_OP, txnMetadata.txnOperation);
        } else {
            httpOp = new HttpUpload(inputStream, target);
            httpOp.setBufferSize(64 * 1024); // 64KB
            long contentLength = -1;
            if (fileMetadata != null) {
                if (fileMetadata.getMd5Sum() != null) {
                    httpOp.setRequestProperty(HttpTransfer.CONTENT_MD5, fileMetadata.getMd5Sum());
                }
                if (fileMetadata.getContentLength() != null) {
                    contentLength = fileMetadata.getContentLength();
                }
                if (fileMetadata.getContentType() != null) {
                    httpOp.setRequestProperty(HttpTransfer.CONTENT_TYPE, fileMetadata.getContentType());
                }
                if (fileMetadata.getContentEncoding() != null) {
                    httpOp.setRequestProperty(HttpTransfer.CONTENT_ENCODING, fileMetadata.getContentEncoding());
                }
            }

            if (txnMetadata != null) {
                if (txnMetadata.txnOperation != null) {
                    httpOp.setRequestProperty(PUT_TXN_OP, txnMetadata.txnOperation);
                }
                if (txnMetadata.txnID != null) {
                    httpOp.setRequestProperty(PUT_TXN_ID, txnMetadata.txnID);
                }
                if (txnMetadata.totalSize != -1) {
                    httpOp.setRequestProperty(PUT_TXN_TOTAL_SIZE, String.valueOf(txnMetadata.totalSize));
                }
                if (txnMetadata.size != -1) {
                    contentLength = txnMetadata.size; // override the original file content length
                    httpOp.setRequestProperty(HttpTransfer.CONTENT_LENGTH, String.valueOf(contentLength));
                }
            }
        }
        httpOp.setLogIO(true);
        httpOp.setConnectionTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);
        httpOp.setReadTimeout(InventoryArtifactStore.DEFAULT_TIMEOUT);

        try {
            httpOp.prepare();
        } catch (TransientException | IOException | InterruptedException | ResourceNotFoundException | AccessControlException pass) {
            throw pass;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error ", ex);
        } finally {
            if (httpOp.getInputStream() != null) {
                httpOp.getInputStream().close();
            }
        }

        int respCode = httpOp.getResponseCode();
        if (respCode >= 300) {
            String msg = "Failed to upload, received response code " + respCode;
            throw new RuntimeException(msg);
        }
        return httpOp;
    }
}
