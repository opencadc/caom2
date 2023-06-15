
package org.opencadc.argus;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.integration.TapAsyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class CaomTapAsyncUploadTest extends TapAsyncUploadTest {

    private static final Logger log = Logger.getLogger(CaomTapAsyncUploadTest.class);

    // prod CADC minoc
    static final URI STORAGE_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/cadc/minoc");

    static {
        Log4jInit.setLevel("ca.nrc.cadc.argus.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
        //Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }

    public CaomTapAsyncUploadTest() {
        super(URI.create("ivo://cadc.nrc.ca/argus"));
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CaomTapSyncUploadTest.class);
        setTestFile(f);
        setTestURL(getVOTableURL(f));
    }

    static URL getVOTableURL(File f) {
        try {
            String tableName = "tab_" + System.currentTimeMillis();

            String certFile = "tmpops.pem";
            File cert = FileUtil.getFileFromResource(certFile, CaomTapAsyncUploadTest.class);
            Subject subject = SSLUtil.createSubject(cert);

            RegistryClient reg = new RegistryClient();
            URL storageURL = reg.getServiceURL(STORAGE_RESOURCE_ID, Standards.SI_FILES, AuthMethod.CERT);
            URL putURL = new URL(storageURL.toExternalForm() + "/tmp:argus-int-test/" + tableName + ".xml");
            HttpUpload httpUpload = new HttpUpload(f, putURL);
            httpUpload.setRequestProperty(HttpTransfer.CONTENT_TYPE, "text/xml");
            Subject.doAs(subject, new RunnableAction(httpUpload));

            // Error during the upload, throw an exception.
            if (httpUpload.getThrowable() != null) {
                throw new RuntimeException("setup: failed to store VOTable upload ", httpUpload.getThrowable());
            }

            return putURL;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate put URL", ex);
        }
    }
}
