
package org.opencadc.argus;

import ca.nrc.cadc.tap.integration.TapSyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class CaomTapSyncUploadTest extends TapSyncUploadTest {

    private static final Logger log = Logger.getLogger(CaomTapSyncUploadTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.argus.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
        //Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }

    public CaomTapSyncUploadTest() {
        super(Constants.RESOURCE_ID);
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", CaomTapSyncUploadTest.class);
        setTestFile(f);
        setTestURL(CaomTapAsyncUploadTest.getVOTableURL(f));
    }
}
