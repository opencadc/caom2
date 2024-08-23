/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package org.opencadc.argus;

import ca.nrc.cadc.conformance.uws2.JobResultWrapper;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.tap.integration.TapSyncQueryTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CaomTapSyncQueryTest extends TapSyncQueryTest {

    private static final Logger log = Logger.getLogger(CaomTapSyncQueryTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.integration", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
    }

    public CaomTapSyncQueryTest() {
        super(Constants.RESOURCE_ID);

        // re-use SyncResultTest files
        File testFile = FileUtil.getFileFromResource("SyncResultTest-abs.properties", CaomTapSyncQueryTest.class);
        if (testFile.exists()) {
            File testDir = testFile.getParentFile();
            super.setPropertiesDir(testDir, "SyncResultTest");
        }
    }
    
    @Test
    public void testUploadBinary() {
        try {
            Log4jInit.setLevel("org.opencadc.argus", Level.DEBUG);
            
            File binFile = FileUtil.getFileFromResource("binary-vot.xml", CaomTapSyncQueryTest.class);
            
            String tableName = "mytab";
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("upload1", binFile);
            params.put("UPLOAD", tableName + ",param:upload1");
            params.put("LANG", "ADQL");
            params.put("QUERY", "select * from tap_upload." + tableName);

            JobResultWrapper result = createAndExecuteSyncParamJobPOST("testUploadFile", params);

            Assert.assertNull(result.throwable);
            Assert.assertEquals(200, result.responseCode);

            Assert.assertNotNull(result.syncOutput);
            ByteArrayInputStream istream = new ByteArrayInputStream(result.syncOutput);
            VOTableReader vrdr = new VOTableReader();
            VOTableDocument vot = vrdr.read(istream);

            String queryStatus = getQueryStatus(vot);
            Assert.assertNotNull("QUERY_STATUS", queryStatus);
            Assert.assertEquals("OK", queryStatus);

            // TODO: verify round-trip of testFile1 -> vot?
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    static String getQueryStatus(VOTableDocument vot) {
        VOTableResource vr = vot.getResourceByType("results");
        Assert.assertNotNull(vr);
        log.debug("found resource: " + vr.getName() + " " + vr.getType());
        String ret = null;
        // find the last QUERY_STATUS and return that because there can be a trailing
        // status when result processing fails
        for (VOTableInfo vi : vr.getInfos()) {
            if ("QUERY_STATUS".equals(vi.getName())) {
                ret = vi.getValue();
                log.warn("found status: " + ret);
            }
        }
        log.warn("return status: " + ret);
        return ret;
    }
}
