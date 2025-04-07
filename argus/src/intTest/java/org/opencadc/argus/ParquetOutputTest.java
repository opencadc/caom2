/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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
************************************************************************
*/

package org.opencadc.argus;

import ca.nrc.cadc.dali.tables.parquet.ParquetReader;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import static org.opencadc.argus.AuthQueryTest.syncURL;
import org.opencadc.tap.TapClient;

/**
 *
 * @author pdowler
 */
public class ParquetOutputTest {
    private static final Logger log = Logger.getLogger(ParquetOutputTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.argus", Level.INFO);
    }

    public ParquetOutputTest() { 
    }
    
    @Test
    public void testParquetOutput() throws Exception {
        TapClient tap = new TapClient(Constants.RESOURCE_ID);
        final URL tapURL = tap.getSyncURL(Standards.SECURITY_METHOD_CERT);
        log.info(" sync: " + syncURL);
        
        // hits duplicate column name bug in ParquetWriter:
        //String adql = "select top 10 * from caom2.Observation o join caom2.Plane p on o.obsID=p.obsID";
        
        String adql = "select top 10 * from caom2.Plane";
        log.info("query: " + adql);
        Map<String, Object> params = new TreeMap<>();
        params.put("LANG", "ADQL");
        params.put("QUERY", adql);
        params.put("RESPONSEFORMAT", "parquet");
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpPost httpPost = new HttpPost(tapURL, params, out);
        httpPost.run();
        log.info("response: " + httpPost.getResponseCode() + " " + httpPost.getThrowable());
        
        if (httpPost.getThrowable() != null) {
            log.error("Post failed", httpPost.getThrowable());
            Assert.fail("exception on post: " + httpPost.getThrowable());
        }

        int code = httpPost.getResponseCode();
        Assert.assertEquals(200, code);

        String contentType = httpPost.getContentType();
        Assert.assertEquals("application/vnd.apache.parquet", contentType);
        
        extractVOTableFromOutputStream(out, adql);
    }
    
    private static VOTableTable extractVOTableFromOutputStream(ByteArrayOutputStream out, String adql) throws IOException {
        ParquetReader reader = new ParquetReader();
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        ParquetReader.TableShape readerResponse = reader.read(inputStream);

        log.info(readerResponse.getColumnCount() + " columns, " + readerResponse.getRecordCount() + " records");

        Assert.assertTrue(readerResponse.getRecordCount() > 0);
        Assert.assertTrue(readerResponse.getColumnCount() > 0);

        VOTableDocument voTableDocument = readerResponse.getVoTableDocument();

        Assert.assertNotNull(voTableDocument.getResources());

        VOTableResource results = voTableDocument.getResourceByType("results");
        Assert.assertNotNull(results);

        boolean queryFound = false;
        boolean queryStatusFound = false;

        for (VOTableInfo voTableInfo : results.getInfos()) {
            if (voTableInfo.getName().equals("QUERY")) {
                queryFound = true;
                Assert.assertEquals(adql, voTableInfo.getValue());
            } else if (voTableInfo.getName().equals("QUERY_STATUS")) {
                queryStatusFound = true;
                Assert.assertEquals("OK", voTableInfo.getValue());
            }
        }

        Assert.assertTrue(queryFound);
        Assert.assertTrue(queryStatusFound);

        Assert.assertNotNull(results.getTable());
        Assert.assertEquals(readerResponse.getColumnCount(), results.getTable().getFields().size());
        return results.getTable();
    }
}
