/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.repo.integration;


import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for the deleted observation resource.
 * 
 * @author pdowler
 */
public class CaomRepoDeletedTest extends CaomRepoBaseIntTests {
    private static final Logger log = Logger.getLogger(CaomRepoDeletedTest.class);

    private final URI resourceID;
    
    public CaomRepoDeletedTest(URI resourceID, String pem1, String pem2, String pem3) {
        super(resourceID, Standards.CAOM2REPO_DEL_23, pem1, pem2, pem3);
        this.resourceID = resourceID;
    }
    
    @Test
    public void testListCollections() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(new URL(baseHTTPURL), bos);
            get.run();
            
            Assert.assertNull("testListCollections", get.getThrowable());
            Assert.assertEquals("testListCollections", 200,  get.getResponseCode());
            Assert.assertEquals("testListCollections", "text/tab-separated-values", get.getContentType());
            
            boolean found = false;
            LineNumberReader r = new LineNumberReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray())));
            String line = r.readLine();
            while (line != null) {
                if (TEST_COLLECTION.equals(line)) {
                    found = true;
                }
                line = r.readLine();
            }
            Assert.assertTrue("testListCollections: found " + TEST_COLLECTION, found);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testListDeletedDenied() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(new URL(baseHTTPURL + "/" + TEST_COLLECTION), bos);
            get.run();
            
            Assert.assertNotNull("testListDeletedDenied", get.getThrowable());
            Assert.assertEquals("testListDeletedDenied permission denied", 403,  get.getResponseCode());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    // need a separate instance of base class to mess with content
    private class HackRepoClient extends CaomRepoBaseIntTests {
        HackRepoClient(URI resourceID) {
            super(resourceID, Standards.CAOM2REPO_OBS_23, null, null, null);
        }
    }
    
    @Test
    public void testListDeletedSuccess() {
        try {
            // setup
            HackRepoClient rc = new HackRepoClient(resourceID);
            final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            Observation obs = new SimpleObservation(TEST_COLLECTION, "testListDeletedSuccess-" + UUID.randomUUID().toString());
            log.info("setup: " + obs.getURI());
            
            final long t1 = System.currentTimeMillis();
            rc.putObservation(obs, subject1, 200, "OK", null);
            obs = rc.getObservation(obs.getURI().getURI().toASCIIString(), subject1, 200, null, null);
            Assert.assertNotNull("test setup", obs);
            final long t2 = System.currentTimeMillis();
            final long st = obs.getMaxLastModified().getTime(); // between t1 and t2 but offset by client-server time diff
            
            rc.deleteObservation(obs.getURI().getURI().toASCIIString(), subject1, null, null);
            final long t3 = System.currentTimeMillis();
            final long delta = t3 - t1; // clock diff between client and server
            
            Date inserted = obs.getMaxLastModified();
            Date afterDelete = new Date(st + delta);
            
            StringBuilder sb = new StringBuilder();
            sb.append(baseHTTPSURL).append("/").append(TEST_COLLECTION);
            sb.append("?").append("maxrec=1");
            sb.append("&").append("start=").append(df.format(inserted));
            sb.append("&").append("end=").append(df.format(afterDelete));
            
            URL url = new URL(sb.toString());
            log.info("testListDeletedSuccess: " + url.toExternalForm());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            Subject.doAs(subject1, new RunnableAction(get));
            
            Assert.assertNull(get.getThrowable());
            Assert.assertEquals(200,  get.getResponseCode());
            
            int num = 0;
            LineNumberReader r = new LineNumberReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray())));
            String line = r.readLine();
            while (line != null) {
                num++;
                String[] tokens = line.split("[\t]");
                Assert.assertEquals("num tokens", 4, tokens.length);

                UUID id = UUID.fromString(tokens[0]);
                Assert.assertNotNull("id", id);
                
                String collection = tokens[1];
                Assert.assertEquals("collection", TEST_COLLECTION, collection);
                
                String observationID = tokens[2];
                Assert.assertEquals("observationID", obs.getObservationID(), observationID);
                
                Date lastModified = df.parse(tokens[3]);
                Assert.assertTrue(inserted.compareTo(lastModified) < 0);
                Assert.assertTrue(afterDelete.compareTo(lastModified) > 0);
                
                log.info("[testListDeletedSuccess] " + id + " " 
                        + collection +  " " + observationID + " " + df.format(lastModified));
                
                line = r.readLine();
            }
            Assert.assertEquals("one line", 1, num); // if zero then test setup assumptions fail
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
