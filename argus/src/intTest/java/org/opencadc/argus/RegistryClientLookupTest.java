/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2014.                            (c) 2014.
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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.tap.TapClient;

/**
 * Half-decent test that authenticated queries work.
 *
 * @author pdowler
 */
public class RegistryClientLookupTest {

    private static final Logger log = Logger.getLogger(RegistryClientLookupTest.class);

     static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.integration", Level.INFO);
    }

    static final URI ARGUS_RESOURCE_ID = Constants.RESOURCE_ID;

    private final TapClient tapClient;
    private final RegistryClient regClient = new RegistryClient();
    private final Subject subject;
    final Map<String, Object> queryParams = new TreeMap<>();

    public RegistryClientLookupTest() throws Exception {
        queryParams.put("RUNID", "RegistryClientLookupTest");
        File cf = FileUtil.getFileFromResource(Constants.PEM_FILE, RegistryClientLookupTest.class);
        subject = SSLUtil.createSubject(cf);
        this.tapClient = new TapClient(ARGUS_RESOURCE_ID);
    }

    @Test
    public void testAnonBase() {
        try {
            URL url = tapClient.getAsyncURL(Standards.SECURITY_METHOD_ANON);
            Assert.assertNotNull(url);

            HttpPost post = new HttpPost(url, queryParams, false);
            post.run();
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(303, post.getResponseCode());
            Assert.assertNotNull(post.getRedirectURL());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonAsync() {
        try {
            URL url = tapClient.getAsyncURL(Standards.SECURITY_METHOD_ANON);
            Assert.assertNotNull(url);

            HttpPost post = new HttpPost(url, queryParams, false);
            post.run();
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(303, post.getResponseCode());
            Assert.assertNotNull(post.getRedirectURL());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonSync() {
        try {
            URL url = tapClient.getSyncURL(Standards.SECURITY_METHOD_ANON);
            Assert.assertNotNull(url);
            HttpPost post = new HttpPost(url, queryParams, false);
            post.run();
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(303, post.getResponseCode());
            Assert.assertNotNull(post.getRedirectURL());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonTables() {
        try {
            URL url = regClient.getServiceURL(ARGUS_RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.ANON);
            Assert.assertNotNull(url);
            
            url = new URL(url.toExternalForm() + "?detail=min");
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertNull(get.getThrowable());
            Assert.assertEquals(200, get.getResponseCode());
            Assert.assertTrue(bos.size() > 0);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testX509Async() {
        try {
            URL url = tapClient.getAsyncURL(Standards.SECURITY_METHOD_CERT);
            Assert.assertNotNull(url);
            
            HttpPost post = new HttpPost(url, queryParams, false);
            Subject.doAs(subject, new RunnableAction(post));

            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(303, post.getResponseCode());
            Assert.assertNotNull(post.getRedirectURL());
            
            // TODO: get job and check ownerID
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testX509Sync() {
        try {
            URL url = tapClient.getSyncURL(Standards.SECURITY_METHOD_CERT);
            Assert.assertNotNull(url);
            
            HttpPost post = new HttpPost(url, queryParams, false);
            Subject.doAs(subject, new RunnableAction(post));

            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(303, post.getResponseCode());
            Assert.assertNotNull(post.getRedirectURL());
            
            // TODO: get job and check ownerID 
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // internal log control
    @Test
    public void testLogControl() {
        try {
            URL url = regClient.getServiceURL(ARGUS_RESOURCE_ID, Standards.LOGGING_CONTROL_10, AuthMethod.CERT);

            Assert.assertNotNull(url);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertNotNull(get.getThrowable());
            Assert.assertEquals(403, get.getResponseCode());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
