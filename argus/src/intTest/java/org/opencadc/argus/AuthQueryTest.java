/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
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

package org.opencadc.argus;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencadc.tap.TapClient;

/**
 * Half-decent test that authenticated queries work.
 *
 * @author pdowler
 */
public class AuthQueryTest {

    private static final Logger log = Logger.getLogger(AuthQueryTest.class);

    static final Subject noauthSubject;
    static final Subject authSubject;
    static URL syncURL;
    static URL asyncURL;

    static {
        Log4jInit.setLevel("org.opencadc.argus", Level.INFO);

        File cf = FileUtil.getFileFromResource(Constants.PEM_FILE, AuthQueryTest.class);
        authSubject = SSLUtil.createSubject(cf);
        log.debug("created subjectWithGroups: " + authSubject);
        
        cf = FileUtil.getFileFromResource(Constants.UNPRIV_PEM_FILE, AuthQueryTest.class);
        noauthSubject = SSLUtil.createSubject(cf);
        log.debug("created subjectNoGroups: " + noauthSubject);
        
        try {
            TapClient tap = new TapClient(Constants.RESOURCE_ID);
            syncURL = tap.getSyncURL(Standards.SECURITY_METHOD_CERT);
            asyncURL = tap.getAsyncURL(Standards.SECURITY_METHOD_CERT);
            log.info(" sync: " + syncURL);
            log.info("async: " + asyncURL);
        } catch (Exception ex) {
            log.error("TEST SETUP BUG: failed to find TAP URL", ex);
        }
    }

    private class SyncQueryAction implements PrivilegedExceptionAction<VOTableDocument> {

        private URL url;
        private Map<String, Object> params;

        public SyncQueryAction(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
        }

        @Override
        public VOTableDocument run()
                throws Exception {
            log.info("starting sync request: " + url);
            HttpPost doit = new HttpPost(url, params, true);
            doit.prepare();
            Assert.assertEquals(200, doit.getResponseCode());

            String contentType = doit.getContentType();
            log.info("result contentType: " + contentType);
            Assert.assertEquals("application/x-votable+xml", contentType);

            VOTableReader r = new VOTableReader();
            VOTableDocument doc = r.read(doit.getInputStream());
            
            return doc;
        }

    }

    private class AsyncQueryAction implements PrivilegedExceptionAction<Job> {

        private URL url;
        private Map<String, Object> params;

        public AsyncQueryAction(URL url, Map<String, Object> params) {
            this.url = url;
            this.params = params;
        }

        @Override
        public Job run()
                throws Exception {
            log.info("create async job: " + url);
            HttpPost doit = new HttpPost(url, params, false);
            doit.prepare();
            int code = doit.getResponseCode();
            Assert.assertEquals(303, code);

            URL jobURL = doit.getRedirectURL();
            log.info("execute job: " + jobURL);
            URL phaseURL = new URL(jobURL.toString() + "/phase");
            Map<String, Object> nextParams = new HashMap<String, Object>();
            nextParams.put("PHASE", "RUN");
            doit = new HttpPost(phaseURL, nextParams, false);
            doit.prepare();

            JobReader jr = new JobReader();
            Job job = null;
            URL waitURL = new URL(jobURL.toExternalForm() + "?WAIT=30");
            boolean done = false;
            while (!done) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                log.info("polling job: " + waitURL);
                HttpGet w = new HttpGet(waitURL, out);
                w.run();
                job = jr.read(new ByteArrayInputStream(out.toByteArray()));
                ExecutionPhase ep = job.getExecutionPhase();
                done = !ep.isActive();
            }
            Assert.assertNotNull("job", job);

            return job;
        }

    }

    // TODO: the CaomReadAccessConverter is currently disabled
    @Test
    @Ignore
    public void testAuthQuery() {
        try {
            Date d = new Date(System.currentTimeMillis() + 365 * 86400 * 1000); // one year in future
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            String future = df.format(d);
            String adql = "SELECT top 1 uri,metaRelease from caom2.Observation where metaRelease IS NULL OR metaRelease > '" + future + "'";

            Map<String, Object> params = new TreeMap<String, Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);

            VOTableDocument doc = Subject.doAs(noauthSubject, new SyncQueryAction(syncURL, params));
            log.info("result: " + doc);
            Assert.assertNotNull(doc);
            
            VOTableResource vr = doc.getResourceByType("results");
            VOTableTable vt = vr.getTable();
            TableData td = vt.getTableData();
            
            // NOTE: the proxy cert is for CADCAuthtest1 which does not belong to
            // any groups that can access proprietary metadata so we expect no rows
            Iterator<List<Object>> iter = td.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // this test requires write permission to a working VOSpace
    // currently hard-coded to the production CADC vault service
    @Test
    public void testVOSAuthQuery() {
        String username = System.getProperty("user.name");
        String dest = "vos://cadc.nrc.ca~vault/" + username + "/test/argus-testVOSAuthQuery";
        try {
            String adql = "SELECT top 1 * from tap_schema.tables";

            Map<String, Object> params = new TreeMap<String, Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);
            params.put("DEST", dest);

            Job job = Subject.doAs(authSubject, new AsyncQueryAction(asyncURL, params));
            log.info("job: " + job);
            Assert.assertTrue("job completed", job.getExecutionPhase().equals(ExecutionPhase.COMPLETED));
            for (Result r : job.getResultsList()) {
                log.info(r.getName() + ": " + r.getURI());
                if ("result".equals(r.getName())) { // spec result name since TAP-1.0
                    Assert.assertTrue("result stored in vault", r.getURI().toASCIIString().contains("vault"));
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
