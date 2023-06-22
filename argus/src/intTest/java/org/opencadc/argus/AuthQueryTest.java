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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.InvalidSignedTokenException;
import ca.nrc.cadc.auth.PrincipalExtractor;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.auth.SSOCookieCredential;
import ca.nrc.cadc.auth.SSOCookieManager;
import ca.nrc.cadc.auth.SignedToken;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Result;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Authenticator;
import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
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
public class AuthQueryTest
{
    private static final Logger log = Logger.getLogger(AuthQueryTest.class);

    private static final URI GMS_RESOURCE_IDENTIFIER_URI = URI.create("ivo://cadc.nrc.ca/gms");

    static final Subject subjectWithGroups;
    static final Principal userWithGroups;
    static URL syncCertURL;
    static URL asyncCertURL;
    static URL syncCookieURL;
    static URL asyncCookieURL;

    private static final String USERNAME = "cadcregtest1";
    private static final String PASSWORD_FILE = USERNAME + ".pass";

    Map<String, Object> params;
    Subject authSubject;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.argus.integration", Level.INFO);

        File cf = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", AuthQueryTest.class);
        subjectWithGroups = SSLUtil.createSubject(cf);
        userWithGroups = subjectWithGroups.getPrincipals(X500Principal.class).iterator().next();
        log.debug("created subjectWithGroups: " + subjectWithGroups);

        try
        {
            TapClient tap = new TapClient(Constants.RESOURCE_ID);
            syncCertURL = tap.getSyncURL(Standards.SECURITY_METHOD_CERT);
            asyncCertURL = tap.getAsyncURL(Standards.SECURITY_METHOD_CERT);
            
            syncCookieURL = tap.getSyncURL(Standards.SECURITY_METHOD_COOKIE);
            asyncCookieURL = tap.getAsyncURL(Standards.SECURITY_METHOD_COOKIE);
        }
        catch(Exception ex)
        {
            log.error("TEST SETUP BUG: failed to find TAP URL", ex);
        }
    }

    private class SyncQueryAction implements PrivilegedExceptionAction<String>
    {
        private URL url;
        private Map<String,Object> params;

        public SyncQueryAction(URL url, Map<String,Object> params)
        {
            this.url = url;
            this.params = params;
        }

        @Override
        public String run()
            throws Exception
        {
            HttpPost doit = new HttpPost(url, params, true);
            doit.run();

            if (doit.getThrowable() != null)
            {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            Assert.assertEquals(200, code);

            String contentType = doit.getResponseContentType();
            String result = doit.getResponseBody();

            log.debug("contentType: " + contentType);
            log.debug("respnse:\n" + result);

            Assert.assertEquals("application/x-votable+xml", contentType);

            return result;
        }

    }

    private class AsyncQueryAction implements PrivilegedExceptionAction<Job>
    {
        private URL url;
        private Map<String,Object> params;

        public AsyncQueryAction(URL url, Map<String,Object> params)
        {
            this.url = url;
            this.params = params;
        }

        @Override
        public Job run()
            throws Exception
        {
            HttpPost doit = new HttpPost(url, params, false);
            doit.run();

            if (doit.getThrowable() != null)
            {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            int code = doit.getResponseCode();
            Assert.assertEquals(303, code);

            URL jobURL = doit.getRedirectURL();
            
            // exec the job
            URL phaseURL = new URL(jobURL.toString() + "/phase");
            Map<String,Object> nextParams = new HashMap<String,Object>();
            nextParams.put("PHASE", "RUN");
            doit = new HttpPost(phaseURL, nextParams, false);
            doit.run();

            if (doit.getThrowable() != null)
            {
                log.error("Post failed", doit.getThrowable());
                Assert.fail("exception on post: " + doit.getThrowable());
            }

            JobReader jr = new JobReader();
            Job job = null;
            URL waitURL = new URL(jobURL.toExternalForm() + "?WAIT=30");
            boolean done = false;
            while (!done)
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpDownload w = new HttpDownload(waitURL, out);
                w.run();
                job = jr.read(new ByteArrayInputStream(out.toByteArray()));
                ExecutionPhase ep = job.getExecutionPhase();
                done = !ep.isActive();
            }
            Assert.assertNotNull("job", job);
            
            return job;
        }

    }

    @Test
    public void testAuthQuery()
    {
        try
        {
            Date d = new Date(System.currentTimeMillis() + 365*86400*1000); // one year in future
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            String future = df.format(d);
            String adql = "SELECT top 1 * from caom2.Observation where metaRelease IS NULL OR metaRelease > '"+future+"'";

            Map<String,Object> params = new TreeMap<String,Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);

            String result = Subject.doAs(subjectWithGroups, new SyncQueryAction(syncCertURL, params));
            Assert.assertNotNull(result);

            VOTableReader r = new VOTableReader();
            VOTableDocument doc = r.read(result);
            VOTableResource vr = doc.getResourceByType("results");
            VOTableTable vt = vr.getTable();
            TableData td = vt.getTableData();

            // NOTE: the proxy cert is for CADCAuthtest1 which does not belong to
            // any groups that can access proprietary metadata so we expect no rows
            Iterator<List<Object>> iter = td.iterator();
            Assert.assertFalse("no result rows", iter.hasNext());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testVOSAuthQuery()
    {
        try
        {
            String adql = "SELECT top 1 * from tap_schema.tables";

            Map<String,Object> params = new TreeMap<String,Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);
            params.put("DEST", "vos://cadc.nrc.ca~vault/CADCAuthtest1/test/argus-testVOSAuthQuery");

            Job job = Subject.doAs(subjectWithGroups, new AsyncQueryAction(asyncCertURL, params));
            log.info("job: " + job);
            Assert.assertTrue("job completed", job.getExecutionPhase().equals(ExecutionPhase.COMPLETED));
            for (Result r : job.getResultsList()) {
                log.info(r.getName() + ": " + r.getURI());
                if ("result".equals(r.getName())) { // spec result name since TAP-1.0
                    Assert.assertTrue("result stored in vault", r.getURI().toASCIIString().contains("vault"));
                }
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }



    private String getTestPassword() {
        String pwd = null;
        try {
            BufferedReader bufferReader = new BufferedReader(new FileReader(System.getenv("A") + "/etc/" + PASSWORD_FILE));
            pwd = bufferReader.readLine();
        } catch (IOException ex) {
            log.error("Password file not located in $A/etc. Using default");
        }
        return pwd;
    }

    //@Test
    public void testLoginCookie() throws InvalidSignedTokenException {

        String currentAuthenticatorClass = System.getProperty(Authenticator.class.getName());
        System.setProperty(Authenticator.class.getName(), TestAuthenticatorImpl.class.getName());
        log.debug("current authenticator class: " + currentAuthenticatorClass + ". Using class: " + TestAuthenticatorImpl.class.getName());

        try {
            String pwd = getTestPassword();
            params = new HashMap<String, Object>();
            params.put("username", USERNAME);
            params.put("password", pwd);
            log.debug("pwd: " + pwd);
            log.debug("username: " + USERNAME);

            authSubject = AuthenticationUtil.getSubject(new PrincipalExtractor()
            {
                public Set<Principal> cookiePrincipals = null;
                private SignedToken cookieToken;
                private String loginToken;
                public X509CertificateChain getCertificateChain() { return null; }
                private String domain = "";

                protected void getCookieTokens() {
                    Map<String,Object> callparams = new TreeMap<String,Object>();

                    String pwd = getTestPassword();
                    callparams = new HashMap<String, Object>();
                    callparams.put("username", USERNAME);
                    callparams.put("password", pwd);

                    log.debug("pwd: " + pwd);
                    log.debug("username: " + USERNAME);

                    try {
                        URL loginServiceUrl = new RegistryClient().getServiceURL(GMS_RESOURCE_IDENTIFIER_URI, 
                            Standards.UMS_LOGIN_01, AuthMethod.ANON);

                        log.debug("login service url: " + loginServiceUrl);
                        Assert.assertNotNull(loginServiceUrl);

                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        HttpPost post = new HttpPost(loginServiceUrl, callparams, out);
                        post.run();

                        Assert.assertNull(post.getThrowable());
                        log.debug("login post response code: " + post.getResponseCode());
                        Assert.assertEquals(200, post.getResponseCode());

                        if (post.getThrowable() != null)
                            throw new RuntimeException("login failed: " + post.getResponseCode(), post.getThrowable());
                        loginToken = out.toString();
                        log.debug("token: " + loginToken);

                        SSOCookieManager ssoCookieManager = new SSOCookieManager();
                        cookieToken = ssoCookieManager.parse(loginToken);
                        cookiePrincipals = cookieToken.getIdentityPrincipals();
                        domain = loginServiceUrl.getHost();
                    } catch (InvalidSignedTokenException ex) {
                        log.error("can't log in for  for test.", ex);
                    }
                }

                public Date getExpirationDate()
                {
                    final Calendar cal = Calendar.getInstance(DateUtil.UTC);
                    cal.add(Calendar.HOUR, SSO_COOKIE_LIFETIME_HOURS);
                    return cal.getTime();
                }

                public static final int SSO_COOKIE_LIFETIME_HOURS = 2 * 24; // in hours

                public List<SSOCookieCredential> getSSOCookieCredentials()  {
                    List<SSOCookieCredential> cookieList = new ArrayList<>();
                    SSOCookieCredential ret = new SSOCookieCredential(loginToken, domain, getExpirationDate() );
                    cookieList.add(ret);
                    log.debug("login cookie credential: " + ret);
                    return cookieList;
                }

                public Set<Principal> getPrincipals()
                {
                    if (cookiePrincipals == null) {
                        getCookieTokens();
                    }
                    return cookiePrincipals;
                }

            });

            System.clearProperty(Authenticator.class.getName());

            log.debug("tap sync cookie service url: " + syncCookieURL.toExternalForm());

            Date d = new Date(System.currentTimeMillis() + 365*86400*1000); // one year in future
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            String future = df.format(d);
            String adql = "SELECT top 1 * from caom2.Observation where metaRelease IS NULL OR metaRelease > '"+future+"'";

            Map<String,Object> params = new TreeMap<String,Object>();
            params.put("LANG", "ADQL");
            params.put("QUERY", adql);

            String queryStr = "?LANG=ADQL&QUERY=" + NetUtil.encode(adql);


            URL nextUrl = new URL(syncCookieURL.toExternalForm() + queryStr);

            ByteArrayOutputStream whoamiout = new ByteArrayOutputStream(1024);

            // HttpPost will not forward the cookie correctly on the redirect, so use
            // HttpDownload (get) for this test.
            HttpDownload httpGet = new HttpDownload(nextUrl, whoamiout);
            httpGet.setFollowRedirects(true);
            Subject.doAs(authSubject, new RunnableAction(httpGet));
            String contentType = httpGet.getContentType();
            Assert.assertNull("GET returned errors", httpGet.getThrowable());
            Assert.assertEquals("Wrong response code", 200, httpGet.getResponseCode());

            log.debug("contentType: " + contentType);

            Assert.assertEquals("application/x-votable+xml", contentType);


        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally  {
            System.clearProperty(Authenticator.class.getName());
        }
    }


}
