/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package ca.nrc.cadc.tap.caom2;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for the CaomReadAccessConverter.
 *
 * @author pdowler
 */
public class CaomReadAccessTest {

    private static final Logger log = Logger.getLogger(CaomReadAccessTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.argus", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.auth", Level.INFO);
    }

    static String[] ASSET_TABLES = new String[]{
        "caom2.Observation".toLowerCase(),
        "caom2.Plane".toLowerCase(),
        //"caom2.Artifact".toLowerCase(),
        //"caom2.Part".toLowerCase(),
        "caom2.Chunk".toLowerCase(),
        "caom2.ObsCore".toLowerCase(),
        "caom2.SIAv1".toLowerCase()
    };

    static String[] META_RELEASE_DATE_COLUMN = new String[]{
        "metaRelease".toLowerCase(),
        "metaRelease".toLowerCase(),
        "metaRelease".toLowerCase(),
        "metaRelease".toLowerCase(),
        "metaRelease".toLowerCase()
    };

    static String[] ASSET_COLUMN = new String[]{
        // caom2
        "obsID".toLowerCase(),
        "planeID".toLowerCase(),
        "planeID".toLowerCase(),
        "planeID".toLowerCase(),
        "planeID".toLowerCase()
    };

    static String[] META_READ_ACCESS_COLUMN = new String[]{
        "metaReadAccessGroup".toLowerCase(),
        "metaReadAccessGroup".toLowerCase(),
        "metaReadAccessGroup".toLowerCase(),
        "metaReadAccessGroup".toLowerCase(),
        "metaReadAccessGroup".toLowerCase()
    };

    static X500Principal userWithGroups;
    static X500Principal userWithNoGroups;

    static Subject subjectWithGroups;
    static Subject subjectWithNoGroups;

    public CaomReadAccessTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        // create subjects from certificate files so they have credentials and we never need to call /cred
        // in the fake GMS call
        try {
            File cf1 = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", CaomReadAccessTest.class);
            File cf2 = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", CaomReadAccessTest.class);

            CaomReadAccessTest.subjectWithNoGroups = SSLUtil.createSubject(cf1);
            CaomReadAccessTest.subjectWithNoGroups.getPrincipals().add(new HttpPrincipal("cadcregtest1"));
            CaomReadAccessTest.userWithNoGroups = subjectWithNoGroups.getPrincipals(X500Principal.class).iterator().next();
            log.debug("created subjectWithNoGroups: " + CaomReadAccessTest.subjectWithNoGroups);

            CaomReadAccessTest.subjectWithGroups = SSLUtil.createSubject(cf2);
            CaomReadAccessTest.subjectWithGroups.getPrincipals().add(new HttpPrincipal("cadcauthtest1"));
            CaomReadAccessTest.userWithGroups = subjectWithGroups.getPrincipals(X500Principal.class).iterator().next();
            log.debug("created subjectWithGroups: " + CaomReadAccessTest.subjectWithGroups);
        } finally {
        }
    }

    private class QueryConvertAction implements PrivilegedExceptionAction<String> {

        public String test;
        public String adql;
        boolean toLower;

        public QueryConvertAction(String test, String adql) {
            this(test, adql, true);
        }

        public QueryConvertAction(String test, String adql, boolean toLower) {
            this.test = test;
            this.adql = adql;
            this.toLower = toLower;
        }

        @Override
        public String run()
                throws Exception {
            return doIt(test, adql, toLower);
        }

    }

    @Test
    public final void testNoProtectedTable() {
        String method = "testNoProtectedTable";
        // these are the non-asset tables - currently non since we support prop
        // tables and columns in tap_schema
        String[] tabs = new String[]{
            "caom2.Artifact",
            "caom2.Part"
        };

        for (String tab : tabs) {
            try {
                String query = "select * from " + tab;

                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.debug(method + " before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = Subject
                        .doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));
                log.info(method + " after: " + sql);
                Assert.assertTrue(method, query.equalsIgnoreCase(sql));
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            } finally {
                TestUtil.job.getParameterList().clear();
            }
        }
    }

    @Test
    public final void testWithNoGroups() {
        String method = "testWithNoGroups";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String aCol = ASSET_COLUMN[a];
            String metaRD = META_RELEASE_DATE_COLUMN[a];
            log.info("testWithNoGroups: " + tname);
            try {
                String query = "select * from " + tname;
                String sql = Subject.doAs(CaomReadAccessTest.subjectWithNoGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                String where = sql.substring(i);
                log.info("testWithNoGroup: " + where);
                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " metarelease", where.contains("metarelease"));
                    Assert.assertFalse(method + " " + tname + " groupID", where.contains("groupid"));
                } else {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " " + aCol, where.contains(aCol + " is null"));
                    Assert.assertFalse(method + " " + tname + " groupID", where.contains("groupid"));
                }
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }

    @Test
    public final void testWithGroups() {
        String method = "testWithGroups";
        for (int a = 0; a < 1; a++) {
            String tname = ASSET_TABLES[a];
            String aCol = ASSET_COLUMN[a];
            String metaRD = META_RELEASE_DATE_COLUMN[a];
            log.info("testWithGroups: " + tname);
            try {
                String query = "select * from " + tname;
                String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                String where = sql.substring(i);
                log.debug("testWithGroups: " + where);
                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " metaRelease", where.contains("metarelease <"));
                    Assert.assertTrue(method + " " + tname + " group", where.contains("group"));
                } else {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " " + aCol, where.contains(aCol + " is null"));
                }
                assertGroupRestriction(method, where, ASSET_COLUMN[a], META_READ_ACCESS_COLUMN[a]);
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }

    @Test
    public final void testWithWhere() {
        String method = "testWithWhere";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String aCol = ASSET_COLUMN[a];
            String metaRD = META_RELEASE_DATE_COLUMN[a];
            log.info("testWithWhere: " + tname);
            try {
                String query = "select * from " + tname + " where something='FOO'";
                String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                String where = sql.substring(i);
                Assert.assertTrue(method + " something", where.contains("something"));
                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " metaRelease", where.contains("metarelease <"));
                } else {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " " + aCol, where.contains(aCol + " is null"));
                }
                Assert.assertTrue(method + " and", where.contains(" and "));

                // testing assetID is 'is null' or assetID in join to metaReadAccess table?
                assertGroupRestriction(method, where, null, META_READ_ACCESS_COLUMN[a]);
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }

    @Test
    public final void testSubQueryWhere() {
        String method = "testSubQueryWhere";
        try {
            String query = "select * from caom2.Plane as o where o.calibrationLevel = (select max(i.calibrationLevel) from caom2.Plane as i where o.obsID=i.obsID)";
            String sql = Subject.doAs(CaomReadAccessTest.subjectWithNoGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            String where = sql.substring(i + 5);
            int ii = where.indexOf("where"); // inner where
            String innerWhere = where.substring(ii);

            Assert.assertTrue(method + " metarelease", innerWhere.contains("i.metarelease < "));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public final void testWithAlias() {
        String method = "testWithAlias";
        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String tname = ASSET_TABLES[a];
            String aCol = ASSET_COLUMN[a];
            String metaRD = META_RELEASE_DATE_COLUMN[a];
            try {
                String query = "select * from " + tname + " as aa where aa.something='FOO'";
                String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

                int i = sql.indexOf("where");
                String where = sql.substring(i);
                Assert.assertTrue(method + " something", where.contains("aa.something"));
                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " metaRelease", where.contains("aa.metarelease <"));
                } else {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " aa." + aCol, where.contains("aa." + aCol + " is null"));
                }
                Assert.assertTrue(method + " and", where.contains(" and "));
                // testing assetID is 'is null' or assetID in join to metaReadAccess table?
                assertGroupRestriction(method, where, "aa", META_READ_ACCESS_COLUMN[a]);
            } catch (Exception unexpected) {
                log.error("unexpected exception", unexpected);
                Assert.fail("unexpected exception: " + unexpected);
            }
        }
    }

    @Test
    public final void testMultiTable() {
        String method = "testMultiTable";
        String fromTables = null;
        for (String tname : ASSET_TABLES) {
            if (fromTables == null) {
                fromTables = tname;
            } else {
                fromTables = fromTables + "," + tname;
            }
        }

        String query = "select * from " + fromTables + " where something='FOO'";
        try {
            String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            String where = sql.substring(i);
            Assert.assertTrue(method + " something", where.contains("something"));
            Assert.assertTrue(method + " and", where.contains(" and "));
            for (int a = 0; a < ASSET_TABLES.length; a++) {
                String tname = ASSET_TABLES[a];
                String aCol = ASSET_COLUMN[a];
                String metaRD = META_RELEASE_DATE_COLUMN[a];

                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + " metaRelease", where.contains(tname + ".metarelease <"));
                } else {
                    Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                    Assert.assertTrue(method + " " + tname + tname + "." + aCol, where.contains(tname + "." + aCol + " is null"));
                }
                assertGroupRestriction(method, where, ASSET_COLUMN[a], META_READ_ACCESS_COLUMN[a]);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public final void testUploadTable() {
        String method = "testUploadTable";
        String fromTables = "TAP_UPLOAD.foo JOIN " + ASSET_TABLES[0] + " ON a=b";

        String query = "select * from " + fromTables + " where something='FOO'";
        try {
            String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            String where = sql.substring(i);
            Assert.assertTrue(method + " something", where.contains("something"));
            Assert.assertTrue(method + " and", where.contains(" and "));
            String tname = ASSET_TABLES[0];
            String metaRD = META_RELEASE_DATE_COLUMN[0];
            String aCol = ASSET_COLUMN[0];
            if (metaRD != null) {
                Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                Assert.assertTrue(method + " " + tname + " metaRelease", where.contains(tname + ".metarelease <"));
            } else {
                Assert.assertTrue(method + " " + tname + " where", sql.contains(" where "));
                Assert.assertTrue(method + " " + tname + tname + "." + aCol, where.contains(tname + "." + aCol + " is null"));
            }
            assertGroupRestriction(method, where, ASSET_COLUMN[0], META_READ_ACCESS_COLUMN[0]);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);

        }
    }

    @Test
    public final void testMultiTableAlias() {
        String method = "testMultiTableAlias";
        String fromTables = null;

        for (int a = 0; a < ASSET_TABLES.length; a++) {
            String name = ASSET_TABLES[a];
            String tname = name + " as a" + a; // eg a0, a1, ...
            if (fromTables == null) {
                fromTables = tname;
            } else {
                fromTables = fromTables + "," + tname;
            }
        }

        String query = "select * from " + fromTables + " where something='FOO'";
        try {
            String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query));

            int i = sql.indexOf("where");
            String where = sql.substring(i);
            Assert.assertTrue(method + " something", where.contains("something"));
            Assert.assertTrue(method + " and", where.contains(" and "));
            for (int a = 0; a < ASSET_TABLES.length; a++) {
                String tname = ASSET_TABLES[a];
                String metaRD = META_RELEASE_DATE_COLUMN[a];
                String aname = "a" + a;
                if (metaRD != null) {
                    Assert.assertTrue(method + " " + tname + ".metarelease", where.contains(aname + ".metarelease"));
                } else {
                    Assert.assertFalse(method + " " + tname + ".metarelease", where.contains(aname + ".metarelease"));
                }
                assertGroupRestriction(method, where, aname, META_READ_ACCESS_COLUMN[a]);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);

        }
    }

    @Test
    public final void testCutoutQuery() {
        String method = "testCutoutQuery";
        try {
            String query = "select * from caom2.Artifact a left outer join caom2.Part p on a.artifactID=p.artifactID"
                    + " left outer join caom2.Chunk c on p.partID=c.partID where a.uri='ad:ALMA/Boom.cm.cln'";
            String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction(method, query, false));

            int i = sql.indexOf("where");
            if (i < 0) {
                i = sql.indexOf("WHERE");
            }
            Assert.assertTrue("where", (i > 0));
            String where = sql.substring(i);

            Assert.assertTrue("c.metaRelease", where.contains("c.metaRelease <"));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);

        }
    }

    private String doIt(String testName, String query, boolean toLower)
            throws Exception {
        try {
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.debug(testName + " before: " + query);
            TapQuery tq = new TestQuery();
            TestUtil.job.getParameterList().addAll(params);
            tq.setJob(TestUtil.job);
            String sql = tq.getSQL();
            log.info(testName + " after : " + sql);
            if (toLower) {
                return sql.toLowerCase();
            }
            return sql;
        } finally {
            TestUtil.job.getParameterList().clear();
        }
    }

    private void assertGroupRestriction(String method, String where, String assetCol, String metaRACol) {
        log.debug("assertGroupRestriction: " + where + " vs. " + assetCol + " and " + metaRACol);
        if (assetCol != null) {
            Assert.assertTrue(method + "[" + where + "] has asset column " + assetCol, where.contains(assetCol));
        }
        Assert.assertTrue(method + "[" + where + "] has meta table " + metaRACol, where.contains(metaRACol));
        Assert.assertTrue(method + "[" + where + "] has meta read access group 666", where.contains("666"));
        Assert.assertTrue(method + "[" + where + "] has meta read access group 777", where.contains("777"));
    }

    class TestQuery extends AdqlQuery {

        boolean auth;

        TestQuery() {
            this(true);
        }

        TestQuery(boolean auth) {
            super();
            this.auth = auth;
        }

        @Override
        protected void init() {
            // no super.init() on purpose so we only have one navigator in list
            CaomReadAccessConverter rac = new CaomReadAccessConverter();
            rac.setGMSClient(new TestUtil.TestGMSClient(subjectWithGroups));
            super.navigatorList.add(rac);
        }
    }
}
