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

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class IsDownloadableConverterTest
{
    private static final Logger log = Logger.getLogger(IsDownloadableConverterTest.class);

    public IsDownloadableConverterTest() { }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CaomReadAccessTest.setUpClass();
        Log4jInit.setLevel("ca.nrc.cadc", Level.DEBUG);
    }

    @Test
    public void testAnon()
    {
        try
        {
            String query = "select planeURI, isDownloadable(planeURI) from caom2.Plane";

            String sql = doIt("testAnon", query);
            Assert.assertFalse("sql still contains isDownloadable", sql.contains("isdownloadable"));
            String expected = "caom2.plane.datarelease <";
            Assert.assertTrue("sql contains dataRelease condition", sql.contains(expected));

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonObsCore()
    {
        try
        {
            String query = "select obs_publisher_did, isDownloadable(obs_publisher_did) from caom2.ObsCore";

            String sql = doIt("testAnon", query);
            Assert.assertFalse("sql still contains isDownloadable", sql.contains("isdownloadable"));
            String expected = "caom2.obscore.datarelease <";
            Assert.assertTrue("sql contains dataRelease condition", sql.contains(expected));

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonWithAlias()
    {
        try
        {
            String query = "select p.planeURI, isDownloadable(p.planeURI) AS uri from caom2.Plane AS p";

            String sql = doIt("testAnonWithAlias", query);
            Assert.assertFalse("sql still contains isDownloadable", sql.contains("isDownloadable"));
            String expected = "p.datarelease <";
            Assert.assertTrue("sql contains dataRelease condition", sql.contains(expected));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonWithJoins()
    {
        try
        {
            String query = "select p.planeURI, isDownloadable(p.planeURI) AS uri from caom2.Observation o join caom2.Plane AS p on o.obsID=p.obsID";

            String sql = doIt("testAnonWithJoins", query);
            Assert.assertFalse("sql still contains isDownloadable", sql.contains("isDownloadable"));
            String expected = "p.datarelease <";
            Assert.assertTrue("sql contains dataRelease condition", sql.contains(expected));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAuth()
    {
        try
        {
            log.info("testAuth START");
            String query = "select p.planeURI, isDownloadable(p.planeURI) from caom2.Plane as p";

            String sql = Subject.doAs(CaomReadAccessTest.subjectWithGroups, new QueryConvertAction("testAuth", query));
            Assert.assertFalse("[" + sql + "] still contains isDownloadable", sql.contains("isDownloadable"));
            String expected = "p.datarelease <";
            Assert.assertTrue("[" + sql + "] contains dataRelease condition", sql.contains(expected));

            expected = "p.dataReadAccessGroups @@ '666|777'::tsquery";
            expected = expected.toLowerCase();
            Assert.assertTrue("[" + sql + "] contains text search condition", sql.contains(expected));

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            log.info("testAuth DONE");
        }
    }

    private String doIt(String testName, String query)
        throws Exception
    {
        try
        {
            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter("QUERY", query));
            log.debug(testName + " before: " + query);
            TapQuery tq = new TestQuery();
            TestUtil.job.getParameterList().addAll(params);
            tq.setJob(TestUtil.job);
            String sql = tq.getSQL();
            log.info(testName + " after : " + sql);
            return sql.toLowerCase();
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    private class QueryConvertAction implements PrivilegedExceptionAction<String>
    {
        public String test;
        public String adql;
        public QueryConvertAction(String test, String adql)
        {
            this.test = test;
            this.adql = adql;
        }

        @Override
        public String run()
            throws Exception
        {
            return doIt(test, adql);
        }

    }

    class TestQuery extends AdqlQuery
    {
        boolean auth;
        TestQuery() { this(true); }
        TestQuery(boolean auth)
        {
            super();
            this.auth = auth;
        }
        @Override
        protected void init()
        {
            // no super.init() on purpose so we only have one navigator in list
            IsDownloadableConverter rac = new IsDownloadableConverter();
            if (auth) 
            {
                rac.setGMSClient(new TestUtil.TestGMSClient(CaomReadAccessTest.subjectWithGroups));
            }
            super.navigatorList.add(rac);
        }
    }
}
