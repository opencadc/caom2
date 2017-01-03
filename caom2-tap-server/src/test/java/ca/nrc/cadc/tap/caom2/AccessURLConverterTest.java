package ca.nrc.cadc.tap.caom2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import org.apache.log4j.Level;

/**
 *
 * @author pdowler
 */
public class AccessURLConverterTest
{
    private static final Logger log = Logger.getLogger(AccessURLConverterTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap.caom2", Level.INFO);
    }

    public AccessURLConverterTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public final void testNotFound()
    {
        String[] queries = new String[] { "select foo from caom2.Artifact", "select a.foo from caom2.Artifact as a", };
        try
        {
            for (int t = 0; t < queries.length; t++)
            {
                TestUtil.job.getParameterList().clear();
                String query = queries[t];
                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.debug("testNotFound, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.debug("testNotFound, after: " + sql);

                sql = sql.toLowerCase();
                assertTrue("testNotFound: no change", sql.equalsIgnoreCase(query));
            }
        }
        catch (Throwable t)
        {
            log.error("testNotFound", t);
            fail();
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    @Test
    public final void testFoundNoAlias()
    {
        String[] queries = new String[] { "select foo,accessURL from caom2.Artifact", "select foo,accessURL,bar from caom2.Artifact" };
        try
        {
            for (int t = 0; t < queries.length; t++)
            {
                TestUtil.job.getParameterList().clear();
                String query = queries[t];
                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.debug("testFoundNoAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.debug("testFoundNoAlias, after: " + sql);

                assertTrue("testFoundNoAlias: !accessURL", !sql.contains("accessURL"));
                assertTrue("testFoundNoAlias: uri", sql.contains("uri"));
            }
        }
        catch (Throwable t)
        {
            log.error("testFoundNoAlias", t);
            fail();
        }
    }

    @Test
    public final void testFoundWithAlias()
    {
        String[] queries = new String[] { "select a.foo,a.accessURL from caom2.Artifact as a",
                "select a.foo,a.accessURL,a.bar from caom2.Artifact as a" };
        try
        {
            for (int t = 0; t < queries.length; t++)
            {
                TestUtil.job.getParameterList().clear();
                String query = queries[t];
                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.debug("testFoundWithAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.debug("testFoundWithAlias, after: " + sql);

                assertTrue("testFoundWithAlias: !accessURL", !sql.contains("accessURL"));
                assertTrue("testFoundWithAlias: uri", sql.contains("a.uri"));
            }
        }
        catch (Throwable t)
        {
            log.error("testFoundWithAlias", t);
            fail();
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    static class TestQuery extends AdqlQuery
    {
        @Override
        protected void init()
        {
            //super.init();
            super.navigatorList.add(new AccessURLConverter());
        }
    }
}
