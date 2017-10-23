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
public class CaomSelectListConverterTest
{
    private static final Logger log = Logger.getLogger(CaomSelectListConverterTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap.caom2", Level.INFO);
    }

    public CaomSelectListConverterTest()
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
                log.info("testNotFound, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.info("testNotFound, after: " + sql);

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
    public final void testAccessURLNoAlias()
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
                log.info("testAccessURLNoAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.info("testAccessURLNoAlias, after: " + sql);

                assertTrue("testAccessURLNoAlias: !accessURL", !sql.contains("accessURL"));
                assertTrue("testAccessURLNoAlias: uri", sql.contains("uri"));
            }
        }
        catch (Throwable t)
        {
            log.error("testFoundNoAlias", t);
            fail();
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    @Test
    public final void testAccessURLWithAlias()
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
                log.info("testAccessURLWithAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.info("testAccessURLWithAlias, after: " + sql);

                assertTrue("testAccessURLWithAlias: !accessURL", !sql.contains("accessURL"));
                assertTrue("testAccessURLWithAlias: uri", sql.contains("a.uri"));
            }
        }
        catch (Throwable t)
        {
            log.error("testAccessURLWithAlias", t);
            fail();
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }
    
    // feature disabled because position_bounds_points is polymorphic
    //@Test
    public final void testPositonBoundsNoAlias()
    {
        String[] queries = new String[] 
        { 
            "select foo,position_bounds from caom2.Plane", 
            "select foo,position_bounds,bar from caom2.Plane" 
        };
        try
        {
            for (int t = 0; t < queries.length; t++)
            {
                TestUtil.job.getParameterList().clear();
                String query = queries[t];
                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.info("testPositonBoundsNoAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.info("testPositonBoundsNoAlias, after: " + sql);

                assertTrue("testPositonBoundsNoAlias: !position_bounds", !sql.contains("position_bounds "));
                assertTrue("testPositonBoundsNoAlias: position_bounds_points", sql.contains("position_bounds_points"));
            }
        }
        catch (Throwable t)
        {
            log.error("testPositonBoundsNoAlias", t);
            fail();
        }
    }

    // feature disabled because position_bounds_points is polymorphic
    //@Test
    public final void testPositonBoundsWithAlias()
    {
        String[] queries = new String[] 
        { 
            "select a.foo,a.position_bounds from caom2.Plane as a",
            "select a.foo,a.position_bounds,a.bar from caom2.Artifact as a" 
        };
        try
        {
            for (int t = 0; t < queries.length; t++)
            {
                TestUtil.job.getParameterList().clear();
                String query = queries[t];
                List<Parameter> params = new ArrayList<Parameter>();
                params.add(new Parameter("QUERY", query));
                log.info("testPositonBoundsWithAlias, before: " + query);
                TapQuery tq = new TestQuery();
                TestUtil.job.getParameterList().addAll(params);
                tq.setJob(TestUtil.job);
                String sql = tq.getSQL();
                log.info("testPositonBoundsWithAlias, after: " + sql);

                assertTrue("testPositonBoundsWithAlias: !position_bounds", !sql.contains("position_bounds "));
                assertTrue("testPositonBoundsWithAlias: position_bounds_points", sql.contains("position_bounds_points"));
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
            super.navigatorList.add(new CaomSelectListConverter());
        }
    }
}
