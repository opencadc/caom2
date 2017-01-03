/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

/**
 * 
 */
package ca.nrc.cadc.tap.caom2;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * test predicate function converter in CAOM context
 * 
 * @author Sailor Zhang
 *
 */
public class CaomRegionConverterTest
{
    private static Logger log = Logger.getLogger(CaomRegionConverterTest.class);
    
    public String _query;
    public String _expected="";
    public String _sql;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc.tap", org.apache.log4j.Level.INFO);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    private class TestQuery extends AdqlQuery
    {
        protected void init()
        {
            //super.init();
            super.navigatorList.add(new CaomRegionConverter());

            // after CaomRegionConverter: triggering off the same column names and converting some uses
            ExpressionNavigator en = new ExpressionNavigator();
            TableNameConverter tnc = new TableNameConverter(true);
            tnc.put("ivoa.ObsCore", "caom2.ObsCore");
            TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
            SelectNavigator sn = new SelectNavigator(en, tnrc, tnc);
            super.navigatorList.add(sn);
        }
    }

    private void run()
    {
        try
        {
            Parameter para = new Parameter("QUERY", _query);
            List<Parameter> paramList = new ArrayList<Parameter>();
            paramList.add(para);

            TapQuery tapQuery = new TestQuery();
            TestUtil.job.getParameterList().addAll(paramList);
            tapQuery.setJob(TestUtil.job);
            _sql = tapQuery.getSQL();
            log.info("    input: " + _query);
            log.info("   result: " + _sql);
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    private String prepareToCompare(String str)
    {
        String ret = str.trim();
        ret = ret.replaceAll("\\s+", " ");
        return ret.toLowerCase();
    }

    private void assertContain()
    {
        Assert.assertTrue(_sql.toLowerCase().indexOf(_expected.toLowerCase()) > 0);
    }

    @Test
    public void testIntersectNoAlias()
    {
        log.debug("testIntersectNoAlias START");
        //_expected = "select * from caom.Plane where planeid in"
        //        + " ( select planeid from "+CaomPredicate.SAMPLE_TABLE + " where sample && scircle(spoint(radians(1),radians(2)),radians(3)) )";
        _expected = "select * from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) && position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select * from caom2.Plane where position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testIntersectWithAlias()
    {
        log.debug("testIntersectWithAlias START");
        _expected = "select * from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) && p.position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane as p where INTERSECTS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testNotIntersect()
    {
        log.debug("testNotIntersect START");
        _expected = "select * from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !&& position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContains()
    {
        log.debug("testContains START");
        _expected = "select * from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testContainsWithAlias()
    {
        log.debug("testContains START");
        _expected = "select * from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) <@ p.position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane as p where CONTAINS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }


    @Test
    public void testNotContains()
    {
        log.debug("testNotContains START");
        //_expected = "select * from caom.Plane where not exists"
        //        + " ( select planeid from "+CaomPredicate.SAMPLE_TABLE + " where scircle(spoint(radians(1),radians(2)),radians(3)) <@ sample )";
        _expected = "select * from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // only test one usage
    }

    @Test
    public void testIntersectPoint()
    {
        log.debug("testIntersectPoint START");
        _expected = "select * from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) && position_bounds";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select * from caom2.Plane where position_bounds && cast(spoint(radians(1), radians(2)) as scircle)";
        _expected = prepareToCompare(_expected);
         _query = "select * from caom2.Plane where INTERSECTS(position_bounds, POINT('',1,2)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContainPoint()
    {
        log.debug("testContainPoint START");
        _expected = "select * from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) <@ position_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where CONTAINS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    // now that we use an spoly column directly in the main table, this does not seem to be necessary
    //@Test
    public void testOptimisedSymmetricIntersect()
    {
        // optimised: put the column on the left of the operator
        log.debug("testOptimisedSymmetricIntersect START");
        _expected = "select * from caom2.Plane where position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);

        _query = "select * from caom2.Plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // different arg order, same result
        _query = "select * from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testSiaView()
    {
        log.debug("testSiaView START");

        // intersects
        _expected = "select * from caom2.SIAv1 where position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.SIAv1 where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select * from caom2.SIAv1 where cast(spoint(radians(1), radians(2)) as scircle) && position_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.SIAv1 where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // contains
        _expected = "select * from caom2.SIAv1 where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.SIAv1 where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testObsTapRegion()
    {
        log.debug("testObsTapRegion START");
        
        // intersects
        _expected = "select * from caom2.ObsCore where s_region && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select * from ivoa.ObsCore where INTERSECTS(s_region, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select * from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) && s_region";
        _expected = prepareToCompare(_expected);
         _query = "select * from ivoa.ObsCore where INTERSECTS(POINT('',1,2), s_region) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // contains
        _expected = "select * from caom2.ObsCore where scircle(spoint(radians(1), radians(2)), radians(3)) <@ s_region";
        _expected = prepareToCompare(_expected);
        _query = "select * from ivoa.ObsCore where CONTAINS(CIRCLE('',1,2,3), s_region) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testArea()
    {
        log.debug("testArea START");

        _expected = "select position_bounds_area from someTable";
        _expected = prepareToCompare(_expected);
        _query = "select area(position_bounds) from someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select c.position_bounds_area from c.someTable";
        _expected = prepareToCompare(_expected);
        _query = "select area(c.position_bounds) from c.someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select c.position_bounds_area as \"area\" from c.someTable";
        _expected = prepareToCompare(_expected);
        _query = "select area(c.position_bounds) as \"area\" from c.someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testCentroid()
    {
        log.debug("testCentroid START");

        _expected = "select position_bounds_center from someTable";
        _expected = prepareToCompare(_expected);
        _query = "select centroid(position_bounds) from someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select c.position_bounds_center from c.someTable";
        _expected = prepareToCompare(_expected);
        _query = "select centroid(c.position_bounds) from c.someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select c.position_bounds_center as \"centroid\" from c.someTable";
        _expected = prepareToCompare(_expected);
        _query = "select centroid(c.position_bounds) as \"centroid\" from c.someTable";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContainsWithSchemaRewrite()
    {
        log.debug("testContainsWithSchemaRewrite START");
        _expected = "select * from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) <@ caom2.ObsCore.position_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from ivoa.ObsCore where CONTAINS(POINT('',1,2), ivoa.ObsCore.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testIntervalContains()
    {
        log.debug("testIntervalContains START");
        _expected = "select * from caom2.Plane where point(1.0, 0.0) <@ energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where CONTAINS(1.0, energy_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
        
        _expected = "select * from caom2.Plane where point(1.0, 0.0) <@ time_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where CONTAINS(1.0, time_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testNotIntervalContains()
    {
        log.debug("testNotIntervalContains START");
        _expected = "select * from caom2.Plane where point(1.0, 0.0) !<@ energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where CONTAINS(1.0, energy_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
        
        _expected = "select * from caom2.Plane where point(1.0, 0.0) !<@ time_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where CONTAINS(1.0, time_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testIntersectsEnergy()
    {
        log.debug("testContainsEnergy START");
        _expected = "select * from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) && energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
        
        _expected = "select * from caom2.Plane where energy_bounds && polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testNotIntersectsEnergy()
    {
        log.debug("testContainsEnergy START");
        _expected = "select * from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) !&& energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
        
        _expected = "select * from caom2.Plane where energy_bounds !&& polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        _expected = prepareToCompare(_expected);
        _query = "select * from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

}
