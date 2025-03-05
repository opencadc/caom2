/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package org.opencadc.argus.tap.caom2;

import org.opencadc.argus.tap.query.CaomRegionConverter;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.argus.tap.CaomAdqlQuery;

/**
 * test predicate function converter in CAOM context
 *
 * @author Sailor Zhang
 *
 */
public class CaomRegionConverterTest {

    private static Logger log = Logger.getLogger(CaomRegionConverterTest.class);

    public String _query;
    public String _expected = "";
    public String _sql;

    private static TapSchema caomTapSchema = TestUtil.loadTapSchema();

    static {
        //Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.tap.caom2", Level.INFO);
    }

    private class TestQuery extends CaomAdqlQuery {

        protected void init() {
            //super.init();
            super.navigatorList.add(new CaomRegionConverter(caomTapSchema));

            // after CaomRegionConverter: triggering off the same column names and converting some uses
            ExpressionNavigator en = new ExpressionNavigator();
            TableNameConverter tnc = new TableNameConverter(true);
            tnc.put("ivoa.ObsCore", "caom2.ObsCore");
            TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
            SelectNavigator sn = new SelectNavigator(en, tnrc, tnc);
            super.navigatorList.add(sn);
        }
    }

    private void run() {
        try {
            Parameter para = new Parameter("QUERY", _query);
            List<Parameter> paramList = new ArrayList<Parameter>();
            paramList.add(para);

            TapQuery tapQuery = new TestQuery();
            TestUtil.job.getParameterList().addAll(paramList);
            tapQuery.setJob(TestUtil.job);
            _sql = tapQuery.getSQL();
            log.info("    input: " + _query);
            log.info("   result: " + _sql);
        } finally {
            TestUtil.job.getParameterList().clear();
        }
    }

    private String prepareToCompare(String str) {
        String ret = str.trim();
        ret = ret.replaceAll("\\s+", " ");
        return ret.toLowerCase();
    }

    private void assertContain() {
        Assert.assertTrue(_sql.toLowerCase().indexOf(_expected.toLowerCase()) > 0);
    }

    @Test
    public void testIntersectNoAlias() {
        log.debug("testIntersectNoAlias START");
        //_expected = "select planeid from caom.Plane where planeid in"
        //        + " ( select planeid from "+CaomPredicate.SAMPLE_TABLE + " where sample && scircle(spoint(radians(1),radians(2)),radians(3)) )";
        _expected = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) && position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.Plane where position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testIntersectWithAlias() {
        log.debug("testIntersectWithAlias START");
        _expected = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) && p.position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane as p where INTERSECTS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }
    
    @Test
    public void testIntersectWithAliasAbuse() {
        log.debug("testIntersectWithAliasAbuse START");
        _expected = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) && position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        // alias in from but not in where
        _query = "select planeid from caom2.Plane as p where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testNotIntersect() {
        log.debug("testNotIntersect START");
        _expected = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !&& position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContains() {
        log.debug("testContains START");
        _expected = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContainsWithAlias() {
        log.debug("testContains START");
        _expected = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) <@ p.position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane as p where CONTAINS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testNotContains() {
        log.debug("testNotContains START");
        //_expected = "select planeid from caom.Plane where not exists"
        //        + " ( select planeid from "+CaomPredicate.SAMPLE_TABLE + " where scircle(spoint(radians(1),radians(2)),radians(3)) <@ sample )";
        _expected = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // only test one usage
    }

    @Test
    public void testIntersectPoint() {
        log.debug("testIntersectPoint START");
        _expected = "select planeid from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) && position_bounds_spoly";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.Plane where position_bounds_spoly && cast(spoint(radians(1), radians(2)) as scircle)";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(position_bounds, POINT('',1,2)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testContainPoint() {
        log.debug("testContainPoint START");
        _expected = "select planeid from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) <@ position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where CONTAINS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    // now that we use an spoly column directly in the main table, this does not seem to be necessary
    //@Test
    public void testOptimisedSymmetricIntersect() {
        // optimised: put the column on the left of the operator
        log.debug("testOptimisedSymmetricIntersect START");
        _expected = "select planeid from caom2.Plane where position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);

        _query = "select planeid from caom2.Plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // different arg order, same result
        _query = "select planeid from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testSiaView() {
        log.debug("testSiaView START");

        // intersects
        _expected = "select planeid from caom2.SIAv1 where position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.SIAv1 where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.SIAv1 where cast(spoint(radians(1), radians(2)) as scircle) && position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.SIAv1 where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // contains
        _expected = "select planeid from caom2.SIAv1 where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.SIAv1 where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testObsTapRegion() {
        log.debug("testObsTapRegion START");

        // intersects
        _expected = "select planeid from caom2.ObsCore where position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3))";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from ivoa.ObsCore where INTERSECTS(s_region, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) && position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from ivoa.ObsCore where INTERSECTS(POINT('',1,2), s_region) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        // contains
        _expected = "select planeid from caom2.ObsCore where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from ivoa.ObsCore where CONTAINS(CIRCLE('',1,2,3), s_region) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testArea() {
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
    public void testCentroid() {
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
    public void testContainsWithSchemaRewrite() {
        log.debug("testContainsWithSchemaRewrite START");
        _expected = "select planeid from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) <@ caom2.ObsCore.position_bounds_spoly";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from ivoa.ObsCore where CONTAINS(POINT('',1,2), ivoa.ObsCore.position_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testIntervalContains() {
        log.debug("testIntervalContains START");
        //_expected = "select planeid from caom2.Plane where point(1.0, 0.0) <@ energy_bounds";
        _expected = "select planeid from caom2.Plane where point(1.0, 0.0) <@ energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where CONTAINS(1.0, energy_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        String tmp = prepareToCompare(_sql);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" && energy_bounds"));

        //_expected = "select planeid from caom2.Plane where point(1.0, 0.0) <@ time_bounds";
        _expected = "select planeid from caom2.Plane where point(1.0, 0.0) <@ time_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where CONTAINS(1.0, time_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        tmp = prepareToCompare(_sql);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" && time_bounds"));
    }

    @Test
    public void testNotIntervalContains() {
        log.debug("testNotIntervalContains START");
        _expected = "select planeid from caom2.Plane where point(1.0, 0.0) !<@ energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where CONTAINS(1.0, energy_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        String tmp = prepareToCompare(_sql);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" !&& energy_bounds"));

        _expected = "select planeid from caom2.Plane where point(1.0, 0.0) !<@ time_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where CONTAINS(1.0, time_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        tmp = prepareToCompare(_sql);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" !&& time_bounds"));
    }

    @Test
    public void testIntersectsEnergy() {
        log.debug("testContainsEnergy START");
        _expected = "select planeid from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) && energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.Plane where energy_bounds && polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 1";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testNotIntersectsEnergy() {
        log.debug("testContainsEnergy START");
        _expected = "select planeid from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) !&& energy_bounds";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));

        _expected = "select planeid from caom2.Plane where energy_bounds !&& polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        _expected = prepareToCompare(_expected);
        _query = "select planeid from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 0";
        run();
        log.debug(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testNone() {
        _query = "select obsid, planeid from caom2.plane where obsid=planeid ";
        _expected = "select obsid, planeid from caom2.plane";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testFunctionCoordsys() {
        _query = "select COORDSYS(position_bounds) from caom2.plane";
        _expected = "SELECT 'ICRS' FROM caom2.plane";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testFunctionCentroid() {
        _query = "select CENTROID(position_bounds) from caom2.plane";
        _expected = "SELECT position_bounds_center FROM caom2.plane";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testFunctionCoord1() {
        _query = "select COORD1(CENTROID(position_bounds)) from caom2.plane";
        _expected = "SELECT degrees(long(position_bounds_center)) FROM caom2.plane";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testFunctionCoord2() {
        _query = "select COORD2(CENTROID(position_bounds)) from caom2.plane";
        _expected = "SELECT degrees(lat(position_bounds_center)) FROM caom2.plane";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testIntersectsColumn() // code casts position_bounds_center to scircle
    {
        _query = "select planeid from caom2.plane where INTERSECTS(CENTROID(position_bounds), position_bounds) = 1";
        _expected = "select planeid from caom2.plane where position_bounds_center::scircle && position_bounds";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotIntersectsColumn() {
        _query = "select planeid from caom2.plane where INTERSECTS(CENTROID(position_bounds), position_bounds) = 0";
        _expected = "select planeid from caom2.plane where position_bounds_center::scircle !&& position_bounds";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testIntersectsValue() {
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        _expected = "select planeid from caom2.plane where position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3))";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotIntersectsValue() {
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 0";
        _expected = "select planeid from caom2.plane where position_bounds_spoly !&& scircle(spoint(radians(1), radians(2)), radians(3))";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);

        _query = "select planeid from caom2.plane where NOT (INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1)";
        _expected = "select planeid from caom2.plane where not (position_bounds_spoly && scircle(spoint(radians(1), radians(2)), radians(3)))";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testIntersectsRegion() {
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('CIRCLE 1.0 2.0 3.0')) = 1";
        _expected = "select planeid from caom2.plane where position_bounds_spoly && scircle(spoint(radians(1.0), radians(2.0)), radians(3.0))";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotIntersectsRegion() {
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('CIRCLE 1.0 2.0 3.0')) = 0";
        _expected = "select planeid from caom2.plane where position_bounds_spoly !&& scircle(spoint(radians(1.0), radians(2.0)), radians(3.0))";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testIntersectsRegionPos() {
        // spoint has special handling in a predicate
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('POSITION 1.0 2.0')) = 1";
        _expected = "select planeid from caom2.plane where position_bounds_spoly && cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotIntersectsRegionPos() {
        // spoint has special handling in a predicate
        _query = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('POSITION 1.0 2.0')) = 0";
        _expected = "select planeid from caom2.plane where position_bounds_spoly !&& cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testIntersectsPoint() {
        // spoint has special handling in a predicate
        _query = "select planeid from caom2.plane where INTERSECTS(POINT('',1.0,2.0), position_bounds) = 1";
        _expected = "select planeid from caom2.plane where cast(spoint(radians(1.0), radians(2.0)) as scircle) && position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotIntersectsPoint() {
        // spoint has special handling in a predicate
        _query = "select planeid from caom2.plane where INTERSECTS(POINT('',1.0,2.0), position_bounds) = 0";
        _expected = "select planeid from caom2.plane where cast(spoint(radians(1.0), radians(2.0)) as scircle) !&& position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testContainsColumn() {
        _query = "select planeid from caom2.plane p, cat.Sources s where CONTAINS(s.srcpos, p.position_bounds) = 1";
        _expected = "select planeid from caom2.plane as p, cat.Sources as s where s.srcpos <@ p.position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotContainsColumn() {

        _query = "select planeid from caom2.plane p, cat.Sources s where CONTAINS(s.srcpos, p.position_bounds) = 0";
        _expected = "select planeid from caom2.plane as p, cat.Sources as s where s.srcpos !<@ p.position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testContainsValue() {

        _query = "select planeid from caom2.plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        _expected = "select planeid from caom2.plane where scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotContainsValue() {

        _query = "select planeid from caom2.plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 0";
        _expected = "select planeid from caom2.plane where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);

        _query = "select planeid from caom2.plane where NOT (CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1)";
        _expected = "select planeid from caom2.plane where not (scircle(spoint(radians(1), radians(2)), radians(3)) <@ position_bounds_spoly)";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testContainsPoint() {
        _query = "select planeid from caom2.plane where CONTAINS(POINT('',1,2), position_bounds) = 1";
        _expected = "select planeid from caom2.plane where cast(spoint(radians(1), radians(2)) as scircle) <@ position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);

        _query = "select planeid from caom2.plane where CONTAINS(position_bounds, POINT('',1,2)) = 1";
        _expected = "select planeid from caom2.plane where position_bounds_spoly <@ cast(spoint(radians(1), radians(2)) as scircle)";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testNotContainsPoint() {
        // spoint has special handling in a predicate
        _query = "select planeid from caom2.plane where CONTAINS(POINT('',1,2), position_bounds) = 0";
        _expected = "select planeid from caom2.plane where cast(spoint(radians(1), radians(2)) as scircle) !<@ position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testSpatialJoinIntersects() {

        _query = "select planeid from caom2.plane as t1 join caom2.siav1 as t2 on INTERSECTS(t1.position_bounds, t2.position_bounds) = 1";
        _expected = "select planeid from caom2.plane as t1 join caom2.siav1 as t2 on t1.position_bounds_spoly && t2.position_bounds_spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testPoint() {
        _query = "select POINT('ICRS GEOCENTER', 1, 2) from caom2.plane";
        _expected = "spoint";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testCircle() {
        _query = "select CIRCLE('ICRS GEOCENTER', 10, 10, 1) from caom2.plane";
        _expected = "scircle";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testBox() {
        _query = "select BOX('ICRS GEOCENTER', 10, 20, 1, 2) from caom2.plane";
        _expected = "spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testPolygon() {
        _query = "select POLYGON('ICRS GEOCENTER', 2,2,3,3,1,4) from caom2.plane";
        _expected = "spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testRegionBox() {
        _query = "select REGION('BOX ICRS GEOCENTER 11 22 10 20') from caom2.plane";
        _expected = "spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testRegionPolygon() {
        _query = "select REGION('POLYGON ICRS GEOCENTER 1 2 3 4 5 6 7 8') from caom2.plane";
        _expected = "spoly";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testRegionCircle() {
        _query = "select REGION('CIRCLE ICRS GEOCENTER 11 22 0.5') from caom2.plane";
        _expected = "scircle";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }

    @Test
    public void testRegionPosition() {
        _query = "select REGION('POSITION GALACTIC 11 22') from caom2.plane";
        _expected = "spoint";
        run();
        log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
    }
}
