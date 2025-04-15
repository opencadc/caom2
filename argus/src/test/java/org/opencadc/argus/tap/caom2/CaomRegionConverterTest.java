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
import org.junit.Ignore;
import org.junit.Test;
import org.opencadc.argus.tap.CaomAdqlQuery;
import org.opencadc.argus.tap.query.CaomRegionConverter;

/**
 * test predicate function converter in CAOM context
 *
 * @author Sailor Zhang
 *
 */
public class CaomRegionConverterTest {

    private static Logger log = Logger.getLogger(CaomRegionConverterTest.class);

    public String adqlQuery;
    public String expectedSQL = "";
    public String actualSQL;

    private static TapSchema caomTapSchema = TestUtil.loadTapSchema();

    static {
        //Log4jInit.setLevel("ca.nrc.cadc.tap", Level.INFO);
        Log4jInit.setLevel("org.opencadc.argus", Level.INFO);
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
            Parameter para = new Parameter("QUERY", adqlQuery);
            List<Parameter> paramList = new ArrayList<>();
            paramList.add(para);

            TapQuery tapQuery = new TestQuery();
            TestUtil.job.getParameterList().addAll(paramList);
            tapQuery.setJob(TestUtil.job);
            actualSQL = tapQuery.getSQL();
            log.info("    input: " + adqlQuery);
            log.info("   result: " + actualSQL);
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
        Assert.assertTrue(actualSQL.toLowerCase().indexOf(expectedSQL.toLowerCase()) > 0);
    }

    @Test
    public void testIntersectNoAlias() {
        log.debug("testIntersectNoAlias START");
        expectedSQL = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.Plane where _q_position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectWithAlias() {
        log.debug("testIntersectWithAlias START");
        expectedSQL = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) && p._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane as p where INTERSECTS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }
    
    @Test
    public void testIntersectWithAliasAbuse() {
        log.debug("testIntersectWithAliasAbuse START");
        expectedSQL = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        // alias in from but not in where
        adqlQuery = "select planeid from caom2.Plane as p where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersect() {
        log.debug("testNotIntersect START");
        expectedSQL = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !&& _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsSpatial() {
        log.debug("testContains START");
        expectedSQL = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsSpatialWithAlias() {
        log.debug("testContains START");
        expectedSQL = "select planeid from caom2.Plane as p where scircle(spoint(radians(1), radians(2)), radians(3)) <@ p._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane as p where CONTAINS(CIRCLE('',1,2,3), p.position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotContainsSpatial() {
        log.debug("testNotContains START");
        expectedSQL = "select planeid from caom2.Plane where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        // only test one usage
    }

    @Test
    public void testIntersectPoint() {
        log.debug("testIntersectPoint START");
        expectedSQL = "select planeid from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);

        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.Plane where _q_position_bounds && cast(spoint(radians(1), radians(2)) as scircle)";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(position_bounds, POINT('',1,2)) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainPoint() {
        log.debug("testContainPoint START");
        expectedSQL = "select planeid from caom2.Plane where cast(spoint(radians(1), radians(2)) as scircle) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where CONTAINS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    @Ignore
    public void testSiaView() {
        log.debug("testSiaView START");

        // intersects
        expectedSQL = "select planeid from caom2.SIAv1 where _q_position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.SIAv1 where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.SIAv1 where cast(spoint(radians(1), radians(2)) as scircle) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.SIAv1 where INTERSECTS(POINT('',1,2), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        // contains
        expectedSQL = "select planeid from caom2.SIAv1 where scircle(spoint(radians(1), radians(2)), radians(3)) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.SIAv1 where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testObsTapRegion() {
        log.debug("testObsTapRegion START");

        // intersects
        expectedSQL = "select planeid from caom2.ObsCore where _q_position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from ivoa.ObsCore where INTERSECTS(s_region, CIRCLE('',1,2,3)) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from ivoa.ObsCore where INTERSECTS(POINT('',1,2), s_region) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        // contains
        expectedSQL = "select planeid from caom2.ObsCore where scircle(spoint(radians(1), radians(2)), radians(3)) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from ivoa.ObsCore where CONTAINS(CIRCLE('',1,2,3), s_region) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testFunction_Area() {
        log.debug("testArea START");

        expectedSQL = "select _q_position_bounds_area from someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select area(position_bounds) from someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select c._q_position_bounds_area from c.someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select area(c.position_bounds) from c.someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select c._q_position_bounds_area as \"area\" from c.someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select area(c.position_bounds) as \"area\" from c.someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testFunction_Centroid() {
        log.debug("testCentroid START");

        expectedSQL = "select _q_position_bounds_centroid from someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select centroid(position_bounds) from someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select c._q_position_bounds_centroid from c.someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select centroid(c.position_bounds) from c.someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select c._q_position_bounds_centroid as \"centroid\" from c.someTable";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select centroid(c.position_bounds) as \"centroid\" from c.someTable";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testFunction_Coordsys() {
        adqlQuery = "select COORDSYS(position_bounds) from caom2.plane";
        expectedSQL = "SELECT 'ICRS' FROM caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testFunction_Coord1() {
        // TODO:
        //_query = "select COORD1(targetPosition_coordinates) from caom2.observation";
        //_expected = "SELECT targetPosition_coordinates[1] FROM caom2.observation";
        //run();
        //log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        //Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
        
        adqlQuery = "select COORD1(CENTROID(position_bounds)) from caom2.plane";
        expectedSQL = "SELECT degrees(long(_q_position_bounds_centroid)) FROM caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testFunction_Coord2() {
        // TODO:
        //_query = "select COORD2(targetPosition_coordinates) from caom2.observation";
        //_expected = "SELECT degrees(lat(_q_targetPosition_coordinates)) FROM caom2.observation";
        //run();
        //log.debug(" expected: " + _expected);
        //Assert.assertEquals(_expected, prepareToCompare(_sql));
        //Assert.assertTrue(prepareToCompare(_sql).toLowerCase().indexOf(_expected.toLowerCase()) >= 0);
        
        adqlQuery = "select COORD2(CENTROID(position_bounds)) from caom2.plane";
        expectedSQL = "SELECT degrees(lat(_q_position_bounds_centroid)) FROM caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsWithSchemaRewrite() {
        log.debug("testContainsWithSchemaRewrite START");
        expectedSQL = "select planeid from caom2.ObsCore where cast(spoint(radians(1), radians(2)) as scircle) <@ caom2.ObsCore._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from ivoa.ObsCore where CONTAINS(POINT('',1,2), ivoa.ObsCore.position_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntervalContains() {
        log.debug("testIntervalContains START");
        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(0.9999999999999999, -0.1), point(1.0000000000000002, 0.1))) && _q_energy_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where CONTAINS(1.0, energy_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
        String tmp = prepareToCompare(actualSQL);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" && _q_energy_bounds"));

        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(0.9999999999999999, -0.1), point(1.0000000000000002, 0.1))) && _q_time_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where CONTAINS(1.0, time_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
        tmp = prepareToCompare(actualSQL);
        Assert.assertTrue(tmp.contains("polygon(box(point("));
        Assert.assertTrue(tmp.contains(" && _q_time_bounds"));
        
        // TODO: other interval columns
    }

    @Test
    public void testNotIntervalContains() {
        log.debug("testNotIntervalContains START");
        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(0.9999999999999999, -0.1), point(1.0000000000000002, 0.1))) !&& _q_energy_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where CONTAINS(1.0, energy_bounds) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(0.9999999999999999, -0.1), point(1.0000000000000002, 0.1))) !&& _q_time_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where CONTAINS(1.0, time_bounds) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectsEnergy() {
        log.debug("testContainsEnergy START");
        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) && _q_energy_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.Plane where _q_energy_bounds && polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 1";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsEnergy() {
        log.debug("testContainsEnergy START");
        expectedSQL = "select planeid from caom2.Plane where polygon(box(point(1.0, -0.1), point(2.0, 0.1))) !&& _q_energy_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(INTERVAL(1.0, 2.0), energy_bounds) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        expectedSQL = "select planeid from caom2.Plane where _q_energy_bounds !&& polygon(box(point(1.0, -0.1), point(2.0, 0.1)))";
        expectedSQL = prepareToCompare(expectedSQL);
        adqlQuery = "select planeid from caom2.Plane where INTERSECTS(energy_bounds, INTERVAL(1.0, 2.0)) = 0";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNone() {
        adqlQuery = "select obsid, planeid from caom2.plane where obsid is not null";
        expectedSQL = "select obsid, planeid from caom2.plane where obsid is not null";
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    
    @Test
    public void testIntersectsColumn() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(CENTROID(position_bounds), position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds_centroid::scircle && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsColumn() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(CENTROID(position_bounds), position_bounds) = 0";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds_centroid::scircle !&& _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectsValue() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds && scircle(spoint(radians(1), radians(2)), radians(3))";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsValue() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 0";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds !&& scircle(spoint(radians(1), radians(2)), radians(3))";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        adqlQuery = "select planeid from caom2.plane where NOT (INTERSECTS(position_bounds, CIRCLE('',1,2,3)) = 1)";
        expectedSQL = "select planeid from caom2.plane where not (_q_position_bounds && scircle(spoint(radians(1), radians(2)), radians(3)))";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectsRegion() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('CIRCLE 1.0 2.0 3.0')) = 1";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds && scircle(spoint(radians(1.0), radians(2.0)), radians(3.0))";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsRegion() {
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('CIRCLE 1.0 2.0 3.0')) = 0";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds !&& scircle(spoint(radians(1.0), radians(2.0)), radians(3.0))";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectsRegionPos() {
        // spoint has special handling in a predicate
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('POSITION 1.0 2.0')) = 1";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds && cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsRegionPos() {
        // spoint has special handling in a predicate
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(position_bounds, REGION('POSITION 1.0 2.0')) = 0";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds !&& cast(spoint(radians(1.0), radians(2.0)) as scircle)";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testIntersectsPoint() {
        // spoint has special handling in a predicate
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(POINT('',1.0,2.0), position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane where cast(spoint(radians(1.0), radians(2.0)) as scircle) && _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotIntersectsPoint() {
        // spoint has special handling in a predicate
        adqlQuery = "select planeid from caom2.plane where INTERSECTS(POINT('',1.0,2.0), position_bounds) = 0";
        expectedSQL = "select planeid from caom2.plane where cast(spoint(radians(1.0), radians(2.0)) as scircle) !&& _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsColumn() {
        adqlQuery = "select planeid from caom2.plane p, cat.Sources s where CONTAINS(s.srcpos, p.position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane as p, cat.Sources as s where s.srcpos <@ p._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotContainsColumn() {
        adqlQuery = "select planeid from caom2.plane p, cat.Sources s where CONTAINS(s.srcpos, p.position_bounds) = 0";
        expectedSQL = "select planeid from caom2.plane as p, cat.Sources as s where s.srcpos !<@ p._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsValue() {
        adqlQuery = "select planeid from caom2.plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane where scircle(spoint(radians(1), radians(2)), radians(3)) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotContainsValue() {
        adqlQuery = "select planeid from caom2.plane where CONTAINS(CIRCLE('',1,2,3), position_bounds) = 0";
        expectedSQL = "select planeid from caom2.plane where scircle(spoint(radians(1), radians(2)), radians(3)) !<@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        adqlQuery = "select planeid from caom2.plane where NOT (CONTAINS(CIRCLE('',1,2,3), position_bounds) = 1)";
        expectedSQL = "select planeid from caom2.plane where not (scircle(spoint(radians(1), radians(2)), radians(3)) <@ _q_position_bounds)";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testContainsPoint() {
        adqlQuery = "select planeid from caom2.plane where CONTAINS(POINT('',1,2), position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane where cast(spoint(radians(1), radians(2)) as scircle) <@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));

        adqlQuery = "select planeid from caom2.plane where CONTAINS(position_bounds, POINT('',1,2)) = 1";
        expectedSQL = "select planeid from caom2.plane where _q_position_bounds <@ cast(spoint(radians(1), radians(2)) as scircle)";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testNotContainsPoint() {
        // spoint has special handling in a predicate
        adqlQuery = "select planeid from caom2.plane where CONTAINS(POINT('',1,2), position_bounds) = 0";
        expectedSQL = "select planeid from caom2.plane where cast(spoint(radians(1), radians(2)) as scircle) !<@ _q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testSpatialJoinIntersects() {

        adqlQuery = "select planeid from caom2.plane as t1 join caom2.siav1 as t2 on INTERSECTS(t1.position_bounds, t2.position_bounds) = 1";
        expectedSQL = "select planeid from caom2.plane as t1 join caom2.siav1 as t2 on t1._q_position_bounds && t2._q_position_bounds";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testPoint() {
        adqlQuery = "select POINT('ICRS GEOCENTER', 1, 2) from caom2.plane";
        expectedSQL = "SELECT spoint(radians(1), radians(2)) FROM caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testCircle() {
        adqlQuery = "select CIRCLE('ICRS GEOCENTER', 10, 10, 1) from caom2.plane";
        expectedSQL = "SELECT scircle(spoint(radians(10), radians(10)), radians(1)) FROM caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testBox() {
        adqlQuery = "select BOX('ICRS GEOCENTER', 10, 20, 1, 2) from caom2.plane";
        expectedSQL = "select spoly "
            + "'{(9.471189659406665d,19.0d),(9.464427503181486d,21.0d),(10.535572496818514d,21.0d),(10.528810340593335d,19.0d)}'"
            + " from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testPolygon() {
        adqlQuery = "select POLYGON('ICRS GEOCENTER', 2,2,3,3,1,4) from caom2.plane";
        expectedSQL = "select spoly '{(2d,2d),(3d,3d),(1d,4d)}' from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testRegionBox() {
        adqlQuery = "select REGION('BOX ICRS GEOCENTER 11 22 10 20') from caom2.plane";
        expectedSQL = "select spoly "
            + "'{(5.888297025674854d,12.0d),(5.104107983189518d,32.0d),(16.895892016810482d,32.0d),(16.111702974325148d,12.0d)}'"
            + " from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testRegionPolygon() {
        adqlQuery = "select REGION('POLYGON ICRS GEOCENTER 1 2 3 4 5 6 7 8') from caom2.plane";
        expectedSQL = "select spoly '{(1.0d,2.0d),(3.0d,4.0d),(5.0d,6.0d),(7.0d,8.0d)}' from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testRegionCircle() {
        adqlQuery = "select REGION('CIRCLE ICRS GEOCENTER 11 22 0.5') from caom2.plane";
        expectedSQL = "select scircle(spoint(radians(11.0), radians(22.0)), radians(0.5)) from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }

    @Test
    public void testRegionPosition() {
        adqlQuery = "select REGION('POSITION GALACTIC 11 22') from caom2.plane";
        expectedSQL = "select spoint(radians(11.0), radians(22.0)) from caom2.plane";
        expectedSQL = prepareToCompare(expectedSQL);
        run();
        log.debug(" expected: " + expectedSQL);
        Assert.assertEquals(expectedSQL, prepareToCompare(actualSQL));
    }
}
