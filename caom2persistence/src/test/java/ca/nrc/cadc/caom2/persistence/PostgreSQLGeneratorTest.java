/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.MultiPolygon;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.Vertex;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class PostgreSQLGeneratorTest {

    private static final Logger log = Logger.getLogger(PostgreSQLGeneratorTest.class);

    static String schema;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);

        String testSchema = UtilTest.getTestSchema();
        if (testSchema != null) {
            schema = testSchema;
        }
    }

    PostgreSQLGenerator gen = new PostgreSQLGenerator("cadctest", schema);

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    /*
    @Test
    public void testSelectObservationSQL()
    {
        try
        {
            String start = "select observation.maxlastmodified ";
            String end = "order by observation.maxlastmodified limit 10";
            String sql = gen.getSelectLastModifiedRangeSQL(Observation.class, new Date(), null, new Integer(10));
            log.debug("SQL: " + sql);
            sql = sql.toLowerCase();
            String actualStart = sql.substring(0, start.length());
            String actualEnd = sql.substring(sql.length() - end.length());
            Assert.assertEquals(start, actualStart);
            Assert.assertEquals(end, actualEnd);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
     */
    @Test
    public void testSelectByLastModifiedSQL() {
        try {
            String start = "select observationstate.";
            String end = " order by observationstate.maxlastmodified limit 10";
            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 1000000L);
            Integer batchSize = new Integer(10);
            DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
            String exp1 = "observationstate.maxlastmodified >= '" + df.format(d1) + "'";

            String sql = gen.getSelectSQL(ObservationState.class, d1, null, batchSize);
            log.debug("SQL: " + sql);
            sql = sql.toLowerCase();

            String actualStart = sql.substring(0, start.length());
            String actualEnd = sql.substring(sql.length() - end.length());
            Assert.assertEquals(start, actualStart);
            Assert.assertEquals(end, actualEnd);

            log.debug("look for: " + exp1);
            Assert.assertTrue(sql.contains(exp1));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCircleToPolygonApproximatiom() {
        try {
            Circle c = new Circle(new Point(12.0, 34.0), 1.0);
            double ca = c.getArea();
            double cs = c.getSize();

            for (int i = 4; i < 32; i += 2) {
                ca.nrc.cadc.dali.Polygon dpoly = gen.generatePolygonApproximation(c, i);
                List<Vertex> verts = new ArrayList<Vertex>();
                List<Point> points = new ArrayList<Point>();
                SegmentType t = SegmentType.MOVE;
                for (ca.nrc.cadc.dali.Point dp : dpoly.getVertices()) {
                    points.add(new Point(dp.getLongitude(), dp.getLatitude()));
                    verts.add(new Vertex(dp.getLongitude(), dp.getLatitude(), t));
                    t = SegmentType.LINE;
                }
                verts.add(Vertex.CLOSE);
                MultiPolygon mp = new MultiPolygon(verts);
                Polygon poly = new Polygon(points, mp);

                double pa = poly.getArea();
                double ps = poly.getSize();
                double da = pa / ca;
                log.info("n=" + i + " poly: " + ps + " " + pa + " (" + da + ")");
            }
            log.info("circle: " + ca + " " + cs);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
