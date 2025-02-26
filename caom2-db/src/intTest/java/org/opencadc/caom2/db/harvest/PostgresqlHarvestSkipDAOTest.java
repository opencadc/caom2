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

package org.opencadc.caom2.db.harvest;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.BucketSelector;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.text.DateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.caom2.db.TestUtil;
import org.opencadc.caom2.db.version.InitDatabase;

/**
 *
 * @author pdowler
 */
public class PostgresqlHarvestSkipDAOTest {

    private static final Logger log = Logger.getLogger(PostgresqlHarvestSkipDAOTest.class);

    static String schema = "caom2";

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.harvester", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2.harvester.state", Level.INFO);
    }

    DataSource dataSource;
    String database;

    public PostgresqlHarvestSkipDAOTest()
            throws Exception {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(TestUtil.TEST_SERVER, TestUtil.TEST_DB);
        this.dataSource = DBUtil.getDataSource(cc, true, true);

        InitDatabase init = new InitDatabase(dataSource, null, schema);
        init.doInit();

        String sql = "DELETE FROM " + schema + ".HarvestSkip";
        log.debug("cleanup: " + sql);
        dataSource.getConnection().createStatement().execute(sql);
    }

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInsertURI() {
        try {
            HarvestSkipDAO dao = new HarvestSkipDAO(dataSource, database, schema);
            URI id1 = URI.create("foo:" + UUID.randomUUID());
            URI id2 = URI.create("foo:" + UUID.randomUUID());
            URI id3 = URI.create("foo:" + UUID.randomUUID());

            HarvestSkip skip;
            Date start = null;
            Date end = null;

            Date t1 = new Date();
            skip = new HarvestSkip("testInsert", Integer.class.getName(), id1, t1, "m1");
            dao.put(skip);
            Thread.sleep(10L);
            Date t2 = new Date();
            skip = new HarvestSkip("testInsert", Integer.class.getName(), id2, t2, null);
            dao.put(skip);
            Thread.sleep(10L);
            Date t3 = new Date();
            skip = new HarvestSkip("testInsert", Integer.class.getName(), id3, t3, "m2");
            dao.put(skip);

            List<HarvestSkip> skips = dao.get("testInsert", Integer.class.getName(), start, end, null);
            Assert.assertEquals("skips size", 3, skips.size());

            Assert.assertEquals("testInsert", skips.get(0).getSource());
            Assert.assertEquals("testInsert", skips.get(1).getSource());
            Assert.assertEquals("testInsert", skips.get(2).getSource());

            Assert.assertEquals(Integer.class.getName(), skips.get(0).getName());
            Assert.assertEquals(Integer.class.getName(), skips.get(1).getName());
            Assert.assertEquals(Integer.class.getName(), skips.get(2).getName());
            
            Assert.assertEquals(id1, skips.get(0).getURI());
            Assert.assertEquals(id2, skips.get(1).getURI());
            Assert.assertEquals(id3, skips.get(2).getURI());

            Assert.assertEquals(t1, skips.get(0).tryAfter);
            Assert.assertEquals(t2, skips.get(1).tryAfter);
            Assert.assertEquals(t3, skips.get(2).tryAfter);

            Assert.assertEquals("m1", skips.get(0).errorMessage);
            Assert.assertNull(skips.get(1).errorMessage);
            Assert.assertEquals("m2", skips.get(2).errorMessage);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUpdateURI() {
        try {
            HarvestSkipDAO dao = new HarvestSkipDAO(dataSource, database, schema);
            URI id1 = URI.create("foo:" + UUID.randomUUID());

            HarvestSkip skip;

            Date t1 = new Date();
            skip = new HarvestSkip("testUpdate", Integer.class.getName(), id1, t1, "initial error message");
            dao.put(skip);
            final String expectedBucket = skip.getUriBucket();

            HarvestSkip actual1 = dao.get("testUpdate", Integer.class.getName(), id1);
            Assert.assertNotNull(actual1);
            Assert.assertEquals(id1, actual1.getURI());
            Assert.assertEquals(t1, actual1.tryAfter);
            Assert.assertEquals(expectedBucket, actual1.getUriBucket());
            Assert.assertEquals("error message", skip.errorMessage, actual1.errorMessage);
            Date d1 = actual1.lastModified;

            Thread.sleep(100L);

            Date t2 = new Date();
            skip.errorMessage = "modified error message";
            skip.tryAfter = t2;
            dao.put(skip);

            HarvestSkip actual2 = dao.get("testUpdate", Integer.class.getName(), id1);
            Assert.assertNotNull(actual2);
            Assert.assertEquals(id1, actual2.getURI());
            Assert.assertEquals(t2, actual2.tryAfter);
            Assert.assertEquals(expectedBucket, actual2.getUriBucket());
            Assert.assertEquals("error message", skip.errorMessage, actual2.errorMessage);

            log.debug("actual1.lastModified: " + actual1.lastModified.getTime());
            log.debug("actual2.lastModified: " + actual2.lastModified.getTime());
            Assert.assertTrue("lastModified increased", actual1.lastModified.getTime() < actual2.lastModified.getTime());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testIterator() {
        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        final String source = "testIterator";
        final String cname = "Foo";
        final String namespace = "foo:";
        try {
            HarvestSkipDAO dao = new HarvestSkipDAO(dataSource, database, schema);
            URI id1 = URI.create("foo:" + UUID.randomUUID());

            long t = System.currentTimeMillis();
            SortedSet<HarvestSkip> skips = new TreeSet<>(new TryAfterComparator());
            int num = 20;
            Date mid = null;
            Date end = null;
            for (int i = 0; i < num; i++) {
                URI skipID = URI.create(namespace + UUID.randomUUID().toString());
                Date tryAfter = new Date(t + i * 10L);
                HarvestSkip skip = new HarvestSkip(source, cname, skipID, tryAfter, null);
                log.info("created: " + skip.toString(df));
                skips.add(skip);
                dao.put(skip);
                
                end = tryAfter;
                if (i == 9) {
                    mid = tryAfter;
                }
            }
            
            log.info("iterator -> " + df.format(mid));
            ResourceIterator<HarvestSkip> i1 = dao.iterator(cname, namespace, null, mid);
            Iterator<HarvestSkip> si1 = skips.iterator();
            int foundMid = 0;
            while (i1.hasNext()) {
                HarvestSkip expected = si1.next();
                HarvestSkip actual = i1.next();
                log.info("found: " + expected.toString(df));
                Assert.assertEquals(expected.getID(), actual.getID());
                foundMid++;
            }
            Assert.assertEquals((num / 2), foundMid);
            
            
            log.info("iterator -> " + df.format(end));
            ResourceIterator<HarvestSkip> i2 = dao.iterator(cname, namespace, null, end);
            Iterator<HarvestSkip> si2 = skips.iterator();
            int foundEnd = 0;
            while (i2.hasNext()) {
                HarvestSkip expected = si2.next();
                HarvestSkip actual = i2.next();
                log.info("found: " + expected.toString(df));
                Assert.assertEquals(expected.getID(), actual.getID());
                foundEnd++;
            }
            Assert.assertEquals(num, foundEnd);
            
            log.info("iterator close");
            ResourceIterator<HarvestSkip> i3 = dao.iterator(cname, namespace, null, end);
            i3.next();
            Assert.assertTrue(i3.hasNext());
            i3.close();
            Assert.assertFalse(i3.hasNext());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIteratorBucket() {
        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        final String source = "testIteratorBucket";
        final String cname = "Foo";
        final String namespace = "foo:";
        try {
            HarvestSkipDAO dao = new HarvestSkipDAO(dataSource, database, schema);
            URI id1 = URI.create(namespace + UUID.randomUUID());

            long t = System.currentTimeMillis();
            SortedSet<HarvestSkip> skips = new TreeSet<>(new TryAfterComparator());
            SortedSet<String> buckets = new TreeSet<>();
            int num = 20;
            for (int i = 0; i < num; i++) {
                URI skipID = URI.create("foo:" + UUID.randomUUID().toString());
                Date tryAfter = new Date(t + i * 10L);
                HarvestSkip skip = new HarvestSkip(source, cname, skipID, tryAfter, null);
                skips.add(skip);
                dao.put(skip);
                String b = skip.getUriBucket().substring(0, 1);
                buckets.add(b);
                log.info("created: " + skip.getUriBucket() + " " + skip.toString(df));
            }
            
            for (String b : buckets) {
                log.info("iterator: " + b);
                ResourceIterator<HarvestSkip> i = dao.iterator(cname, namespace, b, null);
                Assert.assertTrue("found something in " + b, i.hasNext());
                while (i.hasNext()) {
                    HarvestSkip actual = i.next();
                    log.info("found: " + actual.getUriBucket() + " " + actual.toString(df));
                    Assert.assertTrue(actual.getUriBucket().startsWith(b));
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testIteratorBucketRange() {
        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        final String source = "testIteratorBucket";
        final String cname = "Foo";
        final String namespace = "foo:";
        try {
            HarvestSkipDAO dao = new HarvestSkipDAO(dataSource, database, schema);
            URI id1 = URI.create(namespace + UUID.randomUUID());

            long t = System.currentTimeMillis();
            SortedSet<HarvestSkip> skips = new TreeSet<>(new TryAfterComparator());
            SortedSet<String> buckets = new TreeSet<>();
            int num = 20;
            for (int i = 0; i < num; i++) {
                URI skipID = URI.create("foo:" + UUID.randomUUID().toString());
                Date tryAfter = new Date(t + i * 10L);
                HarvestSkip skip = new HarvestSkip(source, cname, skipID, tryAfter, null);
                skips.add(skip);
                dao.put(skip);
                String b = skip.getUriBucket().substring(0, 1);
                buckets.add(b);
                log.info("created: " + skip.getUriBucket() + " " + skip.toString(df));
            }
            
            BucketSelector bs = new BucketSelector("0-f");
            String minB = bs.getMinBucket(HarvestSkipDAO.BUCKET_LENGTH);
            String maxB = bs.getMaxBucket(HarvestSkipDAO.BUCKET_LENGTH);
            log.info("iterator: " + minB + "-" + maxB);
            ResourceIterator<HarvestSkip> i = dao.iterator(cname, namespace, minB, maxB, null);
            Assert.assertTrue("found something", i.hasNext());
            int found = 0;
            while (i.hasNext()) {
                HarvestSkip actual = i.next();
                log.info("found: " + actual.getUriBucket() + " " + actual.toString(df));
                found++;
            }
            Assert.assertEquals("found", num, found);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private static class TryAfterComparator implements Comparator<HarvestSkip> {

        @Override
        public int compare(HarvestSkip lhs, HarvestSkip rhs) {
            return lhs.tryAfter.compareTo(rhs.tryAfter);
        }
    }
}
