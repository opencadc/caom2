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

package org.opencadc.caom2.db;

import ca.nrc.cadc.caom2.DeletedArtifactDescriptionEvent;
import ca.nrc.cadc.caom2.DeletedObservationEvent;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public abstract class AbstractDeletedEntityDAOTest {

    protected static Logger log;

    DeletedObservationEventDAO doeDAO;
    DeletedArtifactDescriptionEventDAO dadeDAO;

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    public AbstractDeletedEntityDAOTest(Class genClass, String server, String database, String schema)
            throws Exception {
        try {
            DBConfig dbrc = new DBConfig();
            ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
            DBUtil.createJNDIDataSource("jdbc/caom2-db-test", cc);
            
            Map<String, Object> config = new TreeMap<String, Object>();
            config.put("jndiDataSourceName", "jdbc/caom2-db-test");
            //config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            this.doeDAO = new DeletedObservationEventDAO(true);
            doeDAO.setConfig(config);
            this.dadeDAO = new DeletedArtifactDescriptionEventDAO(true);
            dadeDAO.setConfig(config);
        } catch (Exception ex) {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    static Class[] ENTITY_CLASSES = {
        DeletedObservationEvent.class, DeletedArtifactDescriptionEvent.class
    };

    @Before
    public void setup()
            throws Exception {
        log.info("clearing old tables...");
        SQLGenerator gen = doeDAO.getSQLGenerator();
        DataSource ds = doeDAO.getDataSource();
        for (Class c : ENTITY_CLASSES) {
            String cn = c.getSimpleName();
            String s = gen.getTable(c);

            String sql = "delete from " + s;
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
        }
        log.info("clearing old tables... OK");
    }

    @Test
    public void testPutGetDeleteDOE() {
        try {
            UUID id1 = UUID.randomUUID();

            Date d1 = new Date();

            URI u1 = new URI("caom:FOO/bar1");

            DeletedObservationEvent o1 = new DeletedObservationEvent(id1, u1);

            log.info("put: " + o1);
            doeDAO.put(o1);
            Assert.assertNotNull("side effect: DeletedObservation.lastModified", o1.getLastModified());

            DeletedObservationEvent per = doeDAO.get(id1);
            Assert.assertNotNull("found DeletedObservation " + id1, per);
            Assert.assertNotNull("DeletedObservation.lastModified", per.getLastModified());

            doeDAO.delete(per.getID());

            DeletedObservationEvent gone = doeDAO.get(id1);
            Assert.assertNull(gone);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutGetDeleteDADE() {
        try {
            UUID id1 = UUID.randomUUID();

            Date d1 = new Date();

            URI u1 = new URI("cadc:FOO/bar1");

            DeletedArtifactDescriptionEvent o1 = new DeletedArtifactDescriptionEvent(id1, u1);

            log.info("put: " + o1);
            dadeDAO.put(o1);
            Assert.assertNotNull("side effect: DeletedObservation.lastModified", o1.getLastModified());

            DeletedArtifactDescriptionEvent per = dadeDAO.get(id1);
            Assert.assertNotNull("found DeletedArtifactDescriptionEvent " + id1, per);
            Assert.assertNotNull("DeletedArtifactDescriptionEvent.lastModified", per.getLastModified());

            dadeDAO.delete(per.getID());

            DeletedArtifactDescriptionEvent gone = dadeDAO.get(id1);
            Assert.assertNull(gone);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    /*
    @Test
    public void testGetListDeletedObservation() {
        try {
            UUID id1 = new UUID(0L, 100L);
            UUID id2 = new UUID(0L, 200L);
            UUID id3 = new UUID(0L, 300L);
            UUID id4 = new UUID(0L, 400L);
            UUID id5 = new UUID(0L, 500L);

            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 10L);
            Date d3 = new Date(d2.getTime() + 10L);
            Date d4 = new Date(d3.getTime() + 10L);
            Date d5 = new Date(d4.getTime() + 10L);

            URI u1 = new URI("caom:FOO/bar1");
            URI u2 = new URI("caom:FOO/bar2");
            URI u3 = new URI("caom:FOO/bar3");
            URI u4 = new URI("caom:FOO/bar4");
            URI u5 = new URI("caom:FOO/bar5");

            DeletedObservation o1 = new DeletedObservation(id1, u1);
            DeletedObservation o2 = new DeletedObservation(id2, u2);
            DeletedObservation o3 = new DeletedObservation(id3, u3);
            DeletedObservation o4 = new DeletedObservation(id4, u4);
            DeletedObservation o5 = new DeletedObservation(id5, u5);

            log.info("put: \n"
                + o1 + "\n"
                + o2 + "\n"
                + o3 + "\n"
                + o4 + "\n"
                + o5 + "\n"
            );
            
            dao.put(o1);
            Thread.sleep(10L);
            dao.put(o2);
            Thread.sleep(10L);
            dao.put(o3);
            Thread.sleep(10L);
            dao.put(o4);
            Thread.sleep(10L);
            dao.put(o5);

            Assert.assertNotNull("DeletedObservation.lastModified", o1.lastModified);
            Date start = new Date(o1.lastModified.getTime() - 100L); // past
            Date end = null;
            Integer batchSize = new Integer(3);
            List<DeletedObservation> dels;
            
            // get first batch
            dels = dao.getList(DeletedObservation.class, start, end, batchSize);
            Assert.assertNotNull("deleted list", dels);
            Assert.assertEquals(3, dels.size());
            Assert.assertEquals(o1.getID(), dels.get(0).getID());
            Assert.assertEquals(o2.getID(), dels.get(1).getID());
            Assert.assertEquals(o3.getID(), dels.get(2).getID());

            // get next batch
            dels = dao.getList(DeletedObservation.class, o3.lastModified, end, batchSize);
            Assert.assertNotNull("deleted list/next batch", dels);
            Assert.assertEquals(3, dels.size()); // o3 gets picked up by the >=
            Assert.assertEquals(o3.getID(), dels.get(0).getID());
            Assert.assertEquals(o4.getID(), dels.get(1).getID());
            Assert.assertEquals(o5.getID(), dels.get(2).getID());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
     */
}
