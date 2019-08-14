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

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public abstract class AbstractDeletedEntityDAOTest {

    protected static Logger log;

    DeletedEntityDAO dao;

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    public AbstractDeletedEntityDAOTest(Class genClass, String server, String database, String schema)
            throws Exception {
        try {
            Map<String, Object> config = new TreeMap<String, Object>();
            config.put("server", server);
            config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            config.put("basePublisherID", "ivo://opencadc.org/");
            this.dao = new DeletedEntityDAO();
            dao.setConfig(config);
        } catch (Exception ex) {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    //@Before
    public void setup()
            throws Exception {
        log.debug("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        // TODO?
        log.debug("clearing old tables... OK");
    }

    @Test
    public void testPutGetDelete() {
        try {
            UUID id1 = new UUID(0L, 100L);

            Date d1 = new Date();

            ObservationURI u1 = new ObservationURI("FOO", "bar1");

            DeletedObservation o1 = new DeletedObservation(id1, u1);

            log.info("put: " + o1);
            dao.put(o1);
            
            DeletedEntity per = dao.get(DeletedObservation.class, id1);
            Assert.assertNotNull(per);
            
            dao.delete(per);
            
            DeletedEntity gone = dao.get(DeletedObservation.class, id1);
            Assert.assertNull(gone);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
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

            ObservationURI u1 = new ObservationURI("FOO", "bar1");
            ObservationURI u2 = new ObservationURI("FOO", "bar2");
            ObservationURI u3 = new ObservationURI("FOO", "bar3");
            ObservationURI u4 = new ObservationURI("FOO", "bar4");
            ObservationURI u5 = new ObservationURI("FOO", "bar5");

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

            Date start = new Date(o1.getLastModified().getTime() - 100L); // past
            Date end = null;
            Integer batchSize = new Integer(3);
            List<DeletedEntity> dels;
            
            // get first batch
            dels = dao.getList(DeletedObservation.class, start, end, batchSize);
            Assert.assertNotNull(dels);
            Assert.assertEquals(3, dels.size());
            Assert.assertEquals(o1.getID(), dels.get(0).getID());
            Assert.assertEquals(o2.getID(), dels.get(1).getID());
            Assert.assertEquals(o3.getID(), dels.get(2).getID());

            // get next batch
            dels = dao.getList(DeletedObservation.class, o3.getLastModified(), end, batchSize);
            Assert.assertNotNull(dels);
            Assert.assertEquals(3, dels.size()); // o3 gets picked up by the >=
            Assert.assertEquals(o3.getID(), dels.get(0).getID());
            Assert.assertEquals(o4.getID(), dels.get(1).getID());
            Assert.assertEquals(o5.getID(), dels.get(2).getID());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
