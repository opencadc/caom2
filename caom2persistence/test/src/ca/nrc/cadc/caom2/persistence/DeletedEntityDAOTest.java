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
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.date.DateUtil;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public abstract class DeletedEntityDAOTest
{
    protected static Logger log;

    DeletedEntityDAO dao;
    Class[] entityClasses = { DeletedObservation.class,
        DeletedObservationMetaReadAccess.class,
        DeletedPlaneMetaReadAccess.class,
        DeletedPlaneDataReadAccess.class
    };

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    public DeletedEntityDAOTest(Class genClass, String server, String database, String schema)
        throws Exception
    {
        try
        {
            Map<String,Object> config = new TreeMap<String,Object>();
            config.put("server", server);
            config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            this.dao = new DeletedEntityDAO();
            dao.setConfig(config);
        }
        catch(Exception ex)
        {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    @Before
    public void setup()
        throws Exception
    {
        log.debug("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        for (Class c : entityClasses)
        {
            String s = gen.getTable(c);

            String sql = "delete from " + s;
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
        }
        log.debug("clearing old tables... OK");
    }

    @Test
    public void testGetListDeletedObservation()
    {
        try
        {

            Long id1 = new Long(100L);
            Long id2 = new Long(200L);
            Long id3 = new Long(300L);
            Long id4 = new Long(400L);
            Long id5 = new Long(500L);

            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 10L);
            Date d3 = new Date(d2.getTime() + 10L);
            Date d4 = new Date(d3.getTime() + 10L);
            Date d5 = new Date(d4.getTime() + 10L);

            Class c = entityClasses[0];
            log.debug("creating test content: " + c.getSimpleName());
            
            Constructor<DeletedEntity> ctor = c.getConstructor(Long.class, Date.class);
            DeletedEntity o1 = ctor.newInstance(id1, d1);
            DeletedEntity o2 = ctor.newInstance(id2, d2);
            DeletedEntity o3 = ctor.newInstance(id3, d3);
            DeletedEntity o4 = ctor.newInstance(id4, d4);
            DeletedEntity o5 = ctor.newInstance(id5, d5);

            // manually insert list since it is normally maintained by triggers
            DataSource ds = dao.getDataSource();
            String s = dao.getSQLGenerator().getTable(c);

            String sql = "insert into " + s + " (id,lastModified) values ( " + o1.id + ", '" + df.format(o1.lastModified) + "')";
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
            
            sql = "insert into " + s + " (id,lastModified) values ( " + o2.id + ", '" + df.format(o2.lastModified) + "')";
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
            
            sql = "insert into " + s + " (id,lastModified) values ( " + o3.id + ", '" + df.format(o3.lastModified) + "')";
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
            
            sql = "insert into " + s + " (id,lastModified) values ( " + o4.id + ", '" + df.format(o4.lastModified) + "')";
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
            
            sql = "insert into " + s + " (id,lastModified) values ( " + o5.id + ", '" + df.format(o5.lastModified) + "')";
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);

            Date start = new Date(o1.lastModified.getTime() - 100L); // past
            Integer batchSize = new Integer(3);
            List<DeletedEntity> dels;


            // get first batch
            dels = dao.getList(c, start, batchSize);
            Assert.assertNotNull(dels);
            Assert.assertEquals(3, dels.size());
            Assert.assertEquals(o1.id, dels.get(0).id);
            Assert.assertEquals(o2.id, dels.get(1).id);
            Assert.assertEquals(o3.id, dels.get(2).id);

            // get next batch
            dels = dao.getList(c, o3.lastModified, batchSize);
            Assert.assertNotNull(dels);
            Assert.assertEquals(3, dels.size()); // o3 gets picked up by the >=
            Assert.assertEquals(o3.id, dels.get(0).id);
            Assert.assertEquals(o4.id, dels.get(1).id);
            Assert.assertEquals(o5.id, dels.get(2).id);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
