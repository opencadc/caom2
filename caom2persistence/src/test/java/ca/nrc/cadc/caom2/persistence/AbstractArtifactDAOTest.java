/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.persistence;


import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public abstract class AbstractArtifactDAOTest 
{
    private static final Logger log = Logger.getLogger(AbstractArtifactDAOTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.DEBUG);
    }
    
    ObservationDAO obsDAO; // to creater test content
    ArtifactDAO dao;
    TransactionManager txnManager;
    
    Class[] ENTITY_CLASSES =
    {
        Artifact.class, Plane.class, Observation.class
    };
    
    public AbstractArtifactDAOTest(Class genClass, String server, String database, String schema)
    {
        try
        {
            Map<String,Object> config = new TreeMap<String,Object>();
            config.put("server", server);
            config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            this.obsDAO = new ObservationDAO();
            obsDAO.setConfig(config);
            this.dao = new ArtifactDAO();
            dao.setConfig(config);
            this.txnManager = dao.getTransactionManager();
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
        log.info("clearing old tables...");
        SQLGenerator gen = dao.getSQLGenerator();
        DataSource ds = dao.getDataSource();
        for (Class c : ENTITY_CLASSES)
        {
            String cn = c.getSimpleName();
            String s = gen.getTable(c);
            
            String sql = "delete from " + s;
            log.debug("setup: " + sql);
            ds.getConnection().createStatement().execute(sql);
        }
        log.info("clearing old tables... OK");
    }

    //@Test
    public void testTemplate()
    {
        try
        {
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetList()
    {
        try
        {
            Artifact a1 = new Artifact(URI.create("cadc:STUFF/thing1"), ProductType.SCIENCE, ReleaseType.DATA);
            Artifact a2 = new Artifact(URI.create("cadc:STUFF/thing2"), ProductType.SCIENCE, ReleaseType.DATA);
            Artifact a3 = new Artifact(URI.create("cadc:STUFF/thing3"), ProductType.SCIENCE, ReleaseType.DATA);
            Plane p = new Plane("baz");
            Observation o = new SimpleObservation("FOO", "bar");
            o.getPlanes().add(p);
            
            Date t1 = new Date();
            Thread.sleep(10);
            p.getArtifacts().add(a1);
            obsDAO.put(o);
            
            Thread.sleep(10);
            Date t2 = new Date();
            Thread.sleep(10);
            p.getArtifacts().add(a2);
            obsDAO.put(o);
            
            Thread.sleep(10);
            p.getArtifacts().add(a3);
            obsDAO.put(o);
            
            List<Artifact> artifacts = dao.getList(Artifact.class, null, null, null);
            Assert.assertNotNull(artifacts);
            Assert.assertEquals(3, artifacts.size());
            
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            log.info("getList: t1 " + df.format(t1) + " -> null [null]");
            artifacts = dao.getList(Artifact.class, t1, null, null);
            Assert.assertNotNull(artifacts);
            Assert.assertEquals(3, artifacts.size());
            Assert.assertEquals(a1.getURI(), artifacts.get(0).getURI());
            Assert.assertEquals(a2.getURI(), artifacts.get(1).getURI());
            Assert.assertEquals(a3.getURI(), artifacts.get(2).getURI());
            
            log.info("getList: t1 " + df.format(t1) + " -> null [2]");
            artifacts = dao.getList(Artifact.class, t1, null, 2);
            Assert.assertNotNull(artifacts);
            Assert.assertEquals(2, artifacts.size());
            Assert.assertEquals(a1.getURI(), artifacts.get(0).getURI());
            Assert.assertEquals(a2.getURI(), artifacts.get(1).getURI());
            
            log.info("getList: t2 " + df.format(t2) + " -> null [null]");
            artifacts = dao.getList(Artifact.class, t2, null, null);
            Assert.assertNotNull(artifacts);
            Assert.assertEquals(2, artifacts.size());
            Assert.assertEquals(a2.getURI(), artifacts.get(0).getURI());
            Assert.assertEquals(a3.getURI(), artifacts.get(1).getURI());
            
            log.info("getList: t1,t2 " + df.format(t1) + " -> " + df.format(t2) + " [null]");
            artifacts = dao.getList(Artifact.class, t1, t2, null);
            Assert.assertNotNull(artifacts);
            Assert.assertEquals(1, artifacts.size());
            Assert.assertEquals(a1.getURI(), artifacts.get(0).getURI());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
