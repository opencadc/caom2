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

package org.opencadc.caom2.db;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Date;
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
public abstract class AbstractArtifactDAOTest {

    private static final Logger log = Logger.getLogger(AbstractArtifactDAOTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.caom2.db", Level.INFO);
    }

    ObservationDAO obsDAO; // to creater test content
    ArtifactDAO dao;
    TransactionManager txnManager;

    static Class[] ENTITY_CLASSES = {
        // including join tables before FK targets
        ObservationMember.class, ProvenanceInput.class, Chunk.class, Part.class, Artifact.class, Plane.class, Observation.class
    };

    public AbstractArtifactDAOTest(Class genClass, String server, String database, String schema)
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
            this.obsDAO = new ObservationDAO(true);
            obsDAO.setConfig(config);
            this.dao = new ArtifactDAO(true);
            dao.setConfig(config);
            this.txnManager = dao.getTransactionManager();
        } catch (Exception ex) {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    @Before
    public void setup()
            throws Exception {
        try {
            log.info("clearing old tables...");
            SQLGenerator gen = dao.getSQLGenerator();
            DataSource ds = dao.getDataSource();
            for (Class c : ENTITY_CLASSES) {
                String cn = c.getSimpleName();
                String s = gen.getTable(c);

                String sql = "delete from " + s;
                log.debug("setup: " + sql);
                ds.getConnection().createStatement().execute(sql);
            }
            log.info("clearing old tables... OK");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }

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
    public void testIterator() {
        try {
            final Observation o = new SimpleObservation("FOO", URI.create("caom:FOO/bar"), SimpleObservation.EXPOSURE);
            final Plane p = new Plane(URI.create("caom:FOO/bar/baz"));
            p.publisherID = p.getURI();
            final Artifact a1 = new Artifact(URI.create("cadc:STUFF/thing1"), DataLinkSemantics.THIS, ReleaseType.DATA);
            final Artifact a2 = new Artifact(URI.create("cadc:STUFF/thing2"), DataLinkSemantics.THIS, ReleaseType.DATA);
            final Artifact a3 = new Artifact(URI.create("cadc:STUFF/thing3"), DataLinkSemantics.THIS, ReleaseType.DATA);
            o.getPlanes().add(p);
            obsDAO.put(o);
            
            // add artifacts with decent time separations
            Thread.sleep(10);
            p.getArtifacts().add(a1);
            obsDAO.put(o);

            Thread.sleep(20);
            p.getArtifacts().add(a2);
            obsDAO.put(o);

            Thread.sleep(10);
            p.getArtifacts().add(a3);
            obsDAO.put(o);

            try {
                ResourceIterator<Artifact> iter = dao.iterator("cadc:STUFF/", null, null, null);
            } catch (UnsupportedOperationException expected) {
                log.info("caught expected: " + expected);
            }
            
            // cleanup
            obsDAO.delete(o.getID());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetByURI() {
        try {
            // test null param
            try {
                URI uri = null;
                dao.get(uri);
                Assert.fail("expected an exception");
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }

            // test not found
            // single tick to verify that library correctly handles untrusted input via PreparedStatement
            URI uri = URI.create("cadc:not'found");
            Assert.assertNull("expected null artifact", dao.get(uri));

            // test found
            uri = URI.create("cadc:STUFF/thing");
            Artifact a = new Artifact(uri, DataLinkSemantics.THIS, ReleaseType.DATA);

            Observation o = new SimpleObservation("FOO", URI.create("caom:FOO/bar"), SimpleObservation.EXPOSURE);
            Plane p = new Plane(URI.create(o.getURI().toASCIIString() + "/baz"));
            p.publisherID = p.getURI();
            o.getPlanes().add(p);
            p.getArtifacts().add(a);
            obsDAO.put(o);
            Artifact a2 = dao.get(uri);
            log.info("found: " + a2);
            Assert.assertNotNull("expected non-null artifact", a2);
            Assert.assertEquals("expected equal artifacts", a, a2);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
