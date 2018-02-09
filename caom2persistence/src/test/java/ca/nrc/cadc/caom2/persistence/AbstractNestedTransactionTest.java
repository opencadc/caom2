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
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.SimpleObservation;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataIntegrityViolationException;

/**
 *
 * @author pdowler
 */
public abstract class AbstractNestedTransactionTest {

    private static final Logger log = Logger.getLogger(AbstractNestedTransactionTest.class);

    boolean useLongForUUID;
    ObservationDAO dao;

    Class[] ENTITY_CLASSES
            = {
                Chunk.class, Part.class, Artifact.class, Plane.class, Observation.class
            };

    protected AbstractNestedTransactionTest(Class genClass, String server, String database, String schema, boolean useLongForUUID)
            throws Exception {
        this.useLongForUUID = useLongForUUID;
        try {
            Map<String, Object> config = new TreeMap<String, Object>();
            config.put("server", server);
            config.put("database", database);
            config.put("schema", schema);
            config.put(SQLGenerator.class.getName(), genClass);
            config.put("basePublisherID", "ivo://opencadc.org/");
            this.dao = new ObservationDAO();
            dao.setConfig(config);
        } catch (Exception ex) {
            // make sure it gets fully dumped
            log.error("setup DataSource failed", ex);
            throw ex;
        }
    }

    @Test
    public void testNestedTransaction() {
        // the ObservationDAO class uses a txn inside the put and delete methods
        // this verifies that it it fails and does a rollback that an outer txn
        // can still proceed -- eg it is really nested
        try {
            String uniqID1 = "bar-" + UUID.randomUUID().toString();
            String uniqID2 = "bar2-" + UUID.randomUUID().toString();
            Observation obs1 = new SimpleObservation("FOO", uniqID1);
            dao.put(obs1);
            Assert.assertTrue(dao.exists(obs1.getURI()));
            log.info("created: " + obs1);

            Observation dupe = new SimpleObservation("FOO", uniqID1);
            Observation obs2 = new SimpleObservation("FOO", uniqID2);

            log.info("start outer");
            dao.getTransactionManager().startTransaction(); // outer txn
            try {
                dao.put(dupe); // nested txn
                Assert.fail("expected exception, successfully put duplicate observation");
            } catch (DataIntegrityViolationException expected) {
                log.info("caught expected: " + expected);
            }

            dao.put(obs2); // another nested txn
            log.info("created: " + obs2);
            Assert.assertTrue(dao.exists(obs2.getURI()));

            log.info("commit outer");
            dao.getTransactionManager().commitTransaction(); // outer txn
            log.info("commit outer [OK]");

            Observation check1 = dao.get(obs1.getURI());
            Assert.assertNotNull(check1);
            Assert.assertEquals(obs1.getID(), check1.getID());

            Observation check2 = dao.get(obs2.getURI());
            Assert.assertNotNull(check2);
            Assert.assertEquals(obs2.getID(), check2.getID());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
