/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

/**
 *
 * @author pdowler
 */
public class CaomRepoConfigTest {
    private static final Logger log = Logger.getLogger(CaomRepoConfigTest.class);
    
    private static final String OPERATOR_GROUP = "ivo://cadc.nrc.ca/gms?CADC";
    private static final String STAFF_GROUP = "ivo://cadc.nrc.ca/gms?JCMT-Staff";

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    // @Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetItem() {
        try {
            Properties props = new Properties();
            props.setProperty("space",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org "
                + "proposalGroup=true operatorGroup=" + OPERATOR_GROUP + " staffGroup=" + STAFF_GROUP);
            props.setProperty("group-frag",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms#group1 ivo://cadc.nrc.ca/gms#group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org "
                + "proposalGroup=false operatorGroup=" + OPERATOR_GROUP + " staffGroup=" + STAFF_GROUP);
            props.setProperty("spaces",
                "dsname  database  schema  caom2obs  ivo://cadc.nrc.ca/gms?group1  ivo://cadc.nrc.ca/gms?group2  "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org "
                + "proposalGroup=false  operatorGroup="  +  OPERATOR_GROUP);
            props.setProperty("tabs",
                "dsname\tdatabase\tschema\tcaom2obs\tivo://cadc.nrc.ca/gms?group1\tivo://cadc.nrc.ca/gms?group2\t"
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl\tbasePublisherID=ivo://opencadc.org\t"
                + "proposalGroup=true\tstaffGroup=" + STAFF_GROUP);
            props.setProperty("mix",
                "dsname \t database\t schema \tcaom2obs \t ivo://cadc.nrc.ca/gms?group1 \t ivo://cadc.nrc.ca/gms?group2 \t "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl \t basePublisherID=ivo://opencadc.org \t "
                + "proposalGroup=true\t operatorGroup=" + OPERATOR_GROUP + "\t staffGroup=" + STAFF_GROUP);
            props.setProperty("def-impl", 
                "dsname \t database\t schema \tcaom2obs \t ivo://cadc.nrc.ca/gms?group1 \t ivo://cadc.nrc.ca/gms?group2 \t "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl \t basePublisherID=ivo://opencadc.org ");
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("space", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(true, it.getProposalGroup());
            Assert.assertEquals(new GroupURI(OPERATOR_GROUP), it.getOperatorGroup());
            Assert.assertEquals(new GroupURI(STAFF_GROUP), it.getStaffGroup());

            it = CaomRepoConfig.getItem("group-frag", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("group-frag", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms#group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms#group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(false, it.getProposalGroup());
            Assert.assertEquals(new GroupURI(OPERATOR_GROUP), it.getOperatorGroup());
            Assert.assertEquals(new GroupURI(STAFF_GROUP), it.getStaffGroup());

            it = CaomRepoConfig.getItem("spaces", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("spaces", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(false, it.getProposalGroup());
            Assert.assertEquals(new GroupURI(OPERATOR_GROUP), it.getOperatorGroup());
            Assert.assertNull(it.getStaffGroup());

            it = CaomRepoConfig.getItem("tabs", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("tabs", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(true, it.getProposalGroup());
            Assert.assertNull(it.getOperatorGroup());
            Assert.assertEquals(new GroupURI(STAFF_GROUP), it.getStaffGroup());

            it = CaomRepoConfig.getItem("mix", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("mix", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(true, it.getProposalGroup());
            Assert.assertEquals(new GroupURI(OPERATOR_GROUP), it.getOperatorGroup());
            Assert.assertEquals(new GroupURI(STAFF_GROUP), it.getStaffGroup());

            it = CaomRepoConfig.getItem("def-impl", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("def-impl", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(false, it.getProposalGroup());
            Assert.assertNull(it.getOperatorGroup());
            Assert.assertNull(it.getStaffGroup());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPublicRead() {
        try {
            Properties props = new Properties();
            props.setProperty("space1",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org ");
            props.setProperty("space2",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org publicRead=false");
            props.setProperty("space3",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org publicRead=true");
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space1", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("space1", it.getCollection());
            Assert.assertFalse(it.getPublicRead());

            it = CaomRepoConfig.getItem("space2", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("space2", it.getCollection());
            Assert.assertFalse(it.getPublicRead());

            it = CaomRepoConfig.getItem("space3", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("space3", it.getCollection());
            Assert.assertTrue(it.getPublicRead());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testBasePublisherID() {
        try {
            String expected = "ivo://opencadc.org";
            Properties props = new Properties();
            props.setProperty("space1",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                    + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=" + expected);
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space1", props);
            Assert.assertNotNull(it);
            log.debug("found: " + it);
            Assert.assertEquals("space1", it.getCollection());
            Assert.assertEquals(expected, it.getBasePublisherID().toASCIIString());
            
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidBasePublisherID() {
        try {
            Properties props = new Properties();
            props.setProperty("space1",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=foo:bar");
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space1", props);
            Assert.fail("expected IllegalArgumentException but found: " + it);
        } catch (IllegalArgumentException expected) {
            log.info("testInvalidBasePublisherID: caught " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidOption() {
        try {
            Properties props = new Properties();
            props.setProperty("space1",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org foo");
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space1", props);
            Assert.fail("expected IllegalArgumentException but found: " + it);
        } catch (IllegalArgumentException expected) {
            log.info("testInvalidBasePublisherID: caught " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        
        try {
            Properties props = new Properties();
            props.setProperty("space1",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org foo=bar=42");
            
            CaomRepoConfig.Item it = CaomRepoConfig.getItem("space1", props);
            Assert.fail("expected IllegalArgumentException but found: " + it);
        } catch (IllegalArgumentException expected) {
            log.info("testInvalidBasePublisherID: caught " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testProposalGroupNotBoolean() {
        try {
            Properties props = new Properties();
            props.setProperty("proposal-group-not-boolean",
                    "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                    + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org "
                    + "proposalGroup=groupURI");

            CaomRepoConfig.Item it = CaomRepoConfig.getItem("proposal-group-not-boolean", props);
            Assert.fail("expected IllegalArgumentException to have been thrown");
        } catch (IllegalArgumentException ex) {
            log.debug("caught expected: " + ex);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testMissingStaffGroup() {
        try {
            Properties props = new Properties();
            props.setProperty("missing-staffGroup",
                    "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 "
                    + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org "
                    + "proposalGroup=true operatorGroup=" + OPERATOR_GROUP);

            CaomRepoConfig.Item it = CaomRepoConfig.getItem("missing-staffGroup", props);
            Assert.fail("expected IllegalArgumentException to have been thrown");
        } catch (IllegalArgumentException ex) {
            if (ex.getMessage().contains("staff group is not specified")) {
                log.debug("caught expected: " + ex);
            } else {
                Assert.fail("unexpected exception: " + ex);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMissingTokens() {
        try {
            Properties props = new Properties();
            props.setProperty("too-short", "dsname database schema caom2obs ivo://cadc.nrc.ca/gms#group1");

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("too-short", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidURI() {
        try {
            Properties props = new Properties();
            props.setProperty("invalid-syntax-queries",
                "dsname database schema caom2obs ivo:gms?group1?group1 ivo://cadc.nrc.ca/gms?group2 true true true "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org ");
            props.setProperty("invalid-syntax-frags",
                "dsname database schema caom2obs ivo:gms#group1#group1 ivo://cadc.nrc.ca/gms#group2 false true false "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org ");
            props.setProperty("no-name",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms ivo://cadc.nrc.ca/gms?group2 true false false "
                + "ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org");
            props.setProperty("wrong-scheme",
                "dsname database schema caom2obs gms://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 false false "
                + "true ca.nrc.cadc.caom2.repo.DummySQLGeneratorImpl basePublisherID=ivo://opencadc.org");
            props.setProperty("no-sql-impl",
                "dsname database schema caom2obs ivo://cadc.nrc.ca/gms?group1 ivo://cadc.nrc.ca/gms?group2 false false false "
                + "ca.nrc.cadc.caom2.repo.NoImpl basePublisherID=ivo://opencadc.org ");

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("invalid-syntax-queries", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("invalid-syntax-frags", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("no-name", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("wrong-scheme", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

            try {
                CaomRepoConfig.Item i1 = CaomRepoConfig.getItem("no-sql-impl", props);
                Assert.fail("expected IllegalArgumentException, got: " + i1);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected: " + expected);
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testLoadFile() {
        try {
            File cf = FileUtil.getFileFromResource("CaomRepoConfig.properties", CaomRepoConfigTest.class);
            List<CaomRepoConfig.Item> items = CaomRepoConfig.loadConfig(cf);
            Assert.assertNotNull(items);
            Assert.assertEquals(1, items.size());

            CaomRepoConfig.Item it = items.get(0);
            Assert.assertEquals("TEST_OK", it.getCollection());
            Assert.assertEquals("dsname", it.getDataSourceName());
            Assert.assertEquals("database", it.getDatabase());
            Assert.assertEquals("schema", it.getSchema());
            Assert.assertEquals("database.schema.caom2obs", it.getTestTable());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group1"), it.getReadOnlyGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?group2"), it.getReadWriteGroup());
            Assert.assertEquals(DummySQLGeneratorImpl.class, it.getSqlGenerator());
            Assert.assertEquals(true, it.getProposalGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?CADC"), it.getOperatorGroup());
            Assert.assertEquals(new GroupURI("ivo://cadc.nrc.ca/gms?JCMT-Staff"), it.getStaffGroup());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
