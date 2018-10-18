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
*  <http:www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http:www.gnu.org/licenses/>.
*
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.util.Log4jInit;
import java.lang.reflect.Field;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * This tests the methods defined in the CaomEntity interface. The code is typically in
 * the CaomEntity class.
 *
 * @author pdowler
 */
public class CaomEntityTest {

    private static final Logger log = Logger.getLogger(CaomEntityTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    CaomEntity[] entities;

    public CaomEntityTest() {
        try {
            ObservationURI uri = new ObservationURI("FOO", "bar");
            SimpleObservation so = new SimpleObservation("FOO", "bar");
            CompositeObservation co = new CompositeObservation("FOO", "bar", new Algorithm("doit"));
            co.getMembers().add(new ObservationURI("foo", "baz"));

            Plane pl = new Plane("thing");
            pl.provenance = new Provenance("doit");
            pl.provenance.getInputs().add(new PlaneURI(uri, "thing"));

            Artifact ar = new Artifact(new URI("ad", "FOO/bar", null), ProductType.SCIENCE, ReleaseType.DATA);
            Part pa = new Part("x");
            Chunk ch = new Chunk();

            // need to add child objects so the state vs child fields get counted correctly
            so.getPlanes().add(pl);
            co.getPlanes().add(pl);
            pl.getArtifacts().add(ar);
            ar.getParts().add(pa);
            pa.getChunks().add(ch);

            entities = new CaomEntity[]{
                so,
                co,
                pl,
                ar,
                pa,
                ch
            };
        } catch (Exception bug) {
            log.error("BUG: test setup", bug);
        }
    }

    static int[] expectedStateFields = {15, 16, 15, 6, 2, 13};
    static int[] expectedChildFields = {1, 1, 1, 1, 1, 0};

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testUUID() {
        try {
            UUID id;

            id = new UUID(0L, 0L);
            log.debug("[0,0] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(0L, 1L);
            log.debug("[0,1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(1L, 0L);
            log.debug("[1,0] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(1L, 1L);
            log.debug("[1,1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(0L, -1L);
            log.debug("[0,-1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(-1L, 0L);
            log.debug("[-1,0] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(1L, -1L);
            log.debug("[1,-1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(-1L, 1L);
            log.debug("[-1,1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(-1L, -1L);
            log.debug("[-1,-1] as UUID: " + id.toString());
            Assert.assertEquals(id, UUID.fromString(id.toString()));

            id = new UUID(0, 666L);
            log.debug("[0,666] as UUID: " + id.toString());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testEquals() {
        try {
            // only Chunk does not override equals/compareTo/hasCode
            Chunk o1 = new Chunk();
            Chunk o2 = new Chunk();

            Assert.assertTrue(o1.equals(o1));

            Assert.assertFalse(o1.equals(null));

            Assert.assertFalse(o1.equals("foo"));

            Assert.assertFalse(o1.equals(o2));

            // simulate serialize/deserialize so different object with same numeric id
            assignID(o2, o1.getID());
            Assert.assertTrue(o1.equals(o2));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private static void assignID(Object ce, UUID id) {
        try {
            Field f = CaomEntity.class.getDeclaredField("id");
            f.setAccessible(true);
            f.set(ce, id);
        } catch (NoSuchFieldException fex) {
            throw new RuntimeException("BUG", fex);
        } catch (IllegalAccessException bug) {
            throw new RuntimeException("BUG", bug);
        }
    }

    @Test
    public void testMaxDate() {
        try {
            Date d1 = new Date(100000L);
            Date d2 = new Date(200000L);
            Assert.assertEquals(d2, CaomEntity.max(d1, d2));
            Assert.assertEquals(d2, CaomEntity.max(d2, d1));
            Assert.assertEquals(d2, CaomEntity.max(d2, d2));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetStateFields() {

        try {
            Assert.assertEquals("test setup", entities.length, expectedStateFields.length);
            for (int i = 0; i < entities.length; i++) {
                Object o = entities[i];
                Class c = o.getClass();
                log.debug("class: " + c.getName());
                SortedSet<Field> fields = CaomEntity.getStateFields(c, false);
                for (Field f : fields) {
                    log.debug("state: " + f.getName() + " type: " + f.getType().getName());
                }
                Assert.assertEquals("number of state fields:  " + c.getName(), expectedStateFields[i], fields.size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetChildFields() {
        try {
            Assert.assertEquals("test setup", entities.length, expectedChildFields.length);
            for (int i = 0; i < entities.length; i++) {
                Object o = entities[i];
                Class c = o.getClass();
                log.debug("class: " + c.getName());
                Set<Field> fields = CaomEntity.getChildFields(c);
                for (Field f : fields) {
                    log.debug("children: " + f.getName() + " type: " + f.getType().getName());
                }
                Assert.assertEquals("number of child fields:  " + c.getName(), expectedChildFields[i], fields.size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMetaChecksum() {
        try {
            for (CaomEntity ce : entities) {
                URI mc = ce.computeMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertNotNull("minimal entity checksum: " + ce.getClass().getName(), mc);
            }

            Observation obs = new SimpleObservation("FOO", "bar");
            URI mc1 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc1: " + mc1);
            Assert.assertNotNull(mc1);

            // enum
            obs.intent = ObservationIntentType.SCIENCE;
            URI mc2 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc2: " + mc2);
            Assert.assertNotEquals("CaomEnum changes checksum", mc1, mc2);

            // date
            obs.metaRelease = new Date();
            URI mc3 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc3: " + mc3);
            Assert.assertNotEquals("Date changes checksum", mc2, mc3);

            // empty substructure
            obs.environment = new Environment();
            URI mc4 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc4: " + mc4);
            Assert.assertEquals("empty substructure does not change checksum", mc3, mc4);

            // non-empty substructure with double
            obs.environment.ambientTemp = 2.0;
            URI mc5 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc5: " + mc5);
            Assert.assertNotEquals("double value in substructure changes checksum", mc4, mc5);

            // boolean
            obs.environment.photometric = Boolean.TRUE;
            URI mc6 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc6: " + mc6);
            Assert.assertNotEquals("boolean value in substructure changes checksum", mc5, mc6);

            // child
            Plane p = new Plane("baz");
            URI pc1 = p.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pc1: " + pc1);
            Assert.assertNotNull(pc1);

            obs.getPlanes().add(p);
            URI pc2 = p.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pc2: " + pc2);
            Assert.assertEquals("add to parent does not change child", pc1, pc2);

            URI mc7 = obs.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("mc7: " + mc7);
            Assert.assertEquals("add child does not change checksum of parent", mc6, mc7);

            // add artifact to test URI primitive
            Artifact a = new Artifact(URI.create("boo:Stuff/Nonsense"), ProductType.SCIENCE, ReleaseType.DATA);
            URI ac1 = a.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotNull(ac1);

            p.getArtifacts().add(a);
            URI ac2 = a.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("add to parent does not change child", ac1, ac2);

            Assert.assertNull("compute does not effect stored checksum", obs.getMetaChecksum());
            Assert.assertNull("compute does not effect stored acc checksum", obs.getAccMetaChecksum());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAccumulatedMetaChecksum() {
        try {
            for (CaomEntity ce : entities) {
                URI mc = ce.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertNotNull("minimal entity acc checksum: " + ce.getClass().getName(), mc);
            }

            Observation obs = new SimpleObservation("FOO", "bar");
            URI oc1 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc1: " + oc1);
            Assert.assertNotNull(oc1);

            // plane
            Plane pl = new Plane("baz");
            obs.getPlanes().add(pl);
            URI pc1 = pl.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotNull(pc1);

            URI oc2 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc2: " + oc2);
            Assert.assertNotEquals("add child changes acc checksum", oc1, oc2);

            // artifact
            Artifact a = new Artifact(URI.create("boo:Stuff/Nonsense"), ProductType.SCIENCE, ReleaseType.DATA);
            pl.getArtifacts().add(a);
            URI ac1 = a.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotNull(ac1);

            URI oc3 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc3: " + oc3);
            Assert.assertNotEquals("add child changes acc checksum", oc2, oc3);
            URI pc2 = pl.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pc2: " + pc2);
            Assert.assertNotEquals("add child changes acc checksum", pc1, pc2);

            Part pa = new Part("comp");
            a.getParts().add(pa);
            URI pac1 = pa.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotNull(pac1);

            URI oc4 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc4: " + oc4);
            Assert.assertNotEquals("add child changes acc checksum", oc3, oc4);
            URI pc3 = pl.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pc3: " + pc3);
            Assert.assertNotEquals("add child changes acc checksum", pc2, pc3);
            URI ac2 = a.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals("add child changes acc checksum", ac1, ac2);

            Chunk ch = new Chunk();
            pa.getChunks().add(ch);
            URI chc1 = ch.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotNull(chc1);

            URI oc5 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc5: " + oc5);
            Assert.assertNotEquals("add child changes acc checksum", oc4, oc5);
            URI pc4 = pl.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pc4: " + pc4);
            Assert.assertNotEquals("add child changes acc checksum", pc3, pc4);
            URI ac3 = a.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("ac3: " + ac3);
            Assert.assertNotEquals("add child changes acc checksum", ac2, ac3);
            URI pac2 = pa.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("pac2: " + pac2);
            Assert.assertNotEquals("add child changes acc checksum", pac1, pac2);

            Assert.assertNull("compute does not effect stored checksum", obs.getMetaChecksum());
            Assert.assertNull("compute does not effect stored acc checksum", obs.getAccMetaChecksum());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMetaChecksumIncludesUUID() {
        try {
            Assert.assertNotEquals("two planes with same metadata have different checksum",
                    new SimpleObservation("FOO", "bar").computeMetaChecksum(MessageDigest.getInstance("MD5")),
                    new SimpleObservation("FOO", "bar").computeMetaChecksum(MessageDigest.getInstance("MD5")));

            Assert.assertNotEquals("two planes with same metadata have different checksum",
                    new Plane("baz").computeMetaChecksum(MessageDigest.getInstance("MD5")),
                    new Plane("baz").computeMetaChecksum(MessageDigest.getInstance("MD5")));

            Assert.assertNotEquals("two artifacts with same metadata have different checksum",
                    new Artifact(new URI("foo:baz"), ProductType.PREVIEW, ReleaseType.META).computeMetaChecksum(MessageDigest.getInstance("MD5")),
                    new Artifact(new URI("foo:baz"), ProductType.PREVIEW, ReleaseType.META).computeMetaChecksum(MessageDigest.getInstance("MD5")));

            Assert.assertNotEquals("two parts with same metadata have different checksum",
                    new Part("baz").computeMetaChecksum(MessageDigest.getInstance("MD5")),
                    new Part("baz").computeMetaChecksum(MessageDigest.getInstance("MD5")));

            Assert.assertNotEquals("two chunks (empty) have different checksum",
                    new Chunk().computeMetaChecksum(MessageDigest.getInstance("MD5")),
                    new Chunk().computeMetaChecksum(MessageDigest.getInstance("MD5")));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAccMetaCheckIncludesUUID() {
        try {
            // test accumulated
            Observation obs = new SimpleObservation("FOO", "bar");
            URI orig = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc1: " + orig + " id1: " + obs.getID());
            Assert.assertNotNull(orig);

            // plane
            obs.getPlanes().add(new Plane("baz"));

            URI baz1 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("baz1: " + baz1);
            Assert.assertNotEquals("add child changes acc checksum", orig, baz1);

            obs.getPlanes().clear();
            Assert.assertTrue("clear worked", obs.getPlanes().isEmpty());

            URI nobaz = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("nobaz: " + nobaz);
            Assert.assertNotEquals("remove child changes acc checksum", baz1, nobaz);

            Assert.assertEquals("remove child reverts acc checksum", orig, nobaz);

            obs.getPlanes().add(new Plane("baz"));

            URI baz2 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("baz2: " + baz2);
            Assert.assertNotEquals("add child changes acc checksum", nobaz, baz2);
            Assert.assertNotEquals("add child with different UUID changes acc checksum", baz1, baz2);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAccMetaCheckChildOrder() {
        try {
            // test accumulated
            Observation obs = new SimpleObservation("FOO", "bar");
            CaomUtil.assignID(obs, new UUID(0l, 666l));
            URI orig = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("oc1: " + orig + " id1: " + obs.getID());
            Assert.assertNotNull(orig);

            // plane
            UUID uuid1 = new UUID(0l, 1l);
            UUID uuid2 = new UUID(0l, 2l);
            Plane p1 = new Plane("baz1");
            Plane p2 = new Plane("baz2");

            // important: Observation.getPlanes() Set sorts by productID so we have to reverse
            // the uuid order so this test will pass/fail correctly
            CaomUtil.assignID(p2, uuid1);
            CaomUtil.assignID(p1, uuid2);

            // add in reverse uuid order so that a List impl will fail test without explicit sort
            obs.getPlanes().add(p1);
            obs.getPlanes().add(p2);

            URI c1 = obs.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            log.debug("multi-plane accumulated checksum: " + c1);

            // this test only verifies that none of the values above including hard-coded UUIDs changed
            // and the accumulation order in the code did not change... feels icky
            URI expected = URI.create("md5:5a634dbc198c7e61c247f0049f1a0356");
            Assert.assertEquals("hard coded accumulated checksum", expected, c1);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
