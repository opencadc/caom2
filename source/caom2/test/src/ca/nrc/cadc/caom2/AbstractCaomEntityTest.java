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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.util.Log4jInit;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AbstractCaomEntityTest 
{
    private static final Logger log = Logger.getLogger(AbstractCaomEntityTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    AbstractCaomEntity[] entities;

    public AbstractCaomEntityTest()
    {
        try
        {
            SimpleObservation so = new SimpleObservation("FOO", "bar");
            CompositeObservation co = new CompositeObservation("FOO", "bar", new Algorithm("doit"));
            co.getMembers().add(new ObservationURI("foo", "baz"));
            
            Plane pl = new Plane("thing");
            pl.provenance = new Provenance("doit");
            pl.provenance.getInputs().add(new PlaneURI(new ObservationURI("foo", "baz"), "thing"));

            Artifact ar = new Artifact(new URI("ad", "FOO/bar", null));
            Part pa = new Part("x");
            Chunk ch = new Chunk();

            // need to add child objects so the state vs child fields get counted correctly
            so.getPlanes().add(pl);
            co.getPlanes().add(pl);
            pl.getArtifacts().add(ar);
            ar.getParts().add(pa);
            pa.getChunks().add(ch);
            
            entities = new AbstractCaomEntity[] {
                so,
                co,
                pl,
                ar,
                pa,
                ch
            };
        }
        catch(Exception bug)
        {
            log.error("BUG: test setup", bug);
        }
    }

    static int[] expectedStateFields = { 12, 13, 7, 5, 2, 13 };
    static int[] expectedStateFieldsWithTrans = { 12, 13, 11, 5, 2, 13 };
    static int[] expectedChildFields = { 1, 1, 1, 1, 1, 0 };

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
    public void testMaxDate()
    {
        try
        {
            Date d1 = new Date(100000L);
            Date d2 = new Date(200000L);
            Assert.assertEquals(d2, AbstractCaomEntity.max(d1, d2));
            Assert.assertEquals(d2, AbstractCaomEntity.max(d2, d1));
            Assert.assertEquals(d2, AbstractCaomEntity.max(d2, d2));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetStateFields()
    {
        
        try
        {
            Assert.assertEquals("test setup", entities.length, expectedStateFields.length);
            for (int i=0; i<entities.length; i++)
            {
                Object o = entities[i];
                Class c = o.getClass();
                log.debug("class: " + c.getName());
                SortedSet<Field> fields = AbstractCaomEntity.getStateFields(c, false);
                for (Field f : fields)
                    log.debug("state: " + f.getName() + " type: " + f.getType().getName());
                Assert.assertEquals("number of state fields:  " + c.getName(), expectedStateFields[i], fields.size());

                fields = AbstractCaomEntity.getStateFields(c, true);
                for (Field f : fields)
                    log.debug("state: " + f.getName() + " type: " + f.getType().getName());
                Assert.assertEquals("number of state fields (w/ transient):  " + c.getName(), expectedStateFieldsWithTrans[i], fields.size());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetChildFields()
    {
        try
        {
            Assert.assertEquals("test setup", entities.length, expectedChildFields.length);
            for (int i=0; i<entities.length; i++)
            {
                Object o = entities[i];
                Class c = o.getClass();
                log.debug("class: " + c.getName());
                List<Field> fields = AbstractCaomEntity.getChildFields(c);
                for (Field f : fields)
                    log.debug("children: " + f.getName() + " type: " + f.getType().getName());
                Assert.assertEquals("number of child fields:  " + c.getName(), expectedChildFields[i], fields.size());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testChecksum()
    {
        try
        {
            for (AbstractCaomEntity ce : entities)
            {
                int c = ce.getStateCode();
                log.debug("entity: " + ce.getClass().getName() + " checksum: " + c);
            }

            // check that an "empty" objects checksum is equal to the hashCode of the class name
            Chunk chunk = new Chunk();
            int expected = 0;
            int actual = chunk.getStateCode();
            Assert.assertEquals("empty Chunk code", expected, actual);

            // try something with primitive types
            CoordAxis1D axis = new CoordAxis1D(new Axis("WAV", "m"));
            axis.range = new CoordRange1D(new RefCoord(1.0, 1.0), new RefCoord(10.0, 10.0));
            chunk.energy = new SpectralWCS(axis, "TOPOCENT");
            actual = chunk.getStateCode();
            Assert.assertFalse("changed Chunk code", expected == actual);

            Observation o = new SimpleObservation("foo", "bar");
            expected = o.getStateCode();
            o.environment = new Environment();
            actual = o.getStateCode();
            Assert.assertEquals("empty Environment code", expected, actual);

            Plane p = new Plane("baz");
            expected = p.getStateCode();
            p.metrics = new Metrics();
            actual = p.getStateCode();
            Assert.assertEquals("empty Metrics code", expected, actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    //@Test
    public void testGetMaxLastModified()
    {
        try
        {
            Date t0 = new Date(100000L);
            
            SimpleObservation observation = new SimpleObservation("stuff", "thing");
            Plane plane = new Plane("foo");
            Artifact artifact = new Artifact(new URI("ad", "foo/bar.fits", null));
            Part part = new Part("x");
            Chunk chunk = new Chunk();

            observation.getPlanes().add(plane);
            plane.getArtifacts().add(artifact);
            artifact.getParts().add(part);
            part.getChunks().add(chunk);

            Date d1 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t1 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d1);
            Assert.assertTrue("t0 < d1", t0.getTime() < d1.getTime());
            Assert.assertTrue("d1 < t1", d1.getTime() < t1.getTime());

            observation.instrument = new Instrument("instr");
            Date d2 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t2 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d2);
            Assert.assertTrue("t1 < d2", t1.getTime() < d2.getTime());
            Assert.assertTrue("d2 < t2", d2.getTime() < t2.getTime());

            plane.calibrationLevel = CalibrationLevel.CALIBRATED;
            Date d3 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t3 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d3);
            Assert.assertTrue("t2 < d3", t2.getTime() < d3.getTime());
            Assert.assertTrue("d3 < t3", d3.getTime() < t3.getTime());

            artifact.productType = ProductType.SCIENCE;
            Date d4 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t4 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d4);
            Assert.assertTrue("t3 < d4", t3.getTime() < d4.getTime());
            Assert.assertTrue("d4 < t4", d4.getTime() < t4.getTime());

            part.productType = ProductType.PREVIEW;
            Date d5 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t5 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d5);
            Assert.assertTrue("t4 < d5", t4.getTime() < d5.getTime());
            Assert.assertTrue("d5 < t5", d5.getTime() < t5.getTime());

            chunk.productType = ProductType.INFO;
            Date d6 = observation.getMaxLastModified();
            Thread.sleep(10L);
            Date t6 = new Date();
            Thread.sleep(10L);

            Assert.assertNotNull(d6);
            Assert.assertTrue("t5 < d6", t5.getTime() < d6.getTime());
            Assert.assertTrue("d6 < t6", d6.getTime() < t6.getTime());

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
