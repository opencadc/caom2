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

import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ObservationTest 
{
    private static final Logger log = Logger.getLogger(ObservationTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
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
    public void testConstructor()
    {
        try
        {
            Observation o = new SimpleObservation("Stuff", "Thing");
            log.debug("created: " + o);
            Assert.assertEquals("Stuff", o.getCollection());
            Assert.assertEquals("Thing", o.getObservationID());

            o = new CompositeObservation("Stuff", "Thing", new Algorithm("doit"));
            Assert.assertEquals("Stuff", o.getCollection());
            Assert.assertEquals("Thing", o.getObservationID());
            
            try 
            {
                o = new CompositeObservation("Stuff", "Thing", null);
                Assert.fail("excpected IllegalArgumentException from " + o);
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testEquals()
    {
        try
        {
            Observation cat = new SimpleObservation("Stuff", "CatInTheHat");
            Observation thing1 = new SimpleObservation("Stuff", "Thing");
            Observation thing2 = new SimpleObservation("Stuff", "Thing");
            Observation dog = new SimpleObservation("Dog", "Thing");

            Assert.assertTrue(thing1.equals(thing1));
            Assert.assertTrue(thing1.equals(thing2));
            
            Assert.assertFalse(cat.equals(thing1));
            Assert.assertFalse(thing1.equals(cat));
            
            Assert.assertFalse(cat.equals(null));
            Assert.assertFalse(cat.equals("foo"));
            Assert.assertFalse(thing1.equals(dog));

            // TODO: verify that changing any other field does not effect equals()?
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testHashCode()
    {
        try
        {
            Observation cat = new SimpleObservation("Stuff", "CatInTheHat");
            Observation thing1 = new SimpleObservation("Stuff", "Thing");
            Observation thing2 = new SimpleObservation("Stuff", "Thing");

            Assert.assertTrue(thing1.hashCode() == thing2.hashCode());

            Assert.assertFalse(cat.hashCode() == thing1.hashCode());

            // TODO: verify that changing any other field does not effect hashCode()?
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSetAlgorithmSimple()
    {
        try
        {
            Observation o = new SimpleObservation("Stuff", "Thing");
            Assert.assertEquals(SimpleObservation.ALGORITHM.getName(), o.getAlgorithm().getName());

            o.setAlgorithm(SimpleObservation.ALGORITHM);
            Assert.assertEquals(SimpleObservation.ALGORITHM.getName(), o.getAlgorithm().getName());
            
            o.setAlgorithm(new Algorithm("exposure"));
            Assert.assertEquals(SimpleObservation.ALGORITHM.getName(), o.getAlgorithm().getName());

            try 
            {
                o.setAlgorithm(null);
                Assert.fail("excpected IllegalArgumentException for null");
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            Assert.assertEquals(SimpleObservation.ALGORITHM.getName(), o.getAlgorithm().getName());

            try 
            {
                o.setAlgorithm(new Algorithm("foo"));
                Assert.fail("excpected IllegalArgumentException for foo");
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            Assert.assertEquals(SimpleObservation.ALGORITHM.getName(), o.getAlgorithm().getName());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSetAlgorithmComposite()
    {
        try
        {
            Observation o = new CompositeObservation("Stuff", "Thing", new Algorithm("doit"));
            Assert.assertEquals("doit", o.getAlgorithm().getName());

            o.setAlgorithm(new Algorithm("foo"));
            Assert.assertEquals("foo", o.getAlgorithm().getName());

            try 
            {
                o.setAlgorithm(SimpleObservation.ALGORITHM);
                Assert.fail("excpected IllegalArgumentException for " + SimpleObservation.ALGORITHM);
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            Assert.assertEquals("foo", o.getAlgorithm().getName());

            try 
            {
                o.setAlgorithm(new Algorithm("exposure"));
                Assert.fail("excpected IllegalArgumentException for exposure");
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            Assert.assertEquals("foo", o.getAlgorithm().getName());

            try 
            {
                o.setAlgorithm(null);
                Assert.fail("excpected IllegalArgumentException for null");
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            Assert.assertEquals("foo", o.getAlgorithm().getName());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPlaneSet()
    {
        try
        {
            Observation o = new SimpleObservation("Stuff", "Thing");
            Assert.assertNotNull(o.getPlanes());
            Assert.assertEquals(0, o.getPlanes().size());

            // add something
            boolean added = o.getPlanes().add(new Plane("thing1"));
            Assert.assertTrue("add thing1", added);
            Assert.assertEquals(1, o.getPlanes().size());

            // fail to add a duplicate
            added = o.getPlanes().add(new Plane("thing1"));
            Assert.assertFalse("add duplicate thing1", added);
            Assert.assertEquals(1, o.getPlanes().size());

            // add a non-dupe
            added = o.getPlanes().add(new Plane("thing2"));
            Assert.assertTrue("add thing2", added);
            Assert.assertEquals(2, o.getPlanes().size());

            o.getPlanes().clear();
            Assert.assertEquals(0, o.getPlanes().size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMemberSet()
    {
        try
        {
            CompositeObservation o = new CompositeObservation("Stuff", "Thing", new Algorithm("doit"));
            Assert.assertNotNull(o.getMembers());
            Assert.assertEquals(0, o.getMembers().size());

            // add something
            boolean added = o.getMembers().add(new ObservationURI("Stuff", "thing1"));
            Assert.assertTrue("add thing1", added);
            Assert.assertEquals(1, o.getMembers().size());

            // fail to add a duplicate
            added = o.getMembers().add(new ObservationURI("Stuff", "thing1"));
            Assert.assertFalse("add duplicate thing1", added);
            Assert.assertEquals(1, o.getMembers().size());

            // add a non-dupe
            added = o.getMembers().add(new ObservationURI("Stuff", "thing2"));
            Assert.assertTrue("add thing2", added);
            Assert.assertEquals(2, o.getMembers().size());

            o.getMembers().clear();
            Assert.assertEquals(0, o.getMembers().size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
