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
import java.net.URI;
import java.util.ArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class PlaneTest 
{
    private static final Logger log = Logger.getLogger(PlaneTest.class);

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
            Plane p = new Plane("ShinyNewProduct");
            Assert.assertEquals("ShinyNewProduct", p.getProductID());

            try
            {
                p = new Plane(null);
                Assert.fail("expected IllegalArgumentException for productID=null");
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
    public void testArtifactSet()
    {
        try
        {
            Plane p = new Plane("thing1");
            Assert.assertEquals(0, p.getArtifacts().size());

            // add something
            boolean added = p.getArtifacts().add(new Artifact(new URI("foo", "abc", null)));
            Assert.assertTrue("foo:abc", added);
            Assert.assertEquals(1, p.getArtifacts().size());

            // fail to add duplicate
            added = p.getArtifacts().add(new Artifact(new URI("foo", "abc", null)));
            Assert.assertFalse("foo:abc", added);
            Assert.assertEquals(1, p.getArtifacts().size());

            // add non-dupe
            added = p.getArtifacts().add(new Artifact(new URI("foo", "def", null)));
            Assert.assertTrue("foo:abc", added);
            Assert.assertEquals(2, p.getArtifacts().size());

            // clear
            p.getArtifacts().clear();
            Assert.assertEquals(0, p.getArtifacts().size());
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
            Plane plane = new Plane("foo");
            Plane eq = new Plane("foo");
            Plane neq = new Plane("bar");
            
            Assert.assertTrue( plane.equals(plane) );

            log.debug("equals: " + plane + " == " + eq);
            Assert.assertTrue( plane.equals(eq) );
            
            log.debug("equals: " + plane + " != " + neq);
            Assert.assertFalse( plane.equals(neq) );
            
            Assert.assertFalse( plane.equals(null) );
            Assert.assertFalse( plane.equals(new Integer(1)) ); // a different class
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetURI()
    {
        try
        {
            ObservationURI uri = new ObservationURI("FOO", "bar");
            Plane plane = new Plane("foo");
            
            PlaneURI puri = plane.getURI(uri);
            Assert.assertEquals("caom:FOO/bar/foo", puri.getURI().toASCIIString());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCompute()
    {
        try
        {
            Plane plane = new Plane("foo");
            
            Position pos = plane.getPosition();
            Position pos2 = plane.getPosition();
            Assert.assertNotNull(pos);
            Assert.assertTrue(pos == pos2); // same object both times
            
            Energy nrg = plane.getEnergy();
            Energy nrg2 = plane.getEnergy();
            Assert.assertNotNull(nrg);
            Assert.assertTrue(nrg == nrg2); // same object both times
            
            Time tim = plane.getTime();
            Time tim2 = plane.getTime();
            Assert.assertNotNull(tim);
            Assert.assertTrue(tim == tim2); // same object both times
            
            Polarization pol = plane.getPolarization();
            Polarization pol2 = plane.getPolarization();
            Assert.assertNotNull(pol);
            Assert.assertTrue(pol == pol2); // same object both times
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testTransientState()
    {
        try
        {
            Plane plane = new Plane("foo");
            
            int defCode = plane.getStateCode();
            log.debug("testTransientState: " + defCode);
            int nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("default code", defCode, nonTransientCode);
            int transientCode = plane.getStateCode(true);
            log.debug("testTransientState: " + transientCode);
            Assert.assertEquals("not computed", defCode, transientCode);

            plane.clearTransientState();
            plane.computeTransientState();
            transientCode = plane.getStateCode(true);
            Assert.assertEquals("null computed", defCode, transientCode);

            plane.clearTransientState();
            assignPos(plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            transientCode = plane.getStateCode(true);
            log.debug("testTransientState: " + transientCode);
            Assert.assertTrue("computed position", defCode != transientCode);

            plane.clearTransientState();
            assignEnergy(plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            transientCode = plane.getStateCode(true);
            log.debug("testTransientState: " + transientCode);
            Assert.assertTrue("computed position", defCode != transientCode);

            plane.clearTransientState();
            assignTime(plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            transientCode = plane.getStateCode(true);
            log.debug("testTransientState: " + transientCode);
            Assert.assertTrue("computed position", defCode != transientCode);

            plane.clearTransientState();
            assignPol(plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            transientCode = plane.getStateCode(true);
            log.debug("testTransientState: " + transientCode);
            Assert.assertTrue("computed position", defCode != transientCode);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void assignPos(Plane p)
    {
        Position pos = p.getPosition();
        Assert.assertNotNull(pos);
        pos.resolution = new Double(0.01);
    }

    private void assignEnergy(Plane p)
    {
        Energy nrg = p.getEnergy();
        nrg.bandpassName = "foo123";
    }

    private void assignTime(Plane p)
    {
        Time tim = p.getTime();
        tim.exposure = new Double(123.0);
    }

    private void assignPol(Plane p)
    {
        Polarization pol = p.getPolarization();
        pol.states = new ArrayList<PolarizationState>();
        pol.states.add(PolarizationState.I);
        pol.dimension = new Integer(1);
    }

}
