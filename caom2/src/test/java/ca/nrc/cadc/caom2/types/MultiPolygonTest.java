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

package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.util.Log4jInit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class MultiPolygonTest 
{
    private static final Logger log = Logger.getLogger(MultiPolygonTest.class);

    private MultiPolygon target;
    private double expCenterX, expCenterY, expArea, expSize;
    private double expMSCX, expMSCY, expMSCR;
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
    }

    public MultiPolygonTest()
    {
        this.target = new MultiPolygon();
        target.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
        target.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
        target.getVertices().add(new Vertex(2.0, 4.0, SegmentType.LINE));
        target.getVertices().add(new Vertex(1.0, 3.0, SegmentType.LINE));
        target.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
        expCenterX = 2.0;
        expCenterY = 3.0;
        expArea = 2.0;
        expSize = 2.0;
        
        expMSCX = expCenterX;
        expMSCY = expCenterY;
        expMSCR = 1.0;
        
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
    public void testShapeInterface()
    {
        try
        {
            Assert.assertEquals(expCenterX, target.getCenter().cval1, 0.001);
            Assert.assertEquals(expCenterY, target.getCenter().cval2, 0.001);
            Assert.assertEquals(expArea, target.getArea(), 0.01);
            Assert.assertEquals(expSize, target.getSize(), 0.01);
            
            Assert.assertEquals(expMSCX, target.getMinimumSpanningCircle().getCenter().cval1, 0.001);
            Assert.assertEquals(expMSCY, target.getMinimumSpanningCircle().getCenter().cval2, 0.001);
            Assert.assertEquals(expMSCR, target.getMinimumSpanningCircle().getRadius(), 0.001);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testEncodeDecode()
    {
        try
        {
            byte[] encoded = MultiPolygon.encode(target);
            MultiPolygon decoded = MultiPolygon.decode(encoded);
            Assert.assertNotNull(decoded);
            Assert.assertEquals(target.getVertices().size(), decoded.getVertices().size());
            for (int i=0; i<target.getVertices().size(); i++)
            {
                Vertex v1 = target.getVertices().get(i);
                Vertex v2 = decoded.getVertices().get(i);
                Assert.assertEquals(v1.cval1, v2.cval1, 0.0);
                Assert.assertEquals(v1.cval2, v2.cval2, 0.0);
                Assert.assertEquals(v1.getType(), v2.getType());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    // polygon winding driection: viewed in a tangent plane from outside the
    // unit sphere so RA increases to the right and DEC increases to the north
    
    @Test
    public void testPolygonWindingCCW()
    {
        try
        {
            MultiPolygon p = new MultiPolygon();
            p.getVertices().add(new Vertex(1.0, 4.0, SegmentType.MOVE));
            p.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            p.getVertices().add(Vertex.CLOSE);
            
            Assert.assertTrue(p.getCCW());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    @Test
    public void testPolygonWindingCW()
    {
        try
        {
            MultiPolygon p = new MultiPolygon();
            p.getVertices().add(new Vertex(2.0, 2.0, SegmentType.MOVE));
            p.getVertices().add(new Vertex(3.0, 3.0, SegmentType.LINE));
            p.getVertices().add(new Vertex(1.0, 4.0, SegmentType.LINE));
            
            p.getVertices().add(Vertex.CLOSE);
            
            Assert.assertFalse(p.getCCW());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPolygonWindingMeridianCCW()
    {
        try
        {
            MultiPolygon poly = new MultiPolygon();
            poly.getVertices().add(new Vertex(358.0, -2.0, SegmentType.MOVE));
            poly.getVertices().add(new Vertex(358.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, -2.0, SegmentType.LINE));
            poly.getVertices().add(Vertex.CLOSE);
            
            Assert.assertEquals("area check", 16.0, poly.getArea(), 0.02);
            Assert.assertTrue("polygon on meridian ccw", poly.getCCW());
            
            poly.getVertices().clear(); // check range reduction
            poly.getVertices().add(new Vertex(-2.0, -2.0, SegmentType.MOVE));
            poly.getVertices().add(new Vertex(-2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, -2.0, SegmentType.LINE));
            poly.getVertices().add(Vertex.CLOSE);
            
            Assert.assertEquals("area check", 16.0, poly.getArea(), 0.02);
            Assert.assertTrue("range-reduce polygon on meridian ccw", poly.getCCW());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPolygonWindingMeridianCW()
    {
        try
        {
            MultiPolygon poly = new MultiPolygon();
            poly.getVertices().add(new Vertex(358.0, -2.0, SegmentType.MOVE));
            poly.getVertices().add(new Vertex(2.0, -2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(358.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(Vertex.CLOSE);
            
            Assert.assertEquals("area check", 16.0, poly.getArea(), 0.02);
            Assert.assertFalse("polygon on meridian cw", poly.getCCW());
            
            poly.getVertices().clear(); // check range reduction
            poly.getVertices().add(new Vertex(-2.0, -2.0, SegmentType.MOVE));
            poly.getVertices().add(new Vertex(2.0, -2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(new Vertex(-2.0, 2.0, SegmentType.LINE));
            poly.getVertices().add(Vertex.CLOSE);
            
            Assert.assertEquals("area check", 16.0, poly.getArea(), 0.02);
            Assert.assertFalse("range-reduce polygon on meridian cw", poly.getCCW());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
