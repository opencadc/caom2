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
public class CartesianTransformTest 
{
    private static final Logger log = Logger.getLogger(CartesianTransformTest.class);

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
    public void testTransformVertex()
    {
        try
        {
            CartesianTransform trans = new CartesianTransform();
            CartesianTransform inv = trans.getInverseTransform();
            Vertex v1, v2, v3;

            // check null transforms are no-ops
            v1 = new Vertex(2.0, 4.0, SegmentType.LINE);
            v2 = trans.transform(v1);
            Assert.assertEquals(v1.cval1, v2.cval1, 0.01);
            Assert.assertEquals(v1.cval2, v2.cval2, 0.01);
            Assert.assertEquals(v1.getType(), v2.getType());
            v3 = inv.transform(v2);
            Assert.assertEquals(v1.cval1, v3.cval1, 0.01);
            Assert.assertEquals(v1.cval2, v3.cval2, 0.01);
            Assert.assertEquals(v1.getType(), v3.getType());
            
            // check real transforms
            trans.axis = CartesianTransform.Z;
            for (double a=Math.PI/2.0; a <= Math.PI; a += Math.PI/2.0)
            {
                double increment = Math.toDegrees(a);
                trans.angle = a;
                inv = trans.getInverseTransform();
                
                v1 = new Vertex(2.0, 4.0, SegmentType.LINE);
                v2 = trans.transform(v1);
                Assert.assertEquals(v1.cval1 + increment, v2.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v2.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v2.getType());

                v3 = inv.transform(v2);
                Assert.assertEquals(v1.cval1, v3.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v3.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v3.getType());

                v1 = new Vertex(2.0, 4.0, SegmentType.MOVE);
                v2 = trans.transform(v1);
                Assert.assertEquals(v1.cval1 + increment, v2.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v2.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v2.getType());

                v3 = inv.transform(v2);
                Assert.assertEquals(v1.cval1, v3.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v3.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v3.getType());

                v1 = new Vertex(2.0, 4.0, SegmentType.CLOSE);
                v2 = trans.transform(v1);
                // values not relevant
                Assert.assertEquals(v1.getType(), v2.getType());
            }
            
            trans.axis = CartesianTransform.Y;
            for (double a=Math.PI/4.0; a <= Math.PI/2.0; a += Math.PI/4.0)
            {
                trans.angle = a;
                inv = trans.getInverseTransform();
                
                v1 = new Vertex(2.0, 4.0, SegmentType.LINE);
                v2 = trans.transform(v1);
                v3 = inv.transform(v2);
                Assert.assertEquals(v1.cval1, v3.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v3.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v3.getType());

                v1 = new Vertex(2.0, 4.0, SegmentType.MOVE);
                v2 = trans.transform(v1);
                v3 = inv.transform(v2);
                Assert.assertEquals(v1.cval1, v3.cval1, 0.01);
                Assert.assertEquals(v1.cval2, v3.cval2, 0.01);
                Assert.assertEquals(v1.getType(), v3.getType());

                v1 = new Vertex(2.0, 4.0, SegmentType.CLOSE);
                v2 = trans.transform(v1);
                // values not relevant
                Assert.assertEquals(v1.getType(), v2.getType());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testTransformPolygon()
    {
        try
        {
            CartesianTransform trans = new CartesianTransform();
            trans.angle = Math.PI/2.0; // +180
            trans.axis = CartesianTransform.Z;
            
            MultiPolygon p1 = new MultiPolygon();
            p1.getVertices().add(new Vertex(2.0, 2.0, SegmentType.LINE));
            p1.getVertices().add(new Vertex(4.0, 2.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(3.0, 3.0, SegmentType.MOVE));
            p1.getVertices().add(new Vertex(0.0, 0.0, SegmentType.CLOSE));
            
            MultiPolygon p2 = trans.transform(p1);
            MultiPolygon p3 = trans.getInverseTransform().transform(p2);
            
            Assert.assertEquals(p1.getVertices().size(), p3.getVertices().size());
            for (int i=0; i<p1.getVertices().size(); i++)
            {
                Vertex v1 = p1.getVertices().get(i);
                Vertex v3 = p3.getVertices().get(i);
                Assert.assertEquals("vertex longitude " + i, v1.cval1, v3.cval1, 0.01);
                Assert.assertEquals("vertex  latitude " + i, v1.cval2, v3.cval2, 0.01);
                Assert.assertEquals("vertex   segtype " + i, v1.getType(), v3.getType());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
