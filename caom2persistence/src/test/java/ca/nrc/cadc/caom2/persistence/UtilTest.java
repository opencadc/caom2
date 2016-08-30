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

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class UtilTest 
{
    private static final Logger log = Logger.getLogger(UtilTest.class);
    
    @Test
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
    
    // TODO: this is identical to code in CaomUtilTest (module caom2)
    static void testEqual(String s, CoordRange1D expected, CoordRange1D actual)
    {
        Assert.assertTrue(s+".start.pix", expected.getStart().pix == actual.getStart().pix);
        Assert.assertTrue(s+".start.val", expected.getStart().val == actual.getStart().val);
        Assert.assertTrue(s+".end.pix", expected.getEnd().pix == actual.getEnd().pix);
        Assert.assertTrue(s+".end.val", expected.getEnd().val == actual.getEnd().val);
    }
    static void testEqual(String s, CoordBounds1D expected, CoordBounds1D actual)
    {
        Assert.assertEquals("bounds.samples.size", expected.getSamples().size(), actual.getSamples().size());
        Iterator<CoordRange1D> ex = expected.getSamples().iterator();
        Iterator<CoordRange1D> ac = actual.getSamples().iterator();
        while ( ex.hasNext() )
            testEqual(s+".sample", ex.next(), ac.next());
    }
    static void testEqual(String s, CoordFunction1D expected, CoordFunction1D actual)
    {
        Assert.assertEquals(s+".function.naxis", expected.getNaxis(), actual.getNaxis());
        Assert.assertEquals(s+".function.delta", expected.getDelta(), actual.getDelta());
        Assert.assertTrue(s+".function.refCoord.pix", expected.getRefCoord().pix == actual.getRefCoord().pix);
        Assert.assertTrue(s+".function.refCoord.val", expected.getRefCoord().val == actual.getRefCoord().val);
    }
    static void testEqual(CoordRange2D expected, CoordRange2D actual)
    {
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord1().pix == actual.getStart().getCoord1().pix);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord1().val == actual.getStart().getCoord1().val);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord2().pix == actual.getStart().getCoord2().pix);
        Assert.assertTrue("CoordRange2D", expected.getStart().getCoord2().val == actual.getStart().getCoord2().val);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord1().pix == actual.getEnd().getCoord1().pix);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord1().val == actual.getEnd().getCoord1().val);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord2().pix == actual.getEnd().getCoord2().pix);
        Assert.assertTrue("CoordRange2D", expected.getEnd().getCoord2().val == actual.getEnd().getCoord2().val);
    }
    static void testEqual(CoordBounds2D expected, CoordBounds2D actual)
    {
        Assert.assertEquals("bounds class", expected.getClass(), actual.getClass());
        if (expected instanceof CoordCircle2D)
        {
            CoordCircle2D ex = (CoordCircle2D) expected;
            CoordCircle2D ac = (CoordCircle2D) actual;
            Assert.assertTrue("bounds.center.coord1", ex.getCenter().coord1 == ac.getCenter().coord1);
            Assert.assertTrue("bounds.center.coord2",  ex.getCenter().coord2 == ac.getCenter().coord2);
            Assert.assertTrue("bounds.center.radius", ex.getRadius().equals(ac.getRadius()));
        }
        else
        {
            CoordPolygon2D ex = (CoordPolygon2D) expected;
            CoordPolygon2D ac = (CoordPolygon2D) actual;
            Assert.assertEquals("bounds.vertices.size", ex.getVertices().size(), ac.getVertices().size());
            Iterator<ValueCoord2D> exi = ex.getVertices().iterator();
            Iterator<ValueCoord2D> aci = ac.getVertices().iterator();
            while ( exi.hasNext() )
            {
                ValueCoord2D e = exi.next();
                ValueCoord2D a = aci.next();
                Assert.assertTrue(e.coord1 == a.coord1);
                Assert.assertTrue(e.coord2 == a.coord2);
            }

        }
    }
    static void testEqual(CoordFunction2D expected, CoordFunction2D actual)
    {
        Assert.assertEquals("function.dimension.naxis1", expected.getDimension().naxis1, actual.getDimension().naxis1);
        Assert.assertEquals("function.dimension.naxis2", expected.getDimension().naxis2, actual.getDimension().naxis2);
        Assert.assertTrue("function.refCoord1.pix", expected.getRefCoord().getCoord1().pix
                == actual.getRefCoord().getCoord1().pix);
        Assert.assertTrue("function.refCoord1.val", expected.getRefCoord().getCoord1().val
                == actual.getRefCoord().getCoord1().val);
        Assert.assertTrue("function.refCoord2.pix", expected.getRefCoord().getCoord2().pix
                == actual.getRefCoord().getCoord2().pix);
        Assert.assertTrue("function.refCoord2.val", expected.getRefCoord().getCoord2().val
                == actual.getRefCoord().getCoord2().val);
        Assert.assertTrue("cd11", expected.getCd11() == actual.getCd11());
        Assert.assertTrue("cd12", expected.getCd12() == actual.getCd12());
        Assert.assertTrue("cd21", expected.getCd21() == actual.getCd21());
        Assert.assertTrue("cd22", expected.getCd22() == actual.getCd22());
    }
}
