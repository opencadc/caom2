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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ArtifactTest 
{
    private static final Logger log = Logger.getLogger(ArtifactTest.class);

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
            URI uri = new URI("ad", "Stuff/Thing/thing1", null);

            Artifact a = new Artifact(uri, ProductType.AUXILIARY, ReleaseType.DATA);
            log.debug("created: " + a);
            
            Assert.assertNotNull(a.getURI());
            Assert.assertEquals(uri, a.getURI());
            Assert.assertEquals(ProductType.AUXILIARY, a.getProductType());
            Assert.assertEquals(ReleaseType.DATA, a.getReleaseType());
            
            try
            {
                a = new Artifact(null, ProductType.AUXILIARY, ReleaseType.DATA);
                Assert.fail("expected IllegalArgumentException for uri=null, got: " + a);
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            
            try
            {
                a = new Artifact(uri, null, ReleaseType.DATA);
                Assert.fail("expected IllegalArgumentException for productType==null, got: " + a);
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            finally { }
            
            try
            {
                a = new Artifact(uri, ProductType.AUXILIARY, null);
                Assert.fail("expected IllegalArgumentException for releaseType=null, got: " + a);
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
            finally { }
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
            Artifact o1 = new Artifact(new URI("ad", "FOO/bar1", null), ProductType.AUXILIARY, ReleaseType.DATA);
            Artifact o2 = new Artifact(new URI("ad", "FOO/bar2", null), ProductType.AUXILIARY, ReleaseType.DATA);
            Artifact o2d = new Artifact(new URI("ad", "FOO/bar2", null), ProductType.AUXILIARY, ReleaseType.DATA);
            
            Assert.assertTrue(o1.equals(o1));
            
            Assert.assertFalse(o1.equals(null));
            
            Assert.assertFalse(o1.equals("foo"));
            
            Assert.assertFalse(o1.equals(o2));
            
            Assert.assertTrue(o2.equals(o2d));
            
            // test hashCode consistent
            Assert.assertTrue(o1.hashCode() == o1.hashCode());
            
            Assert.assertFalse(o1.hashCode() == o2.hashCode());
            
            Assert.assertTrue(o2.hashCode() == o2d.hashCode());
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testPartSet()
    {
        try
        {
            URI uri = new URI("ad", "Stuff/Thing/thing1", null);
            Artifact a = new Artifact(uri, ProductType.AUXILIARY, ReleaseType.DATA);
            Assert.assertEquals(0, a.getParts().size());

            Part p1 = new Part(new Integer(1));
            Part px = new Part("x");

            boolean added;
            
            // add a numbered part
            added = a.getParts().add(p1);
            Assert.assertTrue("p1", added);
            Assert.assertEquals(1, a.getParts().size());

            // add named part
            added =  a.getParts().add(px);
            Assert.assertTrue("p1 + px", added);
            Assert.assertEquals(2, a.getParts().size());

            // fail to add duplicate
            added =  a.getParts().add(new Part("x"));
            Assert.assertFalse("add duplicate", added);
            Assert.assertEquals(2, a.getParts().size());

            // clear
            a.getParts().clear();
            Assert.assertEquals(0, a.getParts().size());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
