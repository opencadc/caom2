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

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.FileWriter;
import java.net.URI;
import java.security.MessageDigest;
import java.text.DateFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CaomTapQueryTest 
{
    private static final Logger log = Logger.getLogger(CaomTapQueryTest.class);
    
    private static final URI OBS_URI = URI.create("caom:IRIS/f212h000");
    private static final URI PLANE_URI = URI.create("caom:IRIS/f212h000/IRAS-60um");
    private static final URI PUB_ID = URI.create("ivo://cadc.nrc.ca/IRIS?f212h000/IRAS-60um");
    
    private static final String TAP_URI = "ivo://cadc.nrc.ca/argus";
    private final DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.DEBUG);
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
            String runID = "testJobID";
	    URI tapURI = new URI(TAP_URI);
            
            // minimal args
            CaomTapQuery query = new CaomTapQuery(tapURI, null);
            
            // all args
	    query = new CaomTapQuery(tapURI, runID);

            try
            {
                // missing required arg
                query = new CaomTapQuery(null, "foo");
                Assert.fail("expected IllegalArgumentException for tapURL=null");
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
    public void testObservationQuery()
    {
        try
        {
            ObservationURI uri = new ObservationURI(OBS_URI);
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            Observation o = query.performQuery(uri);
            Assert.assertNotNull(o);
            Assert.assertEquals(uri.getCollection(), o.getCollection());
            Assert.assertEquals(uri.getObservationID(), o.getObservationID());
            
            ObservationWriter w = new ObservationWriter();
            w.write(o, new FileWriter("CaomTapQueryTest.xml"));
            
            // this is needed to diagnose which plane has checksum mismatch
            //for (Plane p : o.getPlanes())
            //{
            //    URI metaChecksum = p.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            //    Assert.assertEquals("Plane.metaChecksum", p.getMetaChecksum(), metaChecksum);
            //    
            //    URI accMetaChecksum = p.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            //    Assert.assertEquals("Plane.accMetaChecksum", p.getAccMetaChecksum(), accMetaChecksum);
            //}
            URI accMetaChecksum = o.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("Observation.accMetaChecksum", o.getAccMetaChecksum(), accMetaChecksum);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPlaneURIShallow()
    {
        try
        {
            PlaneURI uri = new PlaneURI(PLANE_URI);
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            ArtifactQueryResult ar = query.performQuery(uri, true);
            Assert.assertNotNull(ar);
            log.info("found: " + ar.getPublisherID().getURI());
            Assert.assertNotNull(ar.getArtifacts());
            Assert.assertFalse(ar.getArtifacts().isEmpty());
            for (Artifact a : ar.getArtifacts())
            {
                Assert.assertTrue(a.getParts().isEmpty());
                URI metaChecksum = a.computeMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("Artifact.metaChecksum", a.getMetaChecksum(), metaChecksum);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPublisherIDShallow()
    {
        try
        {
            PublisherID uri = new PublisherID(PUB_ID);
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            ArtifactQueryResult ar = query.performQuery(uri, true);
            Assert.assertNotNull(ar);
            log.info("found: " + ar.getPublisherID().getURI());
            Assert.assertNotNull(ar.getArtifacts());
            Assert.assertFalse(ar.getArtifacts().isEmpty());
            for (Artifact a : ar.getArtifacts())
            {
                Assert.assertTrue(a.getParts().isEmpty());
                URI metaChecksum = a.computeMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("Artifact.metaChecksum", a.getMetaChecksum(), metaChecksum);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPlaneURIDeep()
    {
        try
        {
            PlaneURI uri = new PlaneURI(PLANE_URI);
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            ArtifactQueryResult ar = query.performQuery(uri, false);
            Assert.assertNotNull(ar);
            log.info("found: " + ar.getPublisherID().getURI());
            Assert.assertNotNull(ar.getArtifacts());
            Assert.assertFalse(ar.getArtifacts().isEmpty());
            for (Artifact a : ar.getArtifacts())
            {
                if (ProductType.SCIENCE.equals(a.getProductType()))
                    Assert.assertFalse(a.getParts().isEmpty());
                else // preview
                    Assert.assertTrue(a.getParts().isEmpty());
                
                URI accMetaChecksum = a.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("Artifact.accMetaChecksum", a.getAccMetaChecksum(), accMetaChecksum);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPublisherIDDeep()
    {
        try
        {
            PublisherID uri = new PublisherID(PUB_ID);
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            ArtifactQueryResult ar = query.performQuery(uri, false);
            Assert.assertNotNull(ar);
            log.info("found: " + ar.getPublisherID().getURI());
            Assert.assertNotNull(ar.getArtifacts());
            Assert.assertFalse(ar.getArtifacts().isEmpty());
            for (Artifact a : ar.getArtifacts())
            {
                if (ProductType.SCIENCE.equals(a.getProductType()))
                    Assert.assertFalse(a.getParts().isEmpty());
                else // preview
                    Assert.assertTrue(a.getParts().isEmpty());
                
                URI accMetaChecksum = a.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
                Assert.assertEquals("Artifact.accMetaChecksum", a.getAccMetaChecksum(), accMetaChecksum);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactQuery()
    {
        try
        {
            URI uri = URI.create("cadc:IRIS/I212B2H0.fits");
            CaomTapQuery query = new CaomTapQuery(URI.create(TAP_URI), "CaomTapQueryTest");
            Artifact a = query.performQuery(uri);
            Assert.assertNotNull(a);
            log.info("found: " + a.getURI());

            URI accMetaChecksum = a.computeAccMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals("Artifact.accMetaChecksum", a.getAccMetaChecksum(), accMetaChecksum);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
