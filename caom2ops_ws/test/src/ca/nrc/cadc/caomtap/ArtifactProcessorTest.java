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

package ca.nrc.cadc.caomtap;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.datalink.DataLink;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ArtifactProcessorTest 
{
    private static final Logger log = Logger.getLogger(ArtifactProcessorTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    static String PLANE_URI = "caom:FOO/bar/baz";
    static String BASE_ARTIFACT_URI = "ad:FOO/bar_baz_";
    static String RUNID = "abc123";

    RegistryClient registryClient;

    public ArtifactProcessorTest()
    {
        this.registryClient = new RegistryClient();
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
    public void testNoArtifacts()
    {
        log.debug("testEmptyList START");
        try
        {
            URI uri = new URI(PLANE_URI);
            ArtifactProcessor ap = new ArtifactProcessor(RUNID, registryClient);

            List<Artifact> artifacts = new ArrayList<Artifact>();
            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            Assert.assertTrue(links.isEmpty());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testWithArtifacts()
    {
        log.debug("testNoFilter START");
        try
        {
            URI uri = new URI(PLANE_URI);
            
            List<Artifact> artifacts = getTestArtifacts(2, 1, 1);
            Assert.assertEquals("test setup", 2, artifacts.size());

            ArtifactProcessor ap = new ArtifactProcessor(RUNID, registryClient);
            
            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            Assert.assertEquals("num links", 2, links.size());

            for (DataLink dl : links)
            {
                log.debug("testNoFilter: " + dl);
                Assert.assertNotNull(dl);
                Assert.assertEquals(uri, dl.getURI());
                Assert.assertNotNull(dl.getURL());
                
                String query = dl.getURL().getQuery();
                Assert.assertNotNull("query string", query);
                String expected = "runid="+RUNID;
                String actual = query.toLowerCase();
                Assert.assertTrue("runid", actual.contains(expected));
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNoRunID()
    {
        log.debug("testRunID START");
        try
        {
            URI uri = new URI(PLANE_URI);

            List<Artifact> artifacts = getTestArtifacts(2, 1, 1);
            Assert.assertEquals("test setup", 2, artifacts.size());

            ArtifactProcessor ap = new ArtifactProcessor(null, registryClient);

            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            Assert.assertEquals("num links", 2, links.size());

            for (DataLink dl : links)
            {
                log.debug("testRunID: " + dl);
                Assert.assertNotNull(dl);
                Assert.assertEquals(uri, dl.getURI());
                Assert.assertNotNull(dl.getURL());
                String query = dl.getURL().getQuery();
                Assert.assertNull(query);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    

    private List<Artifact> getTestArtifacts(int num, int depth, int productTypeLevel)
        throws Exception
    {
        List<Artifact> ret = new ArrayList<Artifact>();
        for (int i=0; i<num; i++)
        {
            Artifact a = new Artifact(new URI(BASE_ARTIFACT_URI + i));
            if (productTypeLevel == 1)
                a.productType = ProductType.SCIENCE;
            ret.add(a);
            if (depth > 1)
            {
                for (int j=0; j<num; j++)
                {
                    Part p = new Part(new Integer(j));
                    if (productTypeLevel == 2)
                        p.productType = ProductType.SCIENCE;
                    a.getParts().add(p);
                }
            }
        }
        return ret;
    }
}
