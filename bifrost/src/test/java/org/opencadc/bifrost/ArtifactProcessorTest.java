/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package org.opencadc.bifrost;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.datalink.DataLink;

/**
 *
 * @author pdowler
 */
public class ArtifactProcessorTest {

    private static final Logger log = Logger.getLogger(ArtifactProcessorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.bifrost", Level.DEBUG);
    }

    static PublisherID PUB_ID = new PublisherID(URI.create("ivo://opencadc.org/BAR?bar/baz"));
    static String BASE_ARTIFACT_URI = "foo:BAR/bar_baz_";

    public ArtifactProcessorTest() {
    }

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNoArtifacts() {
        log.debug("testEmptyList START");
        try {
            URI uri = PUB_ID.getURI();
            ArtifactProcessor ap = new ArtifactProcessor(URI.create("ivo://unused/locator"), new ArrayList<URI>());

            ArtifactQueryResult artifacts = new ArtifactQueryResult(PUB_ID);
            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            Assert.assertTrue(links.isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSimple() {
        try {
            URI uri = PUB_ID.getURI();

            ArtifactQueryResult artifacts = new ArtifactQueryResult(PUB_ID);
            artifacts.getArtifacts().addAll(getTestArtifacts(1, 0));
            Assert.assertEquals("test setup", 1, artifacts.getArtifacts().size());

            ArtifactProcessor ap = new ArtifactProcessor(URI.create("ivo://unused/locator"), new ArrayList<URI>());

            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            Assert.assertEquals("num links", 1, links.size());

            for (DataLink dl : links) {
                log.info("testSimple link: " + dl);
                Assert.assertNotNull(dl);
                Assert.assertEquals(uri.toASCIIString(), dl.getID());
                Assert.assertNotNull(dl.errorMessage);
                //Assert.assertNotNull(dl.accessURL);
                //String query = dl.accessURL.getQuery();
                //Assert.assertNull(query); // no runid
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    //@Test
    public void testPackageLink() {
        log.debug("testPackageLink START");
        try {
            URI uri = PUB_ID.getURI();

            ArtifactQueryResult artifacts = new ArtifactQueryResult(PUB_ID);
            artifacts.getArtifacts().addAll(getTestArtifacts(3, 2));
            Assert.assertEquals("test setup", 5, artifacts.getArtifacts().size());

            ArtifactProcessor ap = new ArtifactProcessor(URI.create("ivo://unused/locator"), new ArrayList<URI>());

            List<DataLink> links = ap.process(uri, artifacts);
            Assert.assertNotNull(links);
            //Assert.assertEquals("num links", 6, links.size());

            boolean foundPkg = false;
            for (DataLink dl : links) {
                log.info("testPackageLink: " + dl);
                Assert.assertNotNull(dl);
                Assert.assertEquals(uri.toASCIIString(), dl.getID());
                Assert.assertNotNull(dl.accessURL);
                String query = dl.accessURL.getQuery();
                if (dl.getSemantics().equals(DataLink.Term.PACKAGE)) {
                    Assert.assertNotNull(query);
                    foundPkg = true;
                }
            }
            Assert.assertTrue("found package link", foundPkg);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private List<Artifact> getTestArtifacts(int numA, int numP)
            throws Exception {
        List<Artifact> ret = new ArrayList<>();
        for (int i = 0; i < numA; i++) {
            Artifact a = new Artifact(new URI(BASE_ARTIFACT_URI + i), ProductType.SCIENCE, ReleaseType.DATA);
            ret.add(a);
        }

        if (numP > 0) {
            ret.add(new Artifact(new URI(BASE_ARTIFACT_URI + numA + 1), ProductType.PREVIEW, ReleaseType.DATA));
        }
        if (numP == 2) {
            ret.add(new Artifact(new URI(BASE_ARTIFACT_URI + numA + 2), ProductType.THUMBNAIL, ReleaseType.DATA));
        }

        return ret;
    }
}
