/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
************************************************************************
 */

package org.opencadc.caom2.xml;

import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.access.ArtifactAccess;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ArtifactAccessReaderWriterTest {

    private static final Logger log = Logger.getLogger(ArtifactAccessReaderWriterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.xml", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.xml", Level.INFO);
    }

    public ArtifactAccessReaderWriterTest() {
    }

    @Test
    public void testMinimal() {
        try {
            Artifact a = new Artifact(URI.create("foo:BAR/baz"), DataLinkSemantics.THIS, ReleaseType.DATA);
            final ArtifactAccess expected = new ArtifactAccess(a);
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ArtifactAccessWriter aw = new ArtifactAccessWriter();
            aw.write(expected, bos);
            
            String xml = bos.toString();
            log.info("xml:\n" + xml);
            
            ArtifactAccessReader ar = new ArtifactAccessReader();
            ArtifactAccess actual = ar.read(xml);
            
            Assert.assertEquals(expected.getArtifact().getURI(), actual.getArtifact().getURI());
            Assert.assertEquals(expected.getArtifact().getProductType(), actual.getArtifact().getProductType());
            Assert.assertEquals(expected.getArtifact().getReleaseType(), actual.getArtifact().getReleaseType());
            Assert.assertEquals(expected.getArtifact().contentChecksum, actual.getArtifact().contentChecksum);
            Assert.assertEquals(expected.getArtifact().contentLength, actual.getArtifact().contentLength);
            Assert.assertEquals(expected.getArtifact().contentType, actual.getArtifact().contentType);
            
            Assert.assertEquals(expected.releaseDate, actual.releaseDate);
            Assert.assertEquals(expected.isPublic, actual.isPublic);
            Assert.assertTrue(actual.getReadGroups().isEmpty());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testRoundTrip() {
        try {
            Artifact a = new Artifact(URI.create("foo:BAR/baz"), DataLinkSemantics.THIS, ReleaseType.DATA);
            final ArtifactAccess expected = new ArtifactAccess(a);
            
            a.contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e");
            a.contentLength = 0L;
            a.contentType = "text/plain";
            expected.isPublic = true;
            expected.getReadGroups().add(URI.create("ivo://example.net/aa?group1"));
            expected.getReadGroups().add(URI.create("ivo://example.net/aa?group2"));
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ArtifactAccessWriter aw = new ArtifactAccessWriter();
            aw.write(expected, bos);
            
            String xml = bos.toString();
            log.info("xml:\n" + xml);
            
            ArtifactAccessReader ar = new ArtifactAccessReader();
            ArtifactAccess actual = ar.read(xml);
            
            Assert.assertEquals(expected.getArtifact().getURI(), actual.getArtifact().getURI());
            Assert.assertEquals(expected.getArtifact().getProductType(), actual.getArtifact().getProductType());
            Assert.assertEquals(expected.getArtifact().getReleaseType(), actual.getArtifact().getReleaseType());
            Assert.assertEquals(expected.getArtifact().contentChecksum, actual.getArtifact().contentChecksum);
            Assert.assertEquals(expected.getArtifact().contentLength, actual.getArtifact().contentLength);
            Assert.assertEquals(expected.getArtifact().contentType, actual.getArtifact().contentType);
            
            Assert.assertEquals(expected.releaseDate, actual.releaseDate);
            Assert.assertEquals(expected.isPublic, actual.isPublic);
            Assert.assertEquals(expected.getReadGroups().size(), actual.getReadGroups().size());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
