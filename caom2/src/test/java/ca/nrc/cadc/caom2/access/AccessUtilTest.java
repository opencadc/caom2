/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package ca.nrc.cadc.caom2.access;


import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.vocab.DataLinkSemantics;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AccessUtilTest {
    private static final Logger log = Logger.getLogger(AccessUtilTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }
    
    public AccessUtilTest() { 
    }
    
    @Test
    public void testGetReleaseDate() {
        try {
            Artifact data = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.THIS, ReleaseType.DATA);
            Artifact meta = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.PREVIEW, ReleaseType.META);
            
            final Date pastMetaRelease = new Date(System.currentTimeMillis() - 1800L);
            final Date futureMetaRelease = new Date(System.currentTimeMillis() + 1800L);
            
            final Date pastDataRelease = new Date(System.currentTimeMillis() - 3600L);
            final Date futureDataRelease = new Date(System.currentTimeMillis() + 3600L);
            
            final Date contentRelease = new Date(System.currentTimeMillis() + 600L);
            
            Date actual;
            
            actual = AccessUtil.getReleaseDate(data, pastMetaRelease, pastDataRelease);
            Assert.assertEquals(pastDataRelease, actual);
            
            actual = AccessUtil.getReleaseDate(meta, pastMetaRelease, pastDataRelease);
            Assert.assertEquals(pastMetaRelease, actual);
            
            actual = AccessUtil.getReleaseDate(data, futureMetaRelease, futureDataRelease);
            Assert.assertEquals(futureDataRelease, actual);
            
            actual = AccessUtil.getReleaseDate(meta, futureMetaRelease, futureMetaRelease);
            Assert.assertEquals(futureMetaRelease, actual);
            
            // contentRelease override
            data.contentRelease = contentRelease;
            actual = AccessUtil.getReleaseDate(data, pastMetaRelease, pastDataRelease);
            Assert.assertEquals(contentRelease, actual);
            
            actual = AccessUtil.getReleaseDate(data, null, null);
            Assert.assertEquals(contentRelease, actual);
            data.contentRelease = null;
            
            // null handling
            actual = AccessUtil.getReleaseDate(data, null, futureDataRelease);
            Assert.assertEquals(futureDataRelease, actual);
            
            actual = AccessUtil.getReleaseDate(meta, futureMetaRelease, null);
            Assert.assertEquals(futureMetaRelease, actual);
            
            // null handling and return
            actual = AccessUtil.getReleaseDate(data, futureMetaRelease, null);
            Assert.assertNull(actual);
            
            actual = AccessUtil.getReleaseDate(meta, null, futureDataRelease);
            Assert.assertNull(actual);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testGetArtifactAccess() {
        try {
            Artifact data = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.THIS, ReleaseType.DATA);
            Artifact meta = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.PREVIEW, ReleaseType.META);
            
            Date pastMetaRelease = new Date(System.currentTimeMillis() - 1800L);
            Date futureMetaRelease = new Date(System.currentTimeMillis() + 1800L);
            
            Date pastDataRelease = new Date(System.currentTimeMillis() - 3600L);
            Date futureDataRelease = new Date(System.currentTimeMillis() + 3600L);
            
            Set<URI> dataReadGroups = new TreeSet<URI>();
            dataReadGroups.add(URI.create("ivo://cadc.nrc.ca/gms?DATA-READ"));
            Set<URI> metaReadGroups = new TreeSet<URI>();
            metaReadGroups.add(URI.create("ivo://cadc.nrc.ca/gms?META-READ"));
            ArtifactAccess actual;
            
            actual = AccessUtil.getArtifactAccess(data, pastMetaRelease, metaReadGroups, pastDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(pastDataRelease, actual.releaseDate);
            Assert.assertTrue(actual.isPublic);
            
            actual = AccessUtil.getArtifactAccess(meta, pastMetaRelease, metaReadGroups, pastDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(pastMetaRelease, actual.releaseDate);
            Assert.assertTrue(actual.isPublic);
            
            actual = AccessUtil.getArtifactAccess(data, futureMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(futureDataRelease, actual.releaseDate);
            Assert.assertFalse(actual.isPublic);
            
            actual = AccessUtil.getArtifactAccess(meta, futureMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(futureMetaRelease, actual.releaseDate);
            Assert.assertFalse(actual.isPublic);
            
            // mixed
            actual = AccessUtil.getArtifactAccess(data, pastMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(futureDataRelease, actual.releaseDate);
            Assert.assertFalse(actual.isPublic);
            assertListEquals(dataReadGroups, actual.getReadGroups());
            
            actual = AccessUtil.getArtifactAccess(meta, pastMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(pastMetaRelease, actual.releaseDate);
            Assert.assertTrue(actual.isPublic);
            assertListEquals(metaReadGroups, actual.getReadGroups());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testArtifactAccessOverride() {
        try {
            Artifact data = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.THIS, ReleaseType.DATA);
            Artifact meta = new Artifact(URI.create("foo:bar/baz"), DataLinkSemantics.PREVIEW, ReleaseType.META);
            
            Date pastMetaRelease = new Date(System.currentTimeMillis() - 2000L);
            Date futureMetaRelease = new Date(System.currentTimeMillis() + 2000L);
            
            Date pastDataRelease = new Date(System.currentTimeMillis() - 4000L);
            Date futureDataRelease = new Date(System.currentTimeMillis() + 4000L);
            
            Date pastArtifactRelease = new Date(System.currentTimeMillis() - 3000L);
            Date futureArtifactRelease = new Date(System.currentTimeMillis() + 3000L);
            data.getContentReadGroups().add(URI.create("ivo://cadc.nrc.ca/gms?DATA-OVERRIDE"));
            meta.getContentReadGroups().add(URI.create("ivo://cadc.nrc.ca/gms?META-OVERRIDE"));
            
            Set<URI> dataReadGroups = new TreeSet<URI>();
            dataReadGroups.add(URI.create("ivo://cadc.nrc.ca/gms?DATA-READ"));
            Set<URI> metaReadGroups = new TreeSet<URI>();
            metaReadGroups.add(URI.create("ivo://cadc.nrc.ca/gms?META-READ"));
            ArtifactAccess actual;
            
            data.contentRelease = pastArtifactRelease;
            actual = AccessUtil.getArtifactAccess(data, pastMetaRelease, metaReadGroups, pastDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(pastArtifactRelease, actual.releaseDate);
            Assert.assertTrue(actual.isPublic);
            
            meta.contentRelease = pastArtifactRelease;
            actual = AccessUtil.getArtifactAccess(meta, pastMetaRelease, metaReadGroups, pastDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(pastArtifactRelease, actual.releaseDate);
            Assert.assertTrue(actual.isPublic);
            
            data.contentRelease = futureArtifactRelease;
            actual = AccessUtil.getArtifactAccess(data, futureMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(futureArtifactRelease, actual.releaseDate);
            Assert.assertFalse(actual.isPublic);
            
            meta.contentRelease = futureArtifactRelease;
            actual = AccessUtil.getArtifactAccess(meta, futureMetaRelease, metaReadGroups, futureDataRelease, dataReadGroups);
            Assert.assertNotNull(actual);
            Assert.assertEquals(futureArtifactRelease, actual.releaseDate);
            Assert.assertFalse(actual.isPublic);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private void assertListEquals(Set<URI> expected, Set<URI> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        Iterator<URI> ei = expected.iterator();
        Iterator<URI> ai = actual.iterator();
        while (ei.hasNext()) {
            URI e = ei.next();
            URI a = ai.next();
            log.info("assertListEquals: " + e + " vs " + a);
            Assert.assertEquals(e, a);
        }
    }
}
