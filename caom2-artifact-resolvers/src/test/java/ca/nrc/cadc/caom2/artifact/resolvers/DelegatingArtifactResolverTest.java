/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.util.Log4jInit;
import com.google.common.io.Files;
import java.io.File;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class DelegatingArtifactResolverTest {
    private static final Logger log = Logger.getLogger(DelegatingArtifactResolverTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.artifact.resolvers", Level.INFO);
    }
    
    public DelegatingArtifactResolverTest() { 
    }
    
    @Test
    public void testConfigNotFound() {
        String home = System.getProperty("user.home");
        log.info("testDelegate: orig home: " + home);
        String testHome = "build/tmp";
        File testConfig = new File(testHome + "/config");
        testConfig.mkdirs();
        // don't copy file
        
        try {
            System.setProperty("user.home", testHome);
            DelegatingArtifactResolver r = new DelegatingArtifactResolver();
            Assert.fail("expected IllegalStateException, got: " + r);
        } catch (IllegalStateException expected) {
            log.info("caught expected: " + expected);
        } finally {
            System.setProperty("user.home", home);
            log.info("testConfigNotFound: restore home: " + home);
        }
    }
    
    @Test
    public void testDelegate() throws Exception {
        String home = System.getProperty("user.home");
        log.info("testDelegate: orig home: " + home);
        String testHome = "build/tmp";
        File testConfig = new File(testHome + "/config");
        testConfig.mkdirs();
        testConfig = new File(testConfig, "caom2-artifact-resolvers.properties");
        Files.copy(new File("build/resources/test/caom2-artifact-resolvers.properties"), testConfig);
        
        try {
            System.setProperty("user.home", testHome);
            StorageResolver r = new DelegatingArtifactResolver();
            
            URI uri1 = URI.create("cadc:TEST/foo");
            URI uri2 = URI.create("mast:HST/product/bar");
            
            URL u1 = r.toURL(uri1);
            log.info(uri1 + " -> " + u1);
            Assert.assertNotNull(u1);
            
            URL u2 = r.toURL(uri2);
            log.info(uri2 + " -> " + u2);
            Assert.assertNotNull(u2);
            
        } finally {
            System.setProperty("user.home", home);
            log.info("testDelegate: restore home: " + home);
            testConfig.delete();
        }
    }
    
    @Test
    public void testSchemeNotSupported() throws Exception {
        String home = System.getProperty("user.home");
        log.info("testSchemeNotSupported: orig home: " + home);
        String testHome = "build/tmp";
        File testConfig = new File(testHome + "/config");
        testConfig.mkdirs();
        testConfig = new File(testConfig, "caom2-artifact-resolvers.properties");
        Files.copy(new File("build/resources/test/caom2-artifact-resolvers.properties"), testConfig);
        
        try {
            System.setProperty("user.home", testHome);
            StorageResolver r = new DelegatingArtifactResolver();
            
            URI uri1 = URI.create("unknown:TEST/foo");
            try {
                URL u1 = r.toURL(uri1);
                Assert.fail("expected IllegalArgumentException, got: " + uri1 + " -> " + u1);
            } catch (IllegalArgumentException expected) {
                log.info("caught expected: " + expected);
            }
            
        } finally {
            System.setProperty("user.home", home);
            log.info("testSchemeNotSupported: restore home: " + home);
            testConfig.delete();
        }
    }
}
