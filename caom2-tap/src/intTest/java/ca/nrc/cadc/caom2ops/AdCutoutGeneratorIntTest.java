/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author adriand
 */
public class AdCutoutGeneratorIntTest {
    private static final Logger log = Logger.getLogger(AdCutoutGeneratorIntTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.INFO);
    }

    final String FILE_URI = "ad:CFHT/806045o.fits.fz";
    private static final String CUTOUT1 = "[9][100:200,100:200]";
    private static final String CUTOUT2 = "[11][100:200,100:200]";

    	private static final String INVALID_CUTOUT = "[100][100:200]";
    AdCutoutGenerator resolver = new AdCutoutGenerator();

    public AdCutoutGeneratorIntTest() {
    }

    //@Test
    public void testValidCutoutUrlWithNoLabel() throws Exception {
        log.info("starting testValidCutoutUrl");
        try {
        	String label = null;
            List<URL> urlList = new ArrayList<URL>();
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            URL url = resolver.toURL(new URI(FILE_URI), cutouts, label);
            
            urlList.add(url);
            for (URL u : urlList) {
                log.debug("opening connection to: " + u.toString());
                String urlString = url.toExternalForm();
                String msg = "url should not contain fo parameter: " + urlString;
                Assert.assertFalse(msg, urlString.contains("fo="));
                OutputStream out = new ByteArrayOutputStream();
                HttpDownload head = new HttpDownload(u, out);
                head.setHeadOnly(true); // head requests work with data ws
                head.run();
                Assert.assertEquals(200, head.getResponseCode());
                log.info("response code: " + head.getResponseCode());
            }

        } catch (Exception unexpected) {
            log.error("Unexpected exception", unexpected);
            Assert.fail("Unexpected exception: " + unexpected);
        }
    }

    //@Test
    public void testValidCutoutUrlWithLabel() throws Exception {
        log.info("starting testValidCutoutUrl");
        try {
        	String label = "label1";
        	String expected_filename = "806045o" + "__" + label + "__" + "9__100_200_100_200___11__100_200_100_200.fits";
            List<URL> urlList = new ArrayList<URL>();
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            URL url = resolver.toURL(new URI(FILE_URI), cutouts, label);
            
            urlList.add(url);
            for (URL u : urlList) {
                log.debug("opening connection to: " + u.toString());
                String urlString = url.toExternalForm();
                String msg = "url should contain fo parameter without compression extension: " + urlString;
                Assert.assertTrue(msg, urlString.contains("fo=" + expected_filename));
                OutputStream out = new ByteArrayOutputStream();
                HttpDownload head = new HttpDownload(u, out);
                head.setHeadOnly(true); // head requests work with data ws
                head.run();
                Assert.assertEquals(200, head.getResponseCode());
                log.info("response code: " + head.getResponseCode());
                String cdis = head.getResponseHeader("Content-Disposition");
                String actual_filename = cdis.split("=")[1];
                Assert.assertEquals("incorrect filename", expected_filename, actual_filename);
            }

        } catch (Exception unexpected) {
            log.error("Unexpected exception", unexpected);
            Assert.fail("Unexpected exception: " + unexpected);
        }
    }
    
    //@Test
    public void testInvalidCutoutUrl() throws Exception {
        log.info("starting testValidCutoutUrl");
        try {
        	String label = null;
            List<URL> urlList = new ArrayList<URL>();
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(INVALID_CUTOUT);
            URL url = resolver.toURL(new URI(FILE_URI), cutouts, label);
            
            urlList.add(url);
            for (URL u : urlList) {
                log.debug("opening connection to: " + u.toString());
                OutputStream out = new ByteArrayOutputStream();
                HttpDownload download = new HttpDownload(u, out);
                download.setHeadOnly(false); // need download request to check cutout
                download.run();
                Assert.assertEquals(400, download.getResponseCode());
                log.info("response code: " + download.getResponseCode());
            }

        } catch (Exception unexpected) {
            log.error("Unexpected exception", unexpected);
            Assert.fail("Unexpected exception: " + unexpected);
        }
    }
}
