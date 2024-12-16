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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author yeunga
 */
public class AdCutoutGeneratorTest {
    private static final Logger log = Logger.getLogger(AdCutoutGeneratorTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.INFO);
    }

    private static final String CUTOUT1 = "[1][100:200, 100:200]";
    private static final String CUTOUT2 = "[2][300:400, 300:400]";
    private static final String CUTOUT3 = "[3][500:600, 500:600]";
    private static final String CUTOUT4 = "[4][700:800, 700:800]";
    private static final String CUTOUT_ALPHA = "[AMP_4_0,3][5:20, 20:30]";

    private static final String FILE_URI = "ad:FOO/bar";
    private static final String FILE_URI_COMPRESSED = "ad:FOO/bar.FZ";
    private static final String FILE_URI_COMPRESSED_FITS = "ad:FOO/bar.fits.FZ";

    AdCutoutGenerator adResolver = new AdCutoutGenerator();

    public AdCutoutGeneratorTest() {

    }

    @Test
    public void testToURLWithNullLabel() {
        try {
        	String label = null;
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT3);
            cutouts.add(CUTOUT4);
            URI uri = new URI(FILE_URI);
            adResolver.setAuthMethod(AuthMethod.ANON);
            URL url = adResolver.toURL(uri, cutouts, label);
            Assert.assertNotNull(url);
            log.info("testFile: " + uri + " -> " + url);
            String urlString = url.toExternalForm();
            String msg = "url should not contain fo parameter: " + urlString;
            Assert.assertFalse(msg, urlString.contains("fo="));
            String[] cutoutArray = NetUtil.decode(url.getQuery()).split("&");
            Assert.assertEquals(CUTOUT1, cutoutArray[0].split("=")[1]);
            Assert.assertEquals(CUTOUT2, cutoutArray[1].split("=")[1]);
            Assert.assertEquals(CUTOUT3, cutoutArray[2].split("=")[1]);
            Assert.assertEquals(CUTOUT4, cutoutArray[3].split("=")[1]);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testToURLWithInvalidLabel() {
        try {
            String label = "label1%";
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT3);
            cutouts.add(CUTOUT4);
            URI uri = new URI(FILE_URI);
            adResolver.setAuthMethod(AuthMethod.ANON);
            URL url = adResolver.toURL(uri, cutouts, label);
            Assert.fail("should have thrown a UsageFault due to an invalid label");
        } catch (UsageFault uf) {
            // expected, success
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testReplaceNonAlphaNumeric() throws Exception {
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            String replacedString = AdCutoutGenerator.replaceNonAlphanumeric(cutouts);
            Assert.assertEquals("replacement failed", "1__100_200__100_200", replacedString);

            cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT_ALPHA);
            replacedString = AdCutoutGenerator.replaceNonAlphanumeric(cutouts);
            Assert.assertEquals("replacement failed", "1__100_200__100_200___2__300_400__300_400___AMP_4_0_3__5_20__20_30", replacedString);
    }
    
    @Test
    public void testToURLWithUncompressedFilename() {
        try {
        	String label = "label1";
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT3);
            cutouts.add(CUTOUT4);
            URI uri = new URI(FILE_URI);
            adResolver.setAuthMethod(AuthMethod.ANON);
            URL url = adResolver.toURL(uri, cutouts, label);
            Assert.assertNotNull(url);
            log.info("testFile: " + uri + " -> " + url);
            String urlString = url.toExternalForm();
            String msg = "url should contain fo parameter: " + urlString;
            Assert.assertTrue(msg, urlString.contains("fo=bar__label1__1__100_200__100_200___2__300_400__300_400___3__500_600__500_600___4__700_800__700_800"));
            String[] cutoutArray = NetUtil.decode(url.getQuery()).split("&");
            Assert.assertEquals(CUTOUT1, cutoutArray[1].split("=")[1]);
            Assert.assertEquals(CUTOUT2, cutoutArray[2].split("=")[1]);
            Assert.assertEquals(CUTOUT3, cutoutArray[3].split("=")[1]);
            Assert.assertEquals(CUTOUT4, cutoutArray[4].split("=")[1]);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testToURLWithCompressedFilename() {
        try {
        	String label = "label2";
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT3);
            cutouts.add(CUTOUT4);
            URI uri = new URI(FILE_URI_COMPRESSED);
            adResolver.setAuthMethod(AuthMethod.ANON);
            URL url = adResolver.toURL(uri, cutouts, label);
            Assert.assertNotNull(url);
            log.info("testFile: " + uri + " -> " + url);
            String urlString = url.toExternalForm();
            String msg = "url should contain fo parameter without compression extension: " + urlString;
            Assert.assertTrue(msg, urlString.contains("fo=bar__label2__1__100_200__100_200___2__300_400__300_400___3__500_600__500_600___4__700_800__700_800"));
            String[] cutoutArray = NetUtil.decode(url.getQuery()).split("&");
            Assert.assertEquals(CUTOUT1, cutoutArray[1].split("=")[1]);
            Assert.assertEquals(CUTOUT2, cutoutArray[2].split("=")[1]);
            Assert.assertEquals(CUTOUT3, cutoutArray[3].split("=")[1]);
            Assert.assertEquals(CUTOUT4, cutoutArray[4].split("=")[1]);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testToURLWithAlphaCompressedFitsFilename() {
        try {
        	String label = "label3";
            List<String> cutouts = new ArrayList<String>();
            cutouts.add(CUTOUT1);
            cutouts.add(CUTOUT2);
            cutouts.add(CUTOUT_ALPHA);
            URI uri = new URI(FILE_URI_COMPRESSED_FITS);
            adResolver.setAuthMethod(AuthMethod.ANON);
            URL url = adResolver.toURL(uri, cutouts, label);
            Assert.assertNotNull(url);
            log.info("testFile: " + uri + " -> " + url);
            String urlString = url.toExternalForm();
            String msg = "url should contain fo parameter without compression extension: " + urlString;
            Assert.assertTrue(msg, urlString.contains("fo=bar__label3__1__100_200__100_200___2__300_400__300_400___AMP_4_0_3__5_20__20_30.fits"));
            String[] cutoutArray = NetUtil.decode(url.getQuery()).split("&");
            Assert.assertEquals(CUTOUT1, cutoutArray[1].split("=")[1]);
            Assert.assertEquals(CUTOUT2, cutoutArray[2].split("=")[1]);
            Assert.assertEquals(CUTOUT_ALPHA, cutoutArray[3].split("=")[1]);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
