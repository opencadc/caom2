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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hjeeves
 */
public class SubaruResolverTest {
    private static final Logger log = Logger.getLogger(SubaruResolverTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc", Level.DEBUG);
    }

    String VALID_SUP_FRAMEID = "SUPE01318470";
    String VALID_DATE1 = "2017-09-09";
    String VALID_SUP_FRAMEID2 = "SUPE01318470";
    String VALID_DATE2 = "2016-06-02";
    String VALID_HSC_FRAMEID = "HSCA069890XX";
    String EXPECTED_HSC_FILENAME = "HSCA069890.png";

    String RAW_DATA_URI = "raw";
    String PREVIEW_URI = "preview";

    String PROTOCOL_STR = "https://";

    String BASE_PREVIEW_URL = "smoka.nao.ac.jp";
    String PREVIEW_URL_QUERY = "grayscale=linear&mosaic=true&frameid=";
    String PREVIEW_URL_PATH = "/qlis/ImagePNG";
    String HSC_PREVIEW_URL_PATH = "/shot/HSC";

    String BASE_DATA_URL = "www.canfar.net";
    String DATA_URL_PATH = "/maq/subaru";
    String DATA_URL_QUERY= "frameinfo=";

    // Invalid checks the scheme and the request type (needs to be 'file' or 'preview')
    String INVALID_URI_BAD_SCHEME = "pokey:little/puppy";

    SubaruResolver subaruResolver = new SubaruResolver();

    public SubaruResolverTest() {
    }

    @Test
    public void testGetSchema() {
        Assert.assertEquals(subaruResolver.getScheme(), subaruResolver.getScheme());
    }

    @Test
    public void testValidRawURI() throws Exception {
        try {
            String uriStr = subaruResolver.getScheme() + ":" + RAW_DATA_URI + "/" + VALID_DATE1 + "/"
                            + VALID_SUP_FRAMEID;
            URI uri = new URI(uriStr);
            URL url = subaruResolver.toURL(uri);
            log.debug("toURL returned: " + url.toString());

            String frameQueryValue = VALID_DATE1 + "/" + VALID_SUP_FRAMEID;
            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_DATA_URL + DATA_URL_PATH + "?"
                                                + DATA_URL_QUERY +  frameQueryValue);
            Assert.assertEquals(DATA_URL_PATH, url.getPath());
            Assert.assertEquals(DATA_URL_QUERY + frameQueryValue, url.getQuery());
            Assert.assertEquals(BASE_DATA_URL, url.getHost());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }


    @Test
    public void testValidHSCPreviewURI() throws Exception {
        log.debug("START - testValidHSCPreviewURI");
        try {
            String uriStr = subaruResolver.getScheme() + ":" + PREVIEW_URI + "/" + VALID_DATE2 + "/"
                            + VALID_HSC_FRAMEID;
            URI uri = new URI(uriStr);
            log.debug("HSC URI: " + uriStr);
            URL url = subaruResolver.toURL(uri);
            log.debug("toURL returned: " + url.toString());

            String fileSpecificPart = VALID_DATE2 + "/" + EXPECTED_HSC_FILENAME;
            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_PREVIEW_URL + HSC_PREVIEW_URL_PATH + "/"
                                                + fileSpecificPart);
            Assert.assertEquals(HSC_PREVIEW_URL_PATH + "/" + fileSpecificPart, url.getPath());
            Assert.assertEquals(BASE_PREVIEW_URL, url.getHost());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        } finally {
            log.debug("END - testValidHSCPreviewURI");
        }
    }

    @Test
    public void testInvalidURIBadScheme() throws Exception {
        try {
            URI uri = new URI(INVALID_URI_BAD_SCHEME);
            URL url = subaruResolver.toURL(uri);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    @Test
    public void testValidSUPPreviewURI() {
        try {
            final String uriStr = subaruResolver.getScheme() + ":" + PREVIEW_URI + "/" + VALID_DATE1 + "/"
                                  + VALID_SUP_FRAMEID2;
            final URI uri = URI.create(uriStr);
            final URL url = subaruResolver.toURL(uri);

            log.debug("toURL returned: " + url.toString());

            String encodedValue = NetUtil.encode(VALID_SUP_FRAMEID2 + " " + VALID_DATE1);

            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_PREVIEW_URL + PREVIEW_URL_PATH + "?" +
                PREVIEW_URL_QUERY + encodedValue);
            Assert.assertEquals(PREVIEW_URL_PATH, url.getPath());
            Assert.assertEquals(PREVIEW_URL_QUERY + encodedValue, url.getQuery());
            Assert.assertEquals(BASE_PREVIEW_URL, url.getHost());

        } catch (Exception e) {
            log.error("unexpected exception", e);
            throw e;
        }
    }

    @Test
    public void testValidPreHSCPreviewURI() {
        try {
            // TODO: this is the original Suprime-Cam preview format that will eventually
            // be phased out. When it is, this test can be removed - April 30, 2018.
            final String uriStr = subaruResolver.getScheme() + ":" + PREVIEW_URI + "/" + VALID_SUP_FRAMEID2;
            final URI uri = URI.create(uriStr);
            final URL url = subaruResolver.toURL(uri);

            log.debug("toURL returned: " + url.toString());

            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_PREVIEW_URL + PREVIEW_URL_PATH + "?"
                                                + PREVIEW_URL_QUERY + VALID_SUP_FRAMEID2);
            Assert.assertEquals(PREVIEW_URL_PATH, url.getPath());
            Assert.assertEquals(PREVIEW_URL_QUERY + VALID_SUP_FRAMEID2, url.getQuery());
            Assert.assertEquals(BASE_PREVIEW_URL, url.getHost());

        } catch (Exception e) {
            log.error("unexpected exception", e);
            throw e;
        }
    }


    @Test
    public void testInvalidNullURI() {
        try {
            URL url = subaruResolver.toURL(null);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

    @Test
    public void testInvalidUriType() throws Exception {
        try {
            String uriStr = GeminiResolver.SCHEME + ":badURIType/" + VALID_SUP_FRAMEID;
            URI uri = new URI(uriStr);
            URL url = subaruResolver.toURL(uri);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            throw unexpected;
        }
    }

}
