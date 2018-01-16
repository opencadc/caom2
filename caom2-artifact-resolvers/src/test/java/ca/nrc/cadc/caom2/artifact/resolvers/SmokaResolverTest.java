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

package ca.nrc.cadc.caom2.artifact.resolvers;

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
public class SmokaResolverTest {
    private static final Logger log = Logger.getLogger(SmokaResolverTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    String VALID_FILE1 = "SUPE01318470";
    String VALID_FILE2 = "SUPE01318470";
    String BASE_URL = "smoka.nao.ac.jp";

    // Copied from SmokaResolver for use in testing
    String FILE_URI = "file";
    String PREVIEW_URI = "preview";
    String FILE_URL_QUERY = "object=&resolver=SIMBAD&coordsys=Equatorial&equinox=J2000&fieldofview=auto"
        + "&RadOrRec=radius&longitudeC=&latitudeC=&radius=10.0&longitudeF=&latitudeF=&longitudeT=&latitudeT"
        + "=&date_obs=&exptime=&observer=&prop_id=&frameid=&dataset=&asciitable=Table"
        + "&frameorshot=Frame&action=Search&instruments=SUP&instruments=HSC&multiselect_0=SUP&multiselect_0=HSC"
        + "&multiselect_0=SUP&multiselect_0=HSC&obs_mod=IMAG&obs_mod=SPEC&obs_mod=IPOL&multiselect_1=IMAG&multiselect_1=SPEC"
        + "&multiselect_1=IPOL&multiselect_1=IMAG&multiselect_1=SPEC&multiselect_1=IPOL&data_typ=OBJECT&multiselect_2=OBJECT"
        + "&multiselect_2=OBJECT&bandwidth_type=FILTER&band=&dispcol=FRAMEID&dispcol=DATE_OBS&dispcol=FITS_SIZE&dispcol=OBS_MODE"
        + "&dispcol=DATA_TYPE&dispcol=OBJECT&dispcol=FILTER&dispcol=WVLEN&dispcol=DISPERSER&dispcol=RA2000&dispcol=DEC2000"
        + "&dispcol=UT_START&dispcol=EXPTIME&dispcol=OBSERVER&dispcol=EXP_ID&orderby=FRAMEID&diff=100&output_equinox=J2000&from=0"
        + "&exp_id="; //&exp_id=SUPE01318470 or similar for last entry here.
    String PREVIEW_URL_QUERY = "grayscale=linear&mosaic=true&frameid=";
    String FILE_URL_PATH = "/fssearch";
    String PREVIEW_URL_PATH = "/qlis/ImagePNG";
    String PROTOCOL_STR = "http://";

    // Invalid checks the scheme and the request type (needs to be 'file' or 'preview'
    String INVALID_URI_BAD_SCHEME = "pokey:little/puppy";

    SmokaResolver smokaResolver = new SmokaResolver();

    public SmokaResolverTest() {
    }

    @Test
    public void testGetSchema() {
        Assert.assertTrue(smokaResolver.getScheme().equals(smokaResolver.getScheme()));
    }

    @Test
    public void testValidURI() {
        try {
            String uriStr = smokaResolver.getScheme() + ":" + FILE_URI + "/" + VALID_FILE1;
            URI uri = new URI(uriStr);
            URL url = smokaResolver.toURL(uri);
            log.debug("toURL returned: " + url.toString());

            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_URL + FILE_URL_PATH + "?" + FILE_URL_QUERY + VALID_FILE2);
            Assert.assertEquals(FILE_URL_PATH, url.getPath());
            Assert.assertEquals(FILE_URL_QUERY + VALID_FILE1, url.getQuery());
            Assert.assertEquals(BASE_URL, url.getHost());


            uriStr = smokaResolver.getScheme() + ":" + PREVIEW_URI + "/" + VALID_FILE2;
            uri = new URI(uriStr);
            url = smokaResolver.toURL(uri);
            log.debug("toURL returned: " + url.toString());
            // http://smoka.nao.ac.jp/qlis/ImagePNG?grayscale=linear&mosaic=true&frameid=SUPE01318470

            Assert.assertEquals(url.toString(), PROTOCOL_STR + BASE_URL + PREVIEW_URL_PATH + "?" + PREVIEW_URL_QUERY + VALID_FILE2);
            Assert.assertEquals(PREVIEW_URL_PATH, url.getPath());
            Assert.assertEquals(PREVIEW_URL_QUERY + VALID_FILE2, url.getQuery());
            Assert.assertEquals(BASE_URL, url.getHost());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidURIBadScheme() {
        try {
            URI uri = new URI(INVALID_URI_BAD_SCHEME);
            URL url = smokaResolver.toURL(uri);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidNullURI() {
        try {
            URL url = smokaResolver.toURL(null);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testInvalidUriType() {
        try {
            String uriStr = GeminiResolver.SCHEME + ":badURIType/" + VALID_FILE1;
            URI uri = new URI(uriStr);
            URL url = smokaResolver.toURL(uri);
            Assert.fail("expected IllegalArgumentException, got " + url);
        } catch (IllegalArgumentException expected) {
            log.info("IllegalArgumentException thrown as expected. Test passed.: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
