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
import ca.nrc.cadc.caom2.artifact.resolvers.VOSpaceResolver;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yeunga
 */
public class VOSpaceCutoutGeneratorTest extends AbstractTest {
    private static final Logger log = Logger.getLogger(VOSpaceCutoutGeneratorTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.INFO);
    }

    private static final String CUTOUT1 = "[1][100:200, 100:200]";
    private static final String CUTOUT2 = "[2][300:400, 300:400]";
    private static final String CUTOUT3 = "[3][500:600, 500:600]";
    private static final String CUTOUT4 = "[4][700:800, 700:800]";
    private static final String CUTOUT_ALPHA = "[AMP_4_0,3][5:20, 20:30]";
    private static final String[] CUTOUTS = new String[] {
        CUTOUT1, CUTOUT2, CUTOUT3, CUTOUT4, CUTOUT_ALPHA
    };
    
    private static final String FILE_URI = "vos://cadc.nrc.ca!vault/FOO/bar.fits";
    private static final String FILE_URI_COMPRESSED = "vos://cadc.nrc.ca!vault/FOO/bar.CF";
    private static final String FILE_URI_COMPRESSED_FITS = "vos://cadc.nrc.ca!vault/FOO/bar.fits.CF";

    VOSpaceCutoutGenerator vosResolver = new VOSpaceCutoutGenerator();

    public VOSpaceCutoutGeneratorTest() {

    }

    @Test
    public void testToURLWithNoLabel() {
        try {
            String label = null;
            List<String> cutouts = Arrays.asList(CUTOUTS);
            URI uri = new URI(FILE_URI);
            vosResolver.setAuthMethod(AuthMethod.ANON);
            URL url = vosResolver.toURL(uri, cutouts, label);
            Assert.assertNotNull(url);
            log.info("testFile: " + uri + " -> " + url);
            
            String path = url.getPath();
            Assert.assertEquals("/vault/files/FOO/bar.fits", path);
            String query = url.getQuery();
            
            String[] paramArray = query.split("&");
            Assert.assertEquals(CUTOUTS.length, paramArray.length);
            for (int i = 0; i < CUTOUTS.length; i++) {
                String[] pv = paramArray[i].split("=");
                String val = NetUtil.decode(pv[1]);
                Assert.assertEquals("SUB", pv[0]);
                Assert.assertEquals(CUTOUTS[i], val);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
