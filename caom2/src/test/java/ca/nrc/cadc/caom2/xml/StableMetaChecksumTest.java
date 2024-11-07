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
************************************************************************
 */

package ca.nrc.cadc.caom2.xml;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.net.URI;
import java.security.MessageDigest;
import java.util.MissingResourceException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class StableMetaChecksumTest {

    private static final Logger log = Logger.getLogger(StableMetaChecksumTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
        Log4jInit.setLevel("org.opencadc.persist", Level.DEBUG);
        org.opencadc.persist.Entity.MCS_DEBUG = true;
    }

    public StableMetaChecksumTest() {
    }

    @Test
    public void testStableChecksums24() {
        // CAOM-2.4 documents are readable but checksum changes
        doit("sample-composite-caom24-complete.xml", false);
    }

    @Test
    public void testStableChecksums25() {
        doit("sample-derived-caom25.xml", true);
    }

    // checksums are not stable between 2.4 and 2.5 due to a change in the checksum algorithm
    // that also allowed other non-compatible changes in the model
    private void doit(String filename, boolean stable) {
        try {
            // read stored file with computed checksums and verify
            File f = FileUtil.getFileFromResource(filename, StableMetaChecksumTest.class);
            if (!f.exists()) {
                log.warn("testStableChecksums: not found: " + f.getName() + " -- SKIPPING TEST");
                return;
            }

            Reader r = new FileReader(f);
            ObservationReader or = new ObservationReader(); // latest model/schema version
            Observation o = or.read(r);
            Assert.assertNotNull(o);

            boolean verifyAcc = false;
            log.info(filename + ": verify metaChecksum: true verify accMetaChecksum: " + verifyAcc);

            MessageDigest digest = MessageDigest.getInstance("MD5");
            URI mcs = o.computeMetaChecksum(digest);
            if (stable) {
                Assert.assertEquals("observation.metaChecksum (" + stable + ")", o.getMetaChecksum(), mcs);
            } else {
                Assert.assertNotEquals("observation.metaChecksum (" + stable + ")", o.getMetaChecksum(), mcs);
            }
            if (verifyAcc) {
                URI acs = o.computeAccMetaChecksum(digest);
                if (stable) {
                    Assert.assertEquals("observation.accMetaChecksum (" + stable + ")", o.getAccMetaChecksum(), acs);
                } else {
                    Assert.assertNotEquals("observation.accMetaChecksum (" + stable + ")", o.getAccMetaChecksum(), acs);
                }
            }

            for (Plane pl : o.getPlanes()) {
                mcs = pl.computeMetaChecksum(digest);
                if (stable) {
                    Assert.assertEquals("plane.metaChecksum (" + stable + ")", pl.getMetaChecksum(), mcs);
                } else {
                    Assert.assertNotEquals("plane.metaChecksum (" + stable + ")", pl.getMetaChecksum(), mcs);
                }
                if (verifyAcc) {
                    URI acs = pl.computeAccMetaChecksum(digest);
                    if (stable) {
                        Assert.assertEquals("plane.accMetaChecksum (" + stable + ")", pl.getAccMetaChecksum(), acs);
                    } else {
                        Assert.assertNotEquals("plane.accMetaChecksum (" + stable + ")", pl.getAccMetaChecksum(), acs);
                    }
                }
                for (Artifact ar : pl.getArtifacts()) {
                    mcs = ar.computeMetaChecksum(digest);
                    if (stable) {
                        Assert.assertEquals("artifact.metaChecksum", ar.getMetaChecksum(), mcs);
                    } else {
                        Assert.assertNotEquals("artifact.metaChecksum", ar.getMetaChecksum(), mcs);
                    }
                    if (verifyAcc) {
                        URI acs = ar.computeAccMetaChecksum(digest);
                        if (stable) {
                            Assert.assertEquals("artifact.accMetaChecksum", ar.getAccMetaChecksum(), acs);
                        } else {
                            Assert.assertNotEquals("artifact.accMetaChecksum", ar.getAccMetaChecksum(), acs);
                        }
                    }
                    for (Part pa : ar.getParts()) {
                        mcs = pa.computeMetaChecksum(digest);
                        if (stable) {
                            Assert.assertEquals("part.metaChecksum", pa.getMetaChecksum(), mcs);
                        } else {
                            Assert.assertNotEquals("part.metaChecksum", pa.getMetaChecksum(), mcs);
                        }
                        if (verifyAcc) {
                            URI acs = pa.computeAccMetaChecksum(digest);
                            if (stable) {
                                Assert.assertEquals("part.accMetaChecksum", pa.getAccMetaChecksum(), acs);
                            } else {
                                Assert.assertNotEquals("part.accMetaChecksum", pa.getAccMetaChecksum(), acs);
                            }
                        }
                        for (Chunk ch : pa.getChunks()) {
                            mcs = ch.computeMetaChecksum(digest);
                            if (stable) {
                                Assert.assertEquals("chunk.metaChecksum", ch.getMetaChecksum(), mcs);
                            } else {
                                Assert.assertNotEquals("chunk.metaChecksum", ch.getMetaChecksum(), mcs);
                            }
                            if (verifyAcc) {
                                URI acs = ch.computeAccMetaChecksum(digest);
                                if (stable) {
                                    Assert.assertEquals("chunk.accMetaChecksum", ch.getAccMetaChecksum(), acs);
                                } else {
                                    Assert.assertNotEquals("chunk.accMetaChecksum", ch.getAccMetaChecksum(), acs);
                                }
                            }
                        }
                    }
                }
            }

            log.info("verify metaChecksum: true verify accMetaChecksum: " + verifyAcc + " [OK]");
        } catch (MissingResourceException oops) {
            log.warn("SKIPPING TEST: " + oops);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
