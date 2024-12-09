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

package ca.nrc.cadc.caom2.repo.integration;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * This integration test round-trips a complete CAOM observation (latest version) and verifies
 * the Observation.accMetaChecksum of the response.
 * 
 * @author pdowler
 */
public class CaomRepoRoundTripTest extends CaomRepoBaseIntTests {
    private static final Logger log = Logger.getLogger(CaomRepoRoundTripTest.class);

    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_4_NAMESPACE;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }
    
    private CaomRepoRoundTripTest() {
    }
    
    /**
     * @param resourceID resource identifier of service to test
     * @param pem       PEM file for user with read-write permission
     */
    public CaomRepoRoundTripTest(URI resourceID, String pem) {
        super(resourceID, Standards.CAOM2REPO_OBS_24, pem, null, null);
    }
    
    @Test
    public void testCleanPutGetDelete() throws Throwable {
        URL src = FileUtil.getURLFromResource("sample-composite-caom24.xml", this.getClass());
        MessageDigest md5 = MessageDigest.getInstance("MD5");

        ObservationReader r = new ObservationReader();
        Observation observation = r.read(src.openStream());
        Assert.assertEquals("test collection", "TEST", observation.getCollection());
        
        URI expected = observation.getAccMetaChecksum();
        URI recomp = observation.computeAccMetaChecksum(md5);
        Assert.assertEquals("observation.accMetaChecksum", expected, recomp);
        
        String uri = observation.getURI().getURI().toASCIIString();
        deleteObservation(uri, subject1, null, null);
         
        putObservation(observation, subject1, 200, "OK", null);

        // get the observation using subject2
        Observation roundtrip = getObservation(uri, subject1, 200, null, EXPECTED_CAOM_VERSION);
        URI actual = roundtrip.computeAccMetaChecksum(md5);
        boolean valid = validate(roundtrip, 5, false);
        Assert.assertTrue("complete checksum validation", valid);
        
        Assert.assertEquals("roundtrip.accMetaChecksum", expected, actual);

        // currently not doing cleanup after so this complete observation remains in the 
        // repository
        //deleteObservation(uri, subject1, null, null);
    }

    private boolean checkMismatch(StringBuilder sb, URI u1, URI u2) {
        sb.append(u1);
        boolean eq = u1.equals(u2);
        if (eq) {
            sb.append(" == ");
        } else {
            sb.append(" != ");
        }
        sb.append(u2);
        if (!eq) {
            sb.append(" [MISMATCH]");
            return true;
        }
        return false;
    }
    
    private void out(String s) {
        System.out.println(s);
    }
    
    private boolean validate(Observation obs, int depth, boolean acc) {
        boolean fail = false;
        try {
            log.info("read: " + obs.getCollection() + "/" + obs.getObservationID() + " :: " + obs.getAccMetaChecksum());
            log.info("depth: " + depth);
            
            StringBuilder cs = new StringBuilder();
            StringBuilder acs = new StringBuilder();
            MessageDigest digest = MessageDigest.getInstance("MD5");
            if (depth > 1) {
                for (Plane pl : obs.getPlanes()) {
                    if (depth > 2) {
                        for (Artifact ar : pl.getArtifacts()) {
                            if (depth > 3) {
                                for (Part pa : ar.getParts()) {
                                    if (depth > 4) {
                                        for (Chunk ch : pa.getChunks()) {
                                            URI chunkCS = ch.computeMetaChecksum(digest);
                                            cs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                            boolean b = checkMismatch(cs, ch.getMetaChecksum(), chunkCS);
                                            fail = fail || b;
                                            if (acc) {
                                                URI chunkACS = ch.computeAccMetaChecksum(digest);
                                                acs.append("\n      chunk: ").append(ch.getID()).append(" ");
                                                boolean ab = checkMismatch(acs, ch.getAccMetaChecksum(), chunkACS);
                                                fail = fail || ab;
                                            }
                                        }
                                    }
                                    URI partCS = pa.computeMetaChecksum(digest);
                                    cs.append("\n       part: ").append(pa.getID()).append(" ");
                                    boolean b = checkMismatch(cs, pa.getMetaChecksum(), partCS);
                                    fail = fail || b;
                                    if (acc) {
                                        URI partACS = pa.computeAccMetaChecksum(digest);
                                        acs.append("\n      part: ").append(pa.getID()).append(" ");
                                        boolean ab = checkMismatch(acs, pa.getAccMetaChecksum(), partACS);
                                        fail = fail || ab;
                                    }
                                }
                            }
                            URI artifactCS = ar.computeMetaChecksum(digest);
                            cs.append("\n       artifact: ").append(ar.getID()).append(" ");
                            boolean b = checkMismatch(cs, ar.getMetaChecksum(), artifactCS);
                            fail = fail || b;
                            if (acc) {
                                URI artifactACS = ar.computeAccMetaChecksum(digest);
                                acs.append("\n      artifact: ").append(ar.getID()).append(" ");
                                boolean ab = checkMismatch(acs, ar.getAccMetaChecksum(), artifactACS);
                                fail = fail || ab;
                            }
                        }
                    }
                    URI planeCS = pl.computeMetaChecksum(digest);
                    cs.append("\n      plane: ").append(pl.getID()).append(" ");
                    boolean b = checkMismatch(cs, pl.getMetaChecksum(), planeCS);
                    fail = fail || b;
                    if (acc) {
                        URI planeACS = pl.computeAccMetaChecksum(digest);
                        acs.append("\n     plane: ").append(pl.getID()).append(" ");
                        boolean ab = checkMismatch(acs, pl.getAccMetaChecksum(), planeACS);
                        fail = fail || ab;
                    }
                }
            }
            URI observationCS = obs.computeMetaChecksum(digest);
            cs.append("\nobservation: ").append(obs.getID()).append(" ");
            boolean b = checkMismatch(cs, obs.getMetaChecksum(), observationCS);
            fail = fail || b;
            if (acc) {
                URI observationACS = obs.computeAccMetaChecksum(digest);
                acs.append("\nobservation: ").append(obs.getID()).append(" ");
                boolean ab = checkMismatch(acs, obs.getAccMetaChecksum(), observationACS);
                fail = fail || ab;
            }

            out("** metaChecksum **");
            out(cs.toString());
            if (acc) {
                out("** accMetaChecksum **");
                out(acs.toString());
            }
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
        }
        return !fail;
    }
}
