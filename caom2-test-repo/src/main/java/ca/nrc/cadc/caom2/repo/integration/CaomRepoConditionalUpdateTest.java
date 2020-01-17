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

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
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
public class CaomRepoConditionalUpdateTest extends CaomRepoBaseIntTests {
    private static final Logger log = Logger.getLogger(CaomRepoRoundTripTest.class);

    private static final String EXPECTED_CAOM_VERSION = XmlConstants.CAOM2_4_NAMESPACE;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }
    
    private CaomRepoConditionalUpdateTest() {
    }
    
    /**
     * @param resourceID resource identifier of service to test
     * @param pem       PEM file for user with read-write permission
     */
    public CaomRepoConditionalUpdateTest(URI resourceID, String pem) {
        super(resourceID, Standards.CAOM2REPO_OBS_24, pem, null, null);
    }
    
    @Test
    public void testConditonalUpdate() throws Throwable {
        
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        
        Observation orig = new SimpleObservation(TEST_COLLECTION, "testConditionalUpdate");
        final URI origAcc = orig.computeAccMetaChecksum(md5);
        deleteObservation(orig.getURI().getURI().toASCIIString(), subject1, null, null);
         
        log.info("put: orig");
        sendObservation("PUT", orig, subject1, 200, "OK", null);

        // get the observation using subject2
        log.info("get: o1");
        Observation o1 = getObservation(orig.getURI().getURI().toASCIIString(), subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertNotNull(o1);
        final URI acc1 = o1.computeAccMetaChecksum(md5);
        
        // modify
        orig.intent = ObservationIntentType.SCIENCE;
        
        // update
        log.info("update: orig");
        sendObservation("POST", orig, subject1, 200, "OK", null, acc1.toASCIIString());
        
        // get and verify
        log.info("get: updated");
        Observation o2 = getObservation(orig.getURI().getURI().toASCIIString(), subject1, 200, null, EXPECTED_CAOM_VERSION);
        Assert.assertNotNull(o2);
        final URI acc2 = o2.computeAccMetaChecksum(md5);
        
        // attempt second update from orig: rejected
        log.info("update: orig [race loser]");
        sendObservation("POST", orig, subject1, 412, "update blocked", null, origAcc.toASCIIString());
        
        // attempt update from current: accepted
        log.info("update: o2");
        sendObservation("POST", orig, subject1, 200, "OK", null, acc2.toASCIIString());
    }
}
