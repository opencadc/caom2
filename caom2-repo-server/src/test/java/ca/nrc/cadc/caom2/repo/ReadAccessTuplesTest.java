/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2014.                            (c) 2014.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.ac.GroupURI;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.PropertiesReader;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ReadAccessTuplesTest {
    private static final Logger log = Logger.getLogger(ReadAccessTuplesTest.class);

    String archive = "TEST";

    public ReadAccessTuplesTest() { }
    
    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom.ac", Level.DEBUG);
        System.setProperty(PropertiesReader.class.getName() + ".dir", "src/test/resources");
    }

    @Test
    public void testPublic() {
        try {
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, -20L); // 20ms in the past

            ReadAccessTuples da = new ReadAccessTuples(archive);
            
            GroupURI propGroupName = da.getProposalGroupID(archive, obs.proposal);

            List<ObservationMetaReadAccess> omraActual = da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 0, omraActual.size());
            
            for (Plane p : obs.getPlanes()) {
                List<PlaneMetaReadAccess> pmraActual = da.createPlaneMetaReadAccess(obs, now, propGroupName);
                Assert.assertEquals("pmra", 0, pmraActual.size());
                List<PlaneDataReadAccess> pdraActual = da.createPlaneDataReadAccess(obs, now, propGroupName);
                Assert.assertEquals("pdra", 0, pdraActual.size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateNull() {
        try {
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, null, 0L); // null release dates

            ReadAccessTuples da = new ReadAccessTuples(archive);
            
            GroupURI propGroupName = da.getProposalGroupID(archive, obs.proposal);

            List<ObservationMetaReadAccess> omraActual = da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 3, omraActual.size());
            
            List<PlaneMetaReadAccess> pmraActual = da.createPlaneMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pmra", 3, pmraActual.size());
            List<PlaneDataReadAccess> pdraActual = da.createPlaneDataReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pdra", 3, pdraActual.size());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateFuture() {
        try {
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, 20L); // 20ms in future
            
            ReadAccessTuples da = new ReadAccessTuples(archive);
            
            GroupURI propGroupName = da.getProposalGroupID(archive, obs.proposal);

            List<ObservationMetaReadAccess> omraActual = da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 3, omraActual.size());
            
            List<PlaneMetaReadAccess> pmraActual = da.createPlaneMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pmra", 3, pmraActual.size());
            List<PlaneDataReadAccess> pdraActual = da.createPlaneDataReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pdra", 3, pdraActual.size());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateNullProposal() {
        try {
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, 20L); // 20ms in future
            obs.proposal = null;
            
            ReadAccessTuples da = new ReadAccessTuples(archive);
            
            GroupURI propGroupName = da.getProposalGroupID(archive, obs.proposal);

            List<ObservationMetaReadAccess> omraActual = da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 2, omraActual.size());
            
            List<PlaneMetaReadAccess> pmraActual = da.createPlaneMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pmra", 2, pmraActual.size());
            List<PlaneDataReadAccess> pdraActual = da.createPlaneDataReadAccess(obs, now, propGroupName);
            Assert.assertEquals("pdra", 2, pdraActual.size());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    Observation getSampleObservation(final String id, final int numPlanes, Date now, long dateOffset) {
        Observation obs = new SimpleObservation("obsID_" + id , "collectionID_" + id);
        obs.proposal = new Proposal("proposalID_" + id);
        Date rd = null;
        if (now != null) {
            rd = new Date(now.getTime() + dateOffset);
        }
        obs.metaRelease = rd;
        for (int i = 0; i < numPlanes; i++) {
            Plane p = new Plane("productID_" + id + i);
            if (now != null) {
                p.metaRelease = rd;
                p.dataRelease = rd;
            }
            obs.getPlanes().add(p);
        }
        return obs;
    }
}
