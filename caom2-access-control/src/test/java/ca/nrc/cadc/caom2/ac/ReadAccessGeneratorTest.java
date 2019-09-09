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
************************************************************************
 */

package ca.nrc.cadc.caom2.ac;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.util.Log4jInit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

public class ReadAccessGeneratorTest {
    private static final Logger log = Logger.getLogger(ReadAccessGeneratorTest.class);

    private static final String GROUP_BASE_URI = "ivo://cadc.nrc.ca/gms";
    private static final String OPERATOR_GROUP = GROUP_BASE_URI + "?CADC";
    private static final String STAFF_GROUP = GROUP_BASE_URI + "?JCMT-Staff";

    String collection = "TEST";
    Map<String, Object> groupConfig;

    public ReadAccessGeneratorTest() { }
    
    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom.ac", Level.DEBUG);
    }

    protected void setup() throws Exception {
        this.groupConfig = new HashMap<String, Object>();
        groupConfig.put("proposalGroup", true);
        groupConfig.put("operatorGroup", new GroupURI(OPERATOR_GROUP));
        groupConfig.put("staffGroup", new GroupURI(STAFF_GROUP));
    }
    
    @Test
    public void testPublic() {
        try {
            setup();
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, -20L); // 20ms in the past

            ReadAccessGenerator da = new ReadAccessGenerator(collection, groupConfig);
            
            GroupURI propGroupName = da.getProposalGroupID(collection, obs.proposal);

            da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 0, obs.getMetaReadGroups().size());
            
            for (Plane p : obs.getPlanes()) {
                da.createPlaneMetaReadAccess(p, now, propGroupName);
                Assert.assertEquals("pmra", 0, p.getMetaReadGroups().size());
                da.createPlaneDataReadAccess(p, now, propGroupName);
                Assert.assertEquals("pdra", 0, p.getDataReadGroups().size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateNull() {
        try {
            setup();
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, null, 0L); // null release dates

            ReadAccessGenerator da = new ReadAccessGenerator(collection, groupConfig);
            
            GroupURI propGroupName = da.getProposalGroupID(collection, obs.proposal);

            da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 3, obs.getMetaReadGroups().size());
            
            for (Plane p : obs.getPlanes()) {
                da.createPlaneMetaReadAccess(p, now, propGroupName);
                Assert.assertEquals("pmra", 3, p.getMetaReadGroups().size());
                da.createPlaneDataReadAccess(p, now, propGroupName);
                Assert.assertEquals("pdra", 3, p.getDataReadGroups().size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateFuture() {
        try {
            setup();
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, 20L); // 20ms in future
            
            ReadAccessGenerator da = new ReadAccessGenerator(collection, groupConfig);
            
            GroupURI propGroupName = da.getProposalGroupID(collection, obs.proposal);

            da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 3, obs.getMetaReadGroups().size());
            
            for (Plane p : obs.getPlanes()) {
                da.createPlaneMetaReadAccess(p, now, propGroupName);
                Assert.assertEquals("pmra", 3, p.getMetaReadGroups().size());
                da.createPlaneDataReadAccess(p, now, propGroupName);
                Assert.assertEquals("pdra", 3, p.getDataReadGroups().size());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPrivateNullProposal() {
        try {
            setup();
            Date now = new Date();
            
            Observation obs = getSampleObservation("1", 1, now, 20000L); // 20 s in future
            obs.proposal = null;
            
            ReadAccessGenerator da = new ReadAccessGenerator(collection, groupConfig);
            
            GroupURI propGroupName = da.getProposalGroupID(collection, obs.proposal);

            da.createObservationMetaReadAccess(obs, now, propGroupName);
            Assert.assertEquals("omra", 2, obs.getMetaReadGroups().size());
            
            for (Plane p : obs.getPlanes()) {
                da.createPlaneMetaReadAccess(p, now, propGroupName);
                Assert.assertEquals("pmra", 2, p.getMetaReadGroups().size());
                da.createPlaneDataReadAccess(p, now, propGroupName);
                Assert.assertEquals("pdra", 2, p.getDataReadGroups().size());
            }
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
