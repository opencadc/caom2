/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2.repo.action;

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.repo.TestSyncOutput;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.RestServlet;
import ca.nrc.cadc.util.Log4jInit;

import java.security.AccessControlException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;


/**
 *
 * @author pdowler
 */
public class RepoActionTest {
    private static final Logger log = Logger.getLogger(RepoActionTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    // @Test
    public void testTemplate() {
        try {
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // test the exception handling in RepoAction.run()

    @Test
    public void testObservationNotFoundException() {
        try {
            TestSyncOutput out = new TestSyncOutput();
            ObservationURI uri = new ObservationURI("FOO", "bar");
            TestAction ta = new TestAction(new ResourceNotFoundException("Observation not found: " + uri));
            ta.setSyncOutput(out);

            ta.run();
            Assert.assertEquals(404, out.getCode());
            String msg = "Observation not found: ";
            String actual = out.getContent().substring(0, msg.length());
            Assert.assertEquals(msg, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAccessControlException() {
        try {
            TestSyncOutput out = new TestSyncOutput();
            ObservationURI uri = new ObservationURI("FOO", "bar");
            TestAction ta = new TestAction(new AccessControlException("permission denied: message"));
            ta.setSyncOutput(out);

            ta.run();
            Assert.assertEquals(403, out.getCode());
            String msg = "permission denied: ";
            String actual = out.getContent().substring(0, msg.length());
            Assert.assertEquals(msg, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testIllegalArgumentException() {
        try {
            TestSyncOutput out = new TestSyncOutput();
            TestAction ta = new TestAction(new IllegalArgumentException("testIllegalArgumentException message"));
            ta.setSyncOutput(out);

            ta.run();
            Assert.assertEquals(400, out.getCode());
            String msg = "testIllegalArgumentException message";
            String actual = out.getContent().substring(0, msg.length());
            Assert.assertEquals(msg, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testObservationAlreadyExistsException() {
        try {
            TestSyncOutput out = new TestSyncOutput();
            ObservationURI uri = new ObservationURI("FOO", "bar");
            TestAction ta = new TestAction(new ResourceAlreadyExistsException("Observation already exists: " + uri));
            ta.setSyncOutput(out);

            ta.run();
            Assert.assertEquals(409, out.getCode());
            String msg = "Observation already exists: ";
            String actual = out.getContent().substring(0, msg.length());
            Assert.assertEquals(msg, actual);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private class TestLogInfo extends WebServiceLogInfo {

    }

    // simple test subclass that throws
    private class TestAction extends RepoAction {
        private Exception ex;

        TestAction(Exception ex) {
            super();
            this.ex = ex;
            setLogInfo(new TestLogInfo());
            Map<String, String> initParams = new HashMap<String, String>();
            initParams.put("authHeaders", "false");
            setInitParams(initParams);
        }

        @Override
        public void doAction() throws Exception {
            throw ex;
        }
    }

}
