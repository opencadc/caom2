/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.TestSyncOutput;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.SyncInput;
import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import org.easymock.EasyMockRunner;
import org.easymock.MockType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 *
 * @author adriand
 */

@RunWith(EasyMockRunner.class)
public class GetActionTest {
    private static final Logger log = Logger.getLogger(GetActionTest.class);

    private ObservationDAO mockDao;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    @Before
    public void setup() {
        mockDao = EasyMock.createMock(MockType.NICE, ObservationDAO.class);
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testCollectionNotFoundException() throws Exception {

        HttpServletRequest mockRequest = mock(HttpServletRequest.class);

        reset(mockDao);
        expect(mockRequest.getMethod()).andReturn("GET");
        expect(mockRequest.getPathInfo()).andReturn("/BLAH");
        expect(mockDao.getObservationList("BLAH", null, null, null, true)).andReturn(null);

        Enumeration<String> params = Collections.emptyEnumeration();
        expect(mockRequest.getParameterNames()).andReturn(params);
        replay(mockDao, mockRequest);
        GetAction getAction = new TestGetAction(mockDao);
        getAction.setSyncInput(new SyncInput(mockRequest, getAction.getInlineContentHandler()));
        getAction.doAction();
    }

    @Test
    public void testDoIt() throws Exception {
        // test the doIt method when it returns 2 observations
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);

        GetAction getAction = new TestGetAction(mockDao);
        TestSyncOutput out = new TestSyncOutput();
        getAction.setSyncOutput(out);

        reset(mockDao);

        expect(mockRequest.getMethod()).andReturn("GET");
        expect(mockRequest.getPathInfo()).andReturn("/TEST");

        // build the list of observations for the mock dao to return
        List<ObservationState> obsList = new ArrayList<ObservationState>();

        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        ObservationState os1 = new ObservationState(new ObservationURI("TEST", "1234"));
        os1.maxLastModified = df.parse("2010-10-10T10:10:10.10");
        os1.accMetaChecksum = URI.create("md5:5b71d023d4729575d550536dce8439e6");
        obsList.add(os1);

        ObservationState os2 = new ObservationState(new ObservationURI("TEST", "6789"));
        os2.maxLastModified = df.parse("2011-11-11T11:11:11.111");
        os2.accMetaChecksum = URI.create("md5:aedbcf5e27a17fc2daa5a0e0d7840009");
        obsList.add(os2);

        Enumeration<String> params = Collections.emptyEnumeration();
        expect(mockRequest.getParameterNames()).andReturn(params);

        // since no maxRec argument given, expect the default one
        expect(mockDao.getObservationList("TEST", null, null, RepoAction.MAX_LIST_SIZE, true)).andReturn(obsList);

        replay(mockDao, mockRequest);

        getAction.setSyncInput(new SyncInput(mockRequest, getAction.getInlineContentHandler()));
        getAction.run();

        String expected = "TEST" + "\t" + "1234" + "\t" + df.format(os1.maxLastModified) + "\t" + os1.accMetaChecksum.toString() + "\n" + "TEST" + "\t" + "6789"
                + "\t" + df.format(os2.maxLastModified) + "\t" + os2.accMetaChecksum.toString() + "\n";
        String content = out.getContent();
        log.debug("\n--list content start--\n" + content + "\n--list content end--");
        Assert.assertEquals(expected, content);

        // repeat test when start, end, maxRec and ascendingOrder specified
        getAction = new TestGetAction(mockDao);

        reset(mockDao);
        reset(mockRequest);
        // get a new OutSync
        out = new TestSyncOutput();

        getAction.setSyncOutput(out);
        // build the list of observations for the mock dao to return
        expect(mockRequest.getMethod()).andReturn("GET");
        expect(mockRequest.getPathInfo()).andReturn("/TEST");
        List<String> keys = new ArrayList<String>();
        keys.add("MAXREC");
        keys.add("Start");
        keys.add("end");
        params = Collections.enumeration(keys);
        String startDate = "2010-10-10T10:10:10.1";
        String endDate = "2011-11-11T11:11:11.111";
        expect(mockRequest.getParameterNames()).andReturn(params);
        expect(mockRequest.getParameterValues("MAXREC")).andReturn(new String[] { "3" });
        expect(mockRequest.getParameterValues("Start")).andReturn(new String[] { startDate });
        expect(mockRequest.getParameterValues("end")).andReturn(new String[] { endDate });

        // all arguments given
        expect(mockDao.getObservationList("TEST", df.parse(startDate), df.parse(endDate), 3, true)).andReturn(obsList);

        replay(mockDao, mockRequest);

        getAction.setSyncInput(new SyncInput(mockRequest, getAction.getInlineContentHandler()));
        getAction.run();
        Assert.assertEquals(expected, out.getContent());
    }

    private class TestLogInfo extends WebServiceLogInfo {

    }

    // simple test subclass that mocks checkReadPermission to always return true
    private class TestGetAction extends GetAction {
        ObservationDAO dao;

        TestGetAction(ObservationDAO dao) {
            super();
            setLogInfo(new TestLogInfo());
            this.dao = dao;
        }

        @Override
        protected void checkReadPermission(String collection) throws AccessControlException, CertificateException, ResourceNotFoundException, IOException {
        }

        @Override
        protected ObservationDAO getDAO() {
            return dao;
        }
    }

}
