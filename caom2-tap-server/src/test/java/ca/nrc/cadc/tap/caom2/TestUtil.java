/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.tap.caom2;

import ca.nrc.cadc.ac.Group;
import ca.nrc.cadc.ac.GroupURI;
import ca.nrc.cadc.ac.Role;
import ca.nrc.cadc.ac.client.GMSClient;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.FunctionDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.uws.Job;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Utility class for testings in CAOM 
 * @author Sailor Zhang
 *
 */
public class TestUtil
{
    public static final Logger log = Logger.getLogger(TestUtil.class);
    
    public static TapSchema loadTapSchema()
    {
        // TODO: load tap_schema from files: caom2.tap_schema_content.sql ivoa.tap_schema_content.sql
        // so these tests can run without a DB
        /*
        try
        {
            DBConfig dbConfig = new DBConfig();
            ConnectionConfig connectionConfig = dbConfig.getConnectionConfig("CAOM2_WS_TEST", "cvodb");
            DataSource ds = DBUtil.getDataSource(connectionConfig, true, true);
            TapSchemaDAO dao = new TapSchemaDAO();
            dao.setDataSource(ds);
            return dao.get();
        }
        catch(NoSuchElementException ex)
        {
            throw new RuntimeException("CONFIG: failed to find entry for [CAOM2_WS_TEST,cvodb] in .dbrc");
        }
        catch (IOException ignore)
        {
            log.debug("failed to read .dbrc - cannot load TapSchema");
        }
        */
        TapDataType uuidType = new TapDataType("char", "36", "caom:uuid");
        
        
        
        TableDesc obs = new TableDesc("caom2", "caom2.Observation");
        obs.getColumnDescs().add(new ColumnDesc("caom2.Observation", "obsID", uuidType));
        
        TableDesc plane = new TableDesc("caom2", "caom2.Plane");
        plane.getColumnDescs().add(new ColumnDesc("caom2.Plane", "obsID", uuidType));
        plane.getColumnDescs().add(new ColumnDesc("caom2.Plane", "planeID", uuidType));
        plane.getColumnDescs().add(new ColumnDesc("caom2.Plane", "position_bounds", TapDataType.POLYGON));
        
        SchemaDesc caom2 = new SchemaDesc("caom2");
        caom2.getTableDescs().add(obs);
        caom2.getTableDescs().add(plane);
        
        TapSchema ret = new TapSchema();
        ret.getSchemaDescs().add(caom2);
        
        ret.getFunctionDescs().addAll(new Sub().getFunctionDescs());
        
        return ret;
    }
    private static class Sub extends TapSchemaDAO
    {
        // make this method accessible
        @Override
        public List<FunctionDesc> getFunctionDescs()
        {
            return super.getFunctionDescs();
        }
    }
    
    static Job job = new Job()
    {
        @Override
        public String getID() { return "internal-test-jobID"; }
    };
    
    static class TestGMSClient extends GMSClient
    {
        private Subject subjectWithGroups;
        public TestGMSClient(Subject subjectWithGroups)
        {
            super(URI.create("ivo://cadc.nrc.ca/no-such-thing"));
            this.subjectWithGroups = subjectWithGroups;
        }

        @Override
        public List<Group> getMemberships(Role role)
        {
            Subject cur = AuthenticationUtil.getCurrentSubject();
            List<Group> memberships = new ArrayList<Group>();
            if (cur == subjectWithGroups)
            {
                if (role == Role.MEMBER)
                {
                    memberships.add(new Group(new GroupURI("ivo://example.org/gms?666")));
                    memberships.add(new Group(new GroupURI("ivo://example.org/gms?777")));
                }
            }
            log.info("TestGMSClient: " + memberships.size() + " groups");
            return memberships;
        }
        
    }
}
