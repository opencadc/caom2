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

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 *
 * @author pdowler
 */
public class SybaseReadAccessDAOTest extends AbstractDatabaseReadAccessDAOTest
{
    static
    {
        log = Logger.getLogger(SybaseReadAccessDAOTest.class);
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
    }

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    
    public SybaseReadAccessDAOTest()
        throws Exception
    {
        super(SybaseSQLGenerator.class, "CAOM2_SYB_TEST", "cadctest", System.getProperty("user.name"), true);
        this.entityClasses = new Class[]
        {
            ObservationMetaReadAccess.class,
            PlaneMetaReadAccess.class,
            PlaneDataReadAccess.class
        };
    }

//    @Override
//    protected void doPutGetDelete(ReadAccess expected)
//        throws Exception
//    {
//        Util.assignLastModified(expected, new Date(), "lastModified");
//        String s = expected.getClass().getSimpleName();
//        //dao.put(expected);
//        StringBuilder sb = new StringBuilder();
//        sb.append("INSERT INTO ");
//        sb.append(dao.getSQLGenerator().getTable(expected.getClass()));
//        sb.append(" (assetID,groupID,lastModified,stateCode,readAccessID) VALUES (");
//        sb.append(expected.getAssetID().toString());
//        sb.append(",'");
//        sb.append(expected.getGroupID().toASCIIString());
//        sb.append("','");
//        sb.append(df.format(expected.getLastModified()));
//        sb.append("',");
//        sb.append(expected.getStateCode());
//        sb.append(",'");
//        sb.append(expected.getID().toString());
//        sb.append("') ");
//        String sql = sb.toString();
//        log.debug("manual INSERT: " + sql);
//        Statement st = dao.getDataSource().getConnection().createStatement();
//        st.execute(sql);
//        st = dao.getDataSource().getConnection().createStatement();
//        sql = "select readAccessID from " + dao.getSQLGenerator().getTable(expected.getClass());
//        log.debug("manual SELECT: " + sql);
//        ResultSet rs = st.executeQuery(sql);
//        if (rs.next())
//        {
//            UUID id = Util.getUUID(rs, 1);
//            log.debug("generated ID: " + id);
//            Assert.assertNotNull("generated id", id);
//            Util.assignID(expected, id);
//        }
//        
//        ReadAccess actual = dao.get(expected.getClass(), expected.getID());
//        Assert.assertNotNull(s, actual);
//        Assert.assertEquals(s+".assetID", expected.getAssetID(), actual.getAssetID());
//        Assert.assertEquals(s+".groupID", expected.getGroupID(), actual.getGroupID());
//        testEqual(s+".lastModified", expected.getLastModified(), actual.getLastModified());
//        Assert.assertEquals(s+".getChecksum", expected.getStateCode(), actual.getStateCode());
//
//        dao.delete(expected.getClass(), expected.getID());
//        actual = dao.get(expected.getClass(), expected.getID());
//        Assert.assertNull(actual);
//    }
}
