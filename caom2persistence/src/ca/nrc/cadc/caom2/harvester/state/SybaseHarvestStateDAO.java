/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2015.                            (c) 2015.
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

package ca.nrc.cadc.caom2.harvester.state;

import ca.nrc.cadc.util.HexUtil;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

public class SybaseHarvestStateDAO extends HarvestStateDAO
{
    public SybaseHarvestStateDAO(DataSource dataSource, String database, String schema)
    {
        super(dataSource, database, schema);
    }

    protected String getTable(String database, String schema)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("caom2");
        sb.append("_");
        sb.append("HarvestState");
        return sb.toString();
    }

    protected void setUUID(PreparedStatement ps, int col, UUID uuid)
        throws SQLException
    {
        if (uuid != null)
        {
            String hex = HexUtil.toHex(uuid.getMostSignificantBits()) +
                         HexUtil.toHex(uuid.getLeastSignificantBits());
            ps.setBytes(col, HexUtil.toBytes(hex));
        }
        else
        {
            ps.setNull(col++, Types.BINARY);
        }
    }

}
