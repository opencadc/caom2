/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.argus.tap.query;

import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;

/**
 *
 * @author pdowler, Sailor Zhang
 */
public class Util {

    private static Logger log = Logger.getLogger(Util.class);

    /**
     * Obtain a PlainSelect object from a query string.
     * If exception happens, returns null.
     *
     * @param query
     * @return
     */
    public static PlainSelect getPlainSelect(String query) {
        PlainSelect ps = null;
        try {
            Statement stmt = ParserUtil.receiveQuery(query);
            Select select = (Select) stmt;
            ps = (PlainSelect) select.getSelectBody();
        } catch (JSQLParserException ex) {
            throw new RuntimeException("unexpected exception: " + ex);
        }
        return ps;
    }

    /**
     * Combine a list of expressions using AND operation.
     *
     * @param exprList
     * @return
     */
    public static Expression combineAndExpressions(List<Expression> exprList) {
        Expression rtn = null;
        for (Expression expr : exprList) {
            if (rtn == null) {
                rtn = expr;
            } else {
                rtn = new AndExpression(rtn, expr);
            }
        }
        return rtn;
    }

    /**
     * If table alias is presented in the select statement,
     * a column of this table should be presented with alias only.
     * This method treats the column and keep the alias only if it exists.
     *
     * @param column
     * @return
     */
    public static Column useTableAliasIfExists(Column column) {
        Column rtn = null;
        Table newTable = null;

        Table table = column.getTable();
        if (table == null) {
            rtn = column; //no treatment is made, return original column
        } else {
            String alias = table.getAlias();
            if (alias == null || alias.equals("")) {
                rtn = column; //no treatment is made, return original column
            } else {
                newTable = new Table(null, alias);
                rtn = new Column(newTable, column.getColumnName());
            }
        }
        return rtn;
    }

    public static List<String> getGroupIDs(IvoaGroupClient gmsClient) throws AccessControlException {
        List<String> groupIDs = new ArrayList<>();

        IvoaGroupClient gms = gmsClient;
        if (gms == null) {
            gms = new IvoaGroupClient();
        }
        try {
            if (CredUtil.checkCredentials()) {
                LocalAuthority loc = new LocalAuthority();
                URI gmsURI = loc.getServiceURI(Standards.GMS_SEARCH_10.toString());
                Set<GroupURI> groups = gms.getMemberships(gmsURI);
                for (GroupURI g : groups) {
                    groupIDs.add(g.getName());
                }
            }
        } catch (NoSuchElementException ex) {
            log.debug("trusted GMS client not configured - return no groups");
        } catch (CertificateException ex) {
            throw new AccessControlException("failed to find group memberships (invalid proxy certficate)");
        } catch (InterruptedException | IOException | ResourceNotFoundException ex) {
            throw new RuntimeException("failed to find group memberships", ex);
        }

        return groupIDs;
    }

    // considers tables in the ivoa schema to be caom2 tables as well
    static boolean isCAOM2(Table t, List<Table> tabs) {
        log.debug("isCAOM2: " + t);
        boolean caom2 = false;
        for (Table t2 : tabs) {
            log.debug("   vs: " + t2);
            if (t2.getSchemaName().equalsIgnoreCase("caom2") || t2.getSchemaName().equalsIgnoreCase("ivoa")) {
                if (t != null && t2.getAlias() != null && t2.getAlias().equals(t.getName())) {
                    // t.name is a table alias
                    caom2 = true;
                }
                if (t != null && t2.getAlias() != null && t2.getAlias().equals(t.getAlias())) {
                    // t was found in the from clause via findTableWithColumn
                    caom2 = true;
                }
                if (t2.getAlias() == null) {
                    // unqualified caom2 table in the FROM clause
                    caom2 = true;
                }
            }
        }
        log.debug("isCAOM2: " + t + " = " + caom2);
        return caom2;
    }

    static boolean isUploadedTable(Table t, List<Table> tabs) {
        log.debug("isUploadedTable: " + t);
        boolean upload = false;
        for (Table t2 : tabs) {
            log.debug("   vs: " + t2.getWholeTableName() + " AS " + t2.getAlias());
            if (t2.getSchemaName().equalsIgnoreCase("tap_upload")) {
                if (t2.getAlias() != null && t2.getAlias().equals(t.getName())) {
                    return true;
                }
                if (t2.getAlias() == null) {
                    upload = true; // unqualified upload table in the FROM clause
                }
            }
        }
        return upload;
    }

    static Table findTableWithColumn(Column c, List<Table> tabs, TapSchema ts) {
        log.debug("find: " + c.getColumnName());
        Table ret = null;
        for (SchemaDesc sd : ts.getSchemaDescs()) {
            for (TableDesc td : sd.getTableDescs()) {
                for (ColumnDesc cd : td.getColumnDescs()) {
                    log.debug("find/check: " + cd.getColumnName() + " vs " + c.getColumnName());
                    if (cd.getColumnName().equalsIgnoreCase(c.getColumnName())) {
                        // found matching column: check if the table is in use
                        for (Table t : tabs) {
                            log.debug("find/verify: " + td.getTableName() + " vs " + t.getWholeTableName());
                            if (td.getTableName().equalsIgnoreCase(t.getWholeTableName())) {
                                if (ret != null) {
                                    throw new IllegalArgumentException("ambigious column reference: " + c.getColumnName());
                                }
                                ret = t;
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }
}
