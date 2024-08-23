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

package ca.nrc.cadc.tap.caom2;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import ca.nrc.cadc.tap.parser.operator.postgresql.PgTextSearchMatch;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.log4j.Logger;
import org.opencadc.gms.IvoaGroupClient;

/**
 * Query converter that injects meta-read-access constraints.
 *
 * @author pdowler, Sailor Zhang
 */
public class CaomReadAccessConverter extends SelectNavigator
{
    private static Logger log = Logger.getLogger(CaomReadAccessConverter.class);

    public static class AssetTable
    {
        public String keyColumn;
        public String metaReleaseColumn;
        public String metaReadAccessColumn;
        public boolean nullKeyIsPublic;
        public boolean isView = false;

        AssetTable() { }
        AssetTable(String assetColumn, String metaReleaseColumn, String metaReadAccessTable)
        {
            this(assetColumn, metaReleaseColumn, metaReadAccessTable, false);
        }
        AssetTable(String keyColumn, String metaReleaseColumn, String metaReadAccessColumn, boolean nullKeyIsPublic)
        {
            this.keyColumn = keyColumn;
            this.metaReleaseColumn = metaReleaseColumn;
            this.metaReadAccessColumn = metaReadAccessColumn;
            this.nullKeyIsPublic = nullKeyIsPublic;
        }
    }

    public static final Map<String,AssetTable> ASSET_TABLES = new HashMap<String,AssetTable>();
    static
    {
        // caom2
        ASSET_TABLES.put("caom2.Observation".toLowerCase(), new AssetTable("obsID", "metaRelease", "metaReadAccessGroups"));
        ASSET_TABLES.put("caom2.Plane".toLowerCase(), new AssetTable("planeID", "metaRelease", "metaReadAccessGroups"));
        //ASSET_TABLES.put("caom2.Artifact".toLowerCase(), new AssetTable("artifactID", "metaRelease", "metaReadAccessGroups"));
        //ASSET_TABLES.put("caom2.Part".toLowerCase(), new AssetTable("partID", "metaRelease", "metaReadAccessGroups"));
        ASSET_TABLES.put("caom2.Chunk".toLowerCase(), new AssetTable("chunkID", "metaRelease", "metaReadAccessGroups"));

        AssetTable at = new AssetTable("planeID", "metaRelease", "metaReadAccessGroups");
        at.isView = true;
        ASSET_TABLES.put("caom2.ObsCore".toLowerCase(), at); // observation join plane
        ASSET_TABLES.put("caom2.SIAv1".toLowerCase(), at);   // observation join plane join artifact
    }

    private transient DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    private IvoaGroupClient gmsClient;

    public CaomReadAccessConverter()
    {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
    }

    void setGMSClient(IvoaGroupClient gmsClient)
    {
        this.gmsClient = gmsClient;
    }

    @Override
    public void visit(PlainSelect ps)
    {
        log.debug("visit(PlainSelect) " + ps);
        super.visit(ps);
        Expression exprAccessControl = accessControlExpression(ps);
        if (exprAccessControl == null)
            return; // nothing to do

        Expression where = ps.getWhere();
        if (where == null)
            ps.setWhere(exprAccessControl);
        else
        {
            Parenthesis par = new Parenthesis(where);
            Expression and = new AndExpression(par, exprAccessControl);
            ps.setWhere(and);
        }
    }

    private Expression accessControlExpression(PlainSelect ps)
    {
        List<Expression> exprAcList = new ArrayList<Expression>();
        List<Table> fromTableList = ParserUtil.getFromTableList(ps);
        for (Table assetTable : fromTableList)
        {
            String fromTableWholeName = assetTable.getWholeTableName().toLowerCase();
            log.debug("check: " + fromTableWholeName);
            AssetTable at = ASSET_TABLES.get(fromTableWholeName);
            if ( at != null)
            {
                Expression accessControlExpr = null;
                Expression assetPublicExpr = metaReleaseControlExpression(assetTable, at);
                if (assetPublicExpr == null)
                    assetPublicExpr = publicAssetByID(assetTable, at.keyColumn, at.nullKeyIsPublic);
                List<String> gids = Util.getGroupIDs(gmsClient);
                Expression groupControlExpr = groupAccessControlExpression(assetTable, at.metaReadAccessColumn, gids);

                if (assetPublicExpr != null && groupControlExpr != null)
                    accessControlExpr = new Parenthesis(new OrExpression(assetPublicExpr, groupControlExpr));
                else if (assetPublicExpr != null)
                    accessControlExpr = new Parenthesis(assetPublicExpr);
                else if (groupControlExpr != null)
                    accessControlExpr = new Parenthesis(groupControlExpr);

                if (accessControlExpr != null)
                    exprAcList.add(accessControlExpr);
                else
                    throw new UnsupportedOperationException("cannot control access to table " + fromTableWholeName);
            }
            else
                log.debug("not an asset table: " + fromTableWholeName);
        }
        if (exprAcList.size() > 0)
            return Util.combineAndExpressions(exprAcList);
        return null;
    }

    private Expression publicAssetByID(Table fromTable, String assetColumn, boolean nullAssetIDPublic)
    {
        if (!nullAssetIDPublic)
            return null;
        Column columnMeta = Util.useTableAliasIfExists(new Column(fromTable, assetColumn));
        IsNullExpression isNull = new IsNullExpression();
        isNull.setLeftExpression(columnMeta);
        return isNull;
    }

    private Expression metaReleaseControlExpression(Table fromTable, AssetTable at)
    {
        if (at.isView) // views already force the non-null primary key in left-join
            return releaseDateControlExpression(fromTable, at.metaReleaseColumn, null, dateFormat);
        return releaseDateControlExpression(fromTable, at.metaReleaseColumn, at.keyColumn, dateFormat);
    }

    static Expression releaseDateControlExpression(Table fromTable, String releaseDateCol, String assetCol, DateFormat df)
    {
        if (releaseDateCol == null)
            return null;
        // ( alias.metaRelease <  currentTime OR alias.primaryKey is null )
        String strCurrentTime = df.format(new Date());
        Column columnMeta = Util.useTableAliasIfExists(new Column(fromTable, releaseDateCol));

        MinorThan metaReleaseExpr = new MinorThan();
        metaReleaseExpr.setLeftExpression(columnMeta);
        metaReleaseExpr.setRightExpression(new StringValue("'" + strCurrentTime + "'"));

        if (assetCol == null)
            return metaReleaseExpr;

        Column assetMeta = Util.useTableAliasIfExists(new Column(fromTable, assetCol));
        IsNullExpression nullLeftJoin = new IsNullExpression();
        nullLeftJoin.setLeftExpression(assetMeta);

        return new Parenthesis(new OrExpression(metaReleaseExpr, nullLeftJoin));
    }

    static Expression groupAccessControlExpression(Table assetTable, String metaReadAccessColumn, List<String> gids)
    {
        log.debug("[groupAccessControlExpression] " + metaReadAccessColumn + "," + gids);
        if (metaReadAccessColumn == null)
        {
            log.debug("[groupAccessControlExpression] no meta-read-access column");
            return null;
        }

        try
        {
            Expression rtn = null;
            if (!gids.isEmpty())
            {
                Column column = Util.useTableAliasIfExists(new Column(assetTable, metaReadAccessColumn));
                StringBuilder sb = new StringBuilder();
                for (String id : gids)
                {
                    sb.append(id);
                    sb.append("|");
                }
                sb.deleteCharAt(sb.length() -1);
                rtn = new PgTextSearchMatch(column, sb.toString());
            }
            else
            {
                log.debug("[groupAccessControlExpression] no groups");
            }
            return rtn;
        }
        finally
        {

        }
    }
}
