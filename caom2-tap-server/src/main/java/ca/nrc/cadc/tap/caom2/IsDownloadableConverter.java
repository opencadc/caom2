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
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;

/**
 * Convert a call to isDownloadable(someColumn) to something that makes the value
 * NULL if the caller is not allowed to download the associated data.
 * 
 * @author pdowler
 */
public class IsDownloadableConverter extends SelectNavigator
{
    private static Logger log = Logger.getLogger(IsDownloadableConverter.class);

    private static final String FUNC_NAME = "isDownloadable";

    private static final Map<String,String> DATA_TABLES = new HashMap<String,String>();
    static
    {
        DATA_TABLES.put("caom2.plane", "caom2.PlaneDataReadAccess");
        DATA_TABLES.put("ivoa.obscore", "caom.PlaneDataReadAccess"); // view on caom2.ObsCore
        DATA_TABLES.put("caom2.obscore", "caom2.PlaneDataReadAccess");
    }

    private transient DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    private GroupClient gmsClient;
    
    // testing support
    private void setGMSClient(GroupClient gmsClient) 
    { 
        this.gmsClient = gmsClient;
    }
    
    public IsDownloadableConverter()
    {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
    }
    
    @Override
    public void visit(PlainSelect ps)
    {
        // not going to recurse, just examine/convert the select list
        List<SelectItem> selectItems = ps.getSelectItems();
        if (selectItems != null)
        {
            for (SelectItem s : selectItems)
            {
                if (s instanceof SelectExpressionItem)
                {
                    SelectExpressionItem se = (SelectExpressionItem) s;
                    Expression e = se.getExpression();
                    if (e instanceof Function)
                    {
                        Function f = (Function) e;
                        if (f.getName().equalsIgnoreCase(FUNC_NAME))
                        {
                            Expression funcArg = getSingleFunctionArgument(f.getParameters());
                            if (funcArg instanceof Column)
                            {
                                Column col = (Column) funcArg;
                                String draTab = null;
                                Table tab = col.getTable();
                                String tabName = col.getTable().getWholeTableName();
                                log.debug("raw tabName = " + tabName);
                                if (tabName == null)
                                {
                                    // look in from for a data table
                                    List<Table> fromList = ParserUtil.getFromTableList(ps);
                                    for (Table t : fromList)
                                    {
                                        log.debug("checking " + t.getWholeTableName());
                                        tabName = t.getWholeTableName().toLowerCase();
                                        draTab = DATA_TABLES.get(tabName);
                                        if (draTab != null)
                                        {
                                            tab = t;
                                            log.debug("tabName found in from list = " + tabName);

                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    tab = ParserUtil.findFromTable(ps, tabName);
                                    tabName = tab.getWholeTableName().toLowerCase();
                                    log.debug("tabName from argument = " + tabName);
                                    draTab = DATA_TABLES.get(tabName);
                                }
                                if ( draTab != null )
                                {
                                    Expression assetPublicExpr = CaomReadAccessConverter.releaseDateControlExpression(tab, "dataRelease", null, dateFormat);
                                    List<String> gids = Util.getGroupIDs(gmsClient);
                                    Expression groupControlExpr = CaomReadAccessConverter.groupAccessControlExpression(tab, "dataReadAccessGroups", gids);

                                    Expression accessControlExpr = assetPublicExpr;
                                    if (groupControlExpr != null)
                                        accessControlExpr = new Parenthesis(new OrExpression(assetPublicExpr, groupControlExpr));

                                    List<Expression> whenExpr = new ArrayList<Expression>();
                                    WhenClause wc = new WhenClause();
                                    wc.setWhenExpression(accessControlExpr);
                                    wc.setThenExpression(col);
                                    whenExpr.add(wc);

                                    Expression elseExpr = new NullValue();
                                    CaseExpression dataAccessCase = new CaseExpression();
                                    dataAccessCase.setWhenClauses(whenExpr);
                                    dataAccessCase.setElseExpression(elseExpr);
                                    se.setExpression(new Parenthesis(dataAccessCase));
                                    log.debug("converted isDownloadable to: " + se);
                                }
                                else
                                {
                                    throw new UnsupportedOperationException(FUNC_NAME + " cannot be called with argument: " + funcArg 
                                            + " from table " + tabName);
                                }
                            }
                            else
                            {
                                throw new UnsupportedOperationException(FUNC_NAME + " cannot be called with argument: " + funcArg);
                            }
                        }
                        else
                        {
                            log.debug("function: " + f.getName() + " -- ignoring");
                        }
                    }
                }
            }
        }
    }

    private Expression getSingleFunctionArgument(ExpressionList elist)
    {
        if (elist == null || elist.getExpressions().size() != 1)
            throw new IllegalArgumentException("isDownloadable() requires a single argument");
        return (Expression) elist.getExpressions().get(0);
    }
}
