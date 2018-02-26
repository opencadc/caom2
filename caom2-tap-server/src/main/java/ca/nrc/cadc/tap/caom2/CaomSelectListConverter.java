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

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.log4j.Logger;

/**
 * Supports converting various CAOM columns from the select list into different
 * columns that can deliver the correct output.
 * 
 * @author pdowler
 */
public class CaomSelectListConverter extends SelectNavigator
{
    private static Logger log = Logger.getLogger(CaomSelectListConverter.class);
    
    public CaomSelectListConverter()
    {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
    }

    @Override
    public void visit(PlainSelect ps)
    {
        // not going to recurse, just examine/convert the select list
        List<Table> tabs = ParserUtil.getFromTableList(ps);
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
                        if (f.isAllColumns())
                        {
                            log.debug("found: " + f.getName() + " arg: AllColumns");
                        }
                        else
                        {
                            ExpressionList elist = f.getParameters();
                            if (elist != null && elist.getExpressions() != null && !elist.getExpressions().isEmpty()) {
                                Iterator i = elist.getExpressions().iterator();
                                while ( i.hasNext() )
                                {
                                    Expression arg = (Expression) i.next();
                                    if (arg instanceof Column)
                                    {
                                        Column c = (Column) arg;
                                        fixColumn(c, tabs);
                                    }
                                }
                            }
                        }
                    }
                    else if(e instanceof Column)
                    {
                        Column c = (Column) e;
                        fixColumn(c, tabs);
                    }
                }
            }
        }
    }

    private void fixColumn(Column c, List<Table> tabs)
    {
        Table t = c.getTable();
        if ( !isUploadedTable(t, tabs))
        {
            if (isCAOM2(c, tabs))
            {
                // caom2.Artifact, caom2.SIAv1
                if (c.getColumnName().equalsIgnoreCase("accessURL"))
                    c.setColumnName("uri");
                
                // caom2.Plane -- position_bounds_points is polymorphic (circles and polygons)
                // and position_bounds is not (circle approximated as polygon) so we have to 
                // return the latter in TAP -- slightly lossy
                
                //if (c.getColumnName().equalsIgnoreCase("position_bounds"))
                //    c.setColumnName("position_bounds_points");
                
                // ivoa.ObsCore has STC-S output so we can select the exact polymorphic column
                if (c.getColumnName().equalsIgnoreCase("s_region"))
                    c.setColumnName("position_bounds_points");
            }
            
        }
    }
    
    // this gets called after TableNameConverter does ivoa.ObsCore to caom2.ObsCore
    private boolean isCAOM2(Column c, List<Table> tabs)
    {
        boolean caom2 = false;
        Table t = c.getTable();
        for (Table t2 : tabs)
        {
            log.debug("isCAOM2: " + c + " vs " + t2);
            if ( t2.getSchemaName().equalsIgnoreCase("caom2"))
            {
                caom2 = true;
                if (t != null && t2.getAlias() != null && t2.getAlias().equals(t.getName()))
                    return true;
            }
        }
        return caom2;
    }
                

    private boolean isUploadedTable(Table t, List<Table> tabs)
    {
        for (Table t2 : tabs)
        {
            if ( t2.getSchemaName().equalsIgnoreCase("tap_upload"))
            {
                if (t2.getAlias() != null && t2.getAlias().equals(t.getName()))
                    return true;
            }
        }
        return false;
    }
}
