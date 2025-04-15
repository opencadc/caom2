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

import ca.nrc.cadc.tap.parser.ParserUtil;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.pgsphere.PgsphereRegionConverter;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Interval;
import ca.nrc.cadc.tap.parser.schema.TapSchemaUtil;
import ca.nrc.cadc.tap.schema.ColumnDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.schema.TapSchema;
import java.util.List;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.log4j.Logger;
import org.opencadc.argus.tap.query.functions.Area;
import org.opencadc.argus.tap.query.functions.Centroid;
import org.opencadc.argus.tap.query.functions.Coordsys;

/**
 * convert predicate functions into CAOM implementation
 *
 * @author pdowler
 *
 */
public class CaomRegionConverter extends PgsphereRegionConverter {

    private static Logger log = Logger.getLogger(CaomRegionConverter.class);

    private final TapSchema tapSchema;

    public CaomRegionConverter(TapSchema tapSchema) {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
        this.tapSchema = tapSchema;
    }

    /**
     * This method is called when a CONTAINS is found.
     * This could occur if the query had CONTAINS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since CONTAINS
     * returns a numeric value).
     *
     * @param left
     * @param right
     * @return replacement expression
     */
    @Override
    protected Expression handleContains(Expression left, Expression right) {
        log.debug("handleContains: " + left + " " + right);
        List<Table> tabs = ParserUtil.getFromTableList(super.getPlainSelect());
        boolean toIntersect = false;
        // CONTAINS(<number>, <interval>)
        TapDataType tdt = getColumnType(tabs, right);
        boolean rhsIntervalCol = (tdt != null && "interval".equals(tdt.xtype));
        log.debug("rhs: " + tdt + " " + rhsIntervalCol);
        
        if (right instanceof Interval || rhsIntervalCol) {
            if (left instanceof Column) {
                // OK
            } else if (left instanceof Interval) {
                // OK
            } else if (left instanceof DoubleValue) {
                DoubleValue dv = (DoubleValue) left;
                //Point2D p = new Point2D(dv, new DoubleValue("0.0"));
                double d = dv.getValue();
                double d1 = Double.longBitsToDouble(Double.doubleToLongBits(d) - 1L);
                double d2 = Double.longBitsToDouble(Double.doubleToLongBits(d) + 1L);
                left = new Interval(new DoubleValue(Double.toString(d1)), new DoubleValue(Double.toString(d2)));
                toIntersect = true;
            } else {
                throw new IllegalArgumentException("invalid argument type for contains: " + left.getClass().getSimpleName());
            }
        }

        // column renaming
        log.debug("left: " + left.getClass().getName());
        if (left instanceof Column) {
            Column c = (Column) left;
            Table t = c.getTable();
            if (t == null || t.getName() == null) {
                log.debug("c.getTable: " + t + " ... searching TapSchema");
                t = Util.findTableWithColumn(c, tabs, tapSchema);
            }
            if (!Util.isUploadedTable(t, tabs) && Util.isCAOM2(t, tabs)) {
                renameColumnToInternal(c);
            }
        }
        log.debug("right: " + right.getClass().getName());
        if (right instanceof Column) {
            Column c = (Column) right;
            Table t = c.getTable();
            if (t == null || t.getName() == null) {
                log.debug("c.getTable: " + t + " ... searching TapSchema");
                t = Util.findTableWithColumn(c, tabs, tapSchema);
            }
            if (!Util.isUploadedTable(t, tabs) && Util.isCAOM2(t, tabs)) {
                renameColumnToInternal(c);
            }
        }

        if (toIntersect) {
            return super.handleIntersects(left, right);
        }
        return super.handleContains(left, right);
    }

    private TapDataType getColumnType(List<Table> tabs, Expression e) {
        if (e instanceof Column) {
            Column c = (Column) e;
            Table t = c.getTable();
            if (t == null || t.getName() == null) {
                log.debug("c.getTable: " + t + " ... searching TapSchema");
                t = Util.findTableWithColumn(c, tabs, tapSchema);
            }
            TableDesc td = TapSchemaUtil.findTableDesc(tapSchema, t);
            if (td != null) {
                ColumnDesc cd = td.getColumn(c.getColumnName());
                if (cd != null && cd.getColumnName().equalsIgnoreCase(c.getColumnName())) {
                    return cd.getDatatype();
                } else {
                    log.debug("column not found: " + c.getColumnName() + " in " + c.getTable());
                }
            }
            log.debug("column not found: " + c.getColumnName() + " in caom2 schema");
        }
        return null; // unknown
    }

    /**
     * This method is called when a INTERSECTS is found.
     * This could occur if the query had INTERSECTS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since INTERSECTS
     * returns a numeric value).
     *
     * @param left
     * @param right
     *
     * @return replacement expression
     */
    @Override
    protected Expression handleIntersects(Expression left, Expression right) {
        log.debug("handleIntersects: " + left + " " + right);
        // column renaming
        List<Table> tabs = ParserUtil.getFromTableList(super.getPlainSelect());
        if (left instanceof Column) {
            Column c = (Column) left;
            Table t = c.getTable();
            if (t == null || t.getName() == null) {
                log.debug("c.getTable: " + t + " ... searching TapSchema");
                t = Util.findTableWithColumn(c, tabs, tapSchema);
            }
            if (!Util.isUploadedTable(t, tabs) && Util.isCAOM2(t, tabs)) {
                renameColumnToInternal(c);
            }
        }
        if (right instanceof Column) {
            Column c = (Column) right;
            Table t = c.getTable();
            if (t == null || t.getName() == null) {
                log.debug("c.getTable: " + t + " ... searching TapSchema");
                t = Util.findTableWithColumn(c, tabs, tapSchema);
            }
            if (!Util.isUploadedTable(t, tabs) && Util.isCAOM2(t, tabs)) {
                renameColumnToInternal(c);
            }
        }

        addCast(left);
        addCast(right);
        return super.handleIntersects(left, right);
    }
    
    private void renameColumnToInternal(Column c) {
        String orig = c.getColumnName();
        if (c.getColumnName().equalsIgnoreCase("targetPosition_coordinates")) {
            c.setColumnName("_q_targetPosition_coordinates");
        } else if (c.getColumnName().equalsIgnoreCase("position_bounds") 
                || c.getColumnName().equalsIgnoreCase("position_samples")
                || c.getColumnName().equalsIgnoreCase("s_region")) {
            c.setColumnName("_q_position_bounds");
        } else if (c.getColumnName().equalsIgnoreCase("position_minBounds")) {
            c.setColumnName("_q_position_minBounds");
        } else if (c.getColumnName().equalsIgnoreCase("energy_bounds")) {
            c.setColumnName("_q_energy_bounds");
        } else if (c.getColumnName().equalsIgnoreCase("energy_samples")) {
            c.setColumnName("_q_energy_samples");
        } else if (c.getColumnName().equalsIgnoreCase("time_bounds")) {
            c.setColumnName("_q_time_bounds");
        } else if (c.getColumnName().equalsIgnoreCase("time_samples")) {
            c.setColumnName("_q_time_samples");
        }
        // TODO: more interval columns
    }

    // if the argument is an Spoint column cast it to scircle
    private void addCast(Expression e) {
        if (e instanceof Column) {
            Column c = (Column) e;
            if (c.getColumnName().equalsIgnoreCase("_q_position_bounds_centroid")) {
                c.setColumnName("_q_position_bounds_centroid::scircle");
            } else if (c.getColumnName().equalsIgnoreCase("_q_position_minBounds_centroid")) {
                c.setColumnName("_q_position_minBounds_centroid::scircle");
            } else if (c.getColumnName().equalsIgnoreCase("_q_targetPosition_coordinates")) {
                c.setColumnName("_q_targetPosition_coordinates::scircle");
            }
        }
    }

    @Override
    protected Expression handleInterval(Expression lower, Expression upper) {
        return new Interval(lower, upper);
    }

    /**
     * In CAOM, CENTROID(position_bounds) is converted to position_bounds_center
     *
     * @param adqlFunction
     * @return replacement expression
     */
    @Override
    protected Expression handleCentroid(Function adqlFunction) {
        log.debug("handleCentroid: " + adqlFunction);
        Centroid centroid = new Centroid(adqlFunction);
        if (centroid.isValidArg()) {
            return centroid.getExpression();
        } else {
            throw new UnsupportedOperationException("CENTROID() used with unsupported argument");
        }
    }

    /**
     * In CAOM, AREA(position_bounds) is converted to position_bounds_area
     *
     * @param adqlFunction
     * @return replacement expression
     */
    @Override
    protected Expression handleArea(Function adqlFunction) {
        log.debug("handleArea: " + adqlFunction);
        Area area = new Area(adqlFunction);
        if (area.isValidArg()) {
            return area.getExpression();
        } else {
            throw new UnsupportedOperationException("AREA() used with unsupported argument");
        }
    }

    /**
     * This method is called when COORDSYS function is found.
     *
     * @param adqlFunction the COORDSYS expression
     * @return replacement expression
     */
    @Override
    protected Expression handleCoordSys(Function adqlFunction) {
        log.debug("handleCoordSys: " + adqlFunction);
        return new Coordsys(adqlFunction);
    }
}
