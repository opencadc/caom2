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

import ca.nrc.cadc.tap.caom2.function.Area;
import ca.nrc.cadc.tap.caom2.function.Centroid;
import ca.nrc.cadc.tap.caom2.function.Coordsys;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.region.pgsphere.PgsphereRegionConverter;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Interval;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Point2D;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import org.apache.log4j.Logger;

/**
 * convert predicate functions into CAOM implementation
 * 
 * @author pdowler
 *
 */
public class CaomRegionConverter extends PgsphereRegionConverter
{
    private static Logger log = Logger.getLogger(CaomRegionConverter.class);

    public CaomRegionConverter()
    {
        super(new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator());
    }
    
    /**
     * This method is called when a CONTAINS is found.
     * This could occur if the query had CONTAINS(...) in the select list or as
     * part of an arithmetic expression or aggregate function (since CONTAINS 
     * returns a numeric value). 
     * 
     * @param left 
     * @param right
     */
    @Override
    protected Expression handleContains(Expression left, Expression right)
    {
        if (right instanceof Interval || Interval.class.equals(getColumnType(right)))
        {
            if (left instanceof Column)
            {   
                // TODO
            }
            else if (left instanceof Interval)
            {
                // OK
            }
            else if (left instanceof DoubleValue)
            {
                DoubleValue dv = (DoubleValue) left;
                Point2D p = new Point2D(dv, new DoubleValue("0.0"));
                return super.handleContains(p, right);
            }
            else
                throw new IllegalArgumentException("invalid argument type for contains: " + left.getClass().getSimpleName());
        }
        else if (right instanceof Column)
        {
        }
        return super.handleContains(left, right);
    }
      
    // HACK: this could be determined automatically by looking for the column type in
    // the TapSchema, but support for that should be pushed up into all base visitors
    // from the ca.nrc.cadc.tap.parser.navigator package (refactor to cadcADQL)
    private Class getColumnType(Expression e)
    {
      if (e instanceof Column)
      {
          Column c = (Column) e;
          if (c.getColumnName().equals("energy_bounds")
                  || c.getColumnName().equals("time_bounds"))
              return Interval.class;
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
     */
    @Override
    protected Expression handleIntersects(Expression left, Expression right)
    {
        addCast(left);
        addCast(right);
        return super.handleIntersects(left, right);
    }

    // if the argument is an Spoint column cast it to scircle
    private void addCast(Expression e)
    {
        if (e instanceof Column)
        {
            Column c = (Column) e;
            if (c.getColumnName().equals("position_bounds_center"))
                c.setColumnName("position_bounds_center::scircle");
        }
    }

    // netbeans doesn't like this annotation because handleInterval is not in the
    // direct superclass... seems like a bug
    //@Override
    protected Expression handleInterval(Expression lower, Expression upper)
    {
        return new Interval(lower, upper);
    }
    
    /**
     * In CAOM, CENTROID(position_bounds) -> position_bounds_center
     */
    @Override
    protected Expression handleCentroid(Function adqlFunction)
    {
        log.debug("handleCentroid: " + adqlFunction);
        Centroid centroid = new Centroid(adqlFunction);
        if (centroid.isOnPositionBounds())
        {
            //return centroid;
            return centroid.getExpression();
        }
        else
            throw new UnsupportedOperationException("CENTROID() used with unsupported parameter.");
    }

    /**
     * In CAOM, AREA(position_bounds) -> position_bounds_area
     */
    @Override
    protected Expression handleArea(Function adqlFunction)
    {
        log.debug("handleArea: " + adqlFunction);
        Area area = new Area(adqlFunction);
        if (area.isOnPositionBounds())
            return area.getExpression();
        else
            throw new UnsupportedOperationException("AREA() used with unsupported parameter.");
    }

    /**
     * This method is called when COORDSYS function is found.
     * 
     * @param ex the COORDSYS expression
     */
    @Override
    protected Expression handleCoordSys(Function adqlFunction)
    {
        log.debug("handleCoordSys: " + adqlFunction);
        return new Coordsys(adqlFunction);
    }
    
}
