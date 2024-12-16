/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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
************************************************************************
*/

package ca.nrc.cadc.tap.caom2;


import ca.nrc.cadc.stc.CoordPair;
import ca.nrc.cadc.stc.Frame;
import ca.nrc.cadc.stc.STC;
import ca.nrc.cadc.tap.writer.format.AbstractResultSetFormat;
import ca.nrc.cadc.tap.writer.format.DoubleArrayFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Format a position_bounds value as xtype="region".
 * 
 * @author pdowler
 */
public class PositionBoundsRegionFormat extends AbstractResultSetFormat {
    private static final Logger log = Logger.getLogger(PositionBoundsRegionFormat.class);

    private DoubleArrayFormat daf = new DoubleArrayFormat();
    
    public PositionBoundsRegionFormat() { 
    }

    /**
     * Takes a ResultSet and column index of the position_bounds_points
     * and returns a polymorphic STC-S String.
     *
     * @param resultSet containing the position_bounds_points column.
     * @param columnIndex index of the column in the ResultSet.
     * @return STC Polygon
     * @throws SQLException if there is an error accessing the ResultSet.
     */
    @Override
    public Object extract(ResultSet resultSet, int columnIndex)
            throws SQLException {
        Object o = daf.extract(resultSet, columnIndex);
        return getRegion(o);
    }

    /**
     * Takes a String representation of the spoly
     * and returns a STC-S Polygon String.
     *
     * @param object to format.
     * @return STC-S Polygon String of the spoly.
     * @throws IllegalArgumentException if the object is not a String, or if
     * the String cannot be parsed.
     */
    @Override
    public String format(Object object) {
        if (object == null) {
            return "";
        }
        return STC.format((ca.nrc.cadc.stc.Region) object);
    }

    ca.nrc.cadc.stc.Region getRegion(Object object) {
        if (object == null) {
            return null;
        }

        if (object instanceof java.sql.Array) {
            try {
                java.sql.Array array = (java.sql.Array) object;
                object = array.getArray();
            } catch (SQLException e) {
                throw new IllegalArgumentException("Error accessing array data for " + object.getClass().getCanonicalName(), e);
            }
        }

        if (object instanceof Double[]) {
            Double[] arr = (Double[]) object;
            double[] tmp = new double[arr.length];
            for (int i = 0; i < arr.length; i++) {
                tmp[i] = arr[i]; // unbox
            }
            object = tmp;
        }

        if (object instanceof double[]) {
            double[] coords = (double[]) object;
            if (coords.length == 3) {
                return new ca.nrc.cadc.stc.Circle(Frame.ICRS, null, null, coords[0], coords[1], coords[2]);
            } else {
                List<CoordPair> coordPairs = new ArrayList<CoordPair>();
                for (int i = 0; i < coords.length; i += 2) {
                    coordPairs.add(new CoordPair(coords[i], coords[i + 1]));
                }
                return new ca.nrc.cadc.stc.Polygon(Frame.ICRS, null, null, coordPairs);
            }
        }
        
        throw new IllegalArgumentException(object.getClass().getCanonicalName() + " not supported.");
    }
}
