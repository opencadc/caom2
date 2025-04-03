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
************************************************************************
*/

package org.opencadc.caom2.db;

import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Shape;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.wcs.Dimension2D;

/**
 * Utility class to support putting values into and getting values out of a
 * backend relational database.
 * 
 * @author pdowler
 */
public class SQLDialect {
    private static final Logger log = Logger.getLogger(SQLDialect.class);

    private final boolean useIntegerForBoolean;
    
    public SQLDialect(boolean useIntegerForBoolean) {
        this.useIntegerForBoolean = useIntegerForBoolean;
    }

    public String literal(UUID value) {
        throw new UnsupportedOperationException();
    }
    
    public void safeSetLong(StringBuilder sb, PreparedStatement ps, int col, Long val) throws SQLException {
        if (val != null) {
            ps.setLong(col, val);
        } else {
            ps.setNull(col, Types.BIGINT);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetDate(StringBuilder sb, PreparedStatement ps, int col, Date val, Calendar cal) throws SQLException {
        if (val != null) {
            ps.setTimestamp(col, new Timestamp(val.getTime()), cal);
        } else {
            ps.setNull(col, Types.TIMESTAMP);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetBinary(StringBuilder sb, PreparedStatement ps, int col, byte[] val) throws SQLException {
        if (val == null) {
            ps.setBytes(col, val);
            if (sb != null) {
                sb.append("null,");
            }
        } else {
            ps.setNull(col, Types.VARBINARY);
            if (sb != null) {
                sb.append("byte[");
                sb.append(val.length);
                sb.append("],");
            }
        }
    }

    // optimisation to persist group names in a separate field for easier querying
    public void safeSetGroupOptimisation(StringBuilder sb, PreparedStatement ps, int col, Collection<URI> groups) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetArray(StringBuilder sb, PreparedStatement prep, int col, Set<URI> values) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetInterval(StringBuilder sb, PreparedStatement ps, int col, Interval<Double> val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetPoint(StringBuilder sb, PreparedStatement ps, int col, Point val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetURI(StringBuilder sb, PreparedStatement ps, int col, URI val) throws SQLException {
        String str = null;
        if (val != null) {
            str = val.toASCIIString();
        }
        safeSetString(sb, ps, col, str);
    }

    public void safeSetUUID(StringBuilder sb, PreparedStatement ps, int col, UUID val) throws SQLException {
        // null UUID is always a bug
        ps.setObject(col, val);
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetKeywords(StringBuilder sb, PreparedStatement ps, int col, Set<String> vals) throws SQLException {
        // default impl:
        String val = CaomUtil.encodeKeywordList(vals);
        if (val != null) {
            ps.setString(col, val);
        } else {
            ps.setNull(col, Types.VARCHAR);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetShapeAsPolygon(StringBuilder sb, PreparedStatement ps, int col, Shape val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetDimension(StringBuilder sb, PreparedStatement ps, int col, Dimension2D val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetString(StringBuilder sb, PreparedStatement ps, int col, String val) throws SQLException {
        if (val != null) {
            ps.setString(col, val);
        } else {
            ps.setNull(col, Types.VARCHAR);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetDouble(StringBuilder sb, PreparedStatement ps, int col, Double val) throws SQLException {
        if (val != null) {
            ps.setDouble(col, val);
        } else {
            ps.setNull(col, Types.DOUBLE);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetMultiShape(StringBuilder sb, PreparedStatement ps, int col, MultiShape val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetInteger(StringBuilder sb, PreparedStatement ps, int col, Integer val) throws SQLException {
        if (val != null) {
            ps.setLong(col, val);
        } else {
            ps.setNull(col, Types.INTEGER);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetIntervalList(StringBuilder sb, PreparedStatement ps, int col, List<Interval<Double>> val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void safeSetBoolean(StringBuilder sb, PreparedStatement ps, int col, Boolean val) throws SQLException {
        if (useIntegerForBoolean) {
            Integer ival = null;
            if (val != null) {
                if (val.booleanValue()) {
                    ival = new Integer(1);
                } else {
                    ival = new Integer(0);
                }
            }
            safeSetInteger(sb, ps, col, ival);
            return;
        }
        if (val != null) {
            ps.setBoolean(col, val);
        } else {
            ps.setNull(col, Types.BOOLEAN);
        }
        if (sb != null) {
            sb.append(val);
            sb.append(",");
        }
    }

    public void safeSetShape(StringBuilder sb, PreparedStatement ps, int col, Shape val) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void extractIntervalList(ResultSet rc, int col, List<Interval<Double>> vals) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Interval<Double> getInterval(ResultSet rs, int col) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void getKeywords(ResultSet rs, int col, Set<String> keywords) throws SQLException {
        CaomUtil.decodeKeywordList(rs.getString(col), keywords);
    }

    public MultiShape getMultiShape(ResultSet rs, int col) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public List<Interval<Double>> getIntervalList(ResultSet rs, int col) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void extractMultiURI(ResultSet rs, int col, Set<URI> out) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public final UUID getUUID(ResultSet rs, int col) throws SQLException {
        throw new UnsupportedOperationException();
    }
    
    public Dimension2D getDimension(ResultSet rs, int col)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Point getPoint(ResultSet rs, int col)
            throws SQLException {
        throw new UnsupportedOperationException();
    }
    
    public Shape getShape(ResultSet rc, int col)
            throws SQLException {
        throw new UnsupportedOperationException();
    }
}
