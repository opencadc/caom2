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

import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.PolygonUtil;
import ca.nrc.cadc.caom2.types.SegmentType;
import ca.nrc.cadc.caom2.types.SubInterval;
import ca.nrc.cadc.caom2.types.Vertex;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class PostgreSQLGenerator extends BaseSQLGenerator
{
    private static final Logger log = Logger.getLogger(PostgreSQLGenerator.class);
    
    public PostgreSQLGenerator(String database, String schema)
    {
        super(database, schema, null, true);
        this.useIntegerForBoolean = true;
        this.persistReadAccessWithAsset = true;
    }

    @Override
    protected String getLimitConstraint(Integer batchSize)
    {
        if (batchSize == null)
            return null;
        return "LIMIT " + batchSize;
    }
    
    @Override
    protected String getUpdateAssetSQL(Class asset, Class ra, boolean add)
    {
        StringBuilder sb = new StringBuilder();
        String col = getReadAccessCol(ra);
        
            
        sb.append("UPDATE ");
        sb.append(getTable(asset));
        sb.append(" SET ").append(col).append(" = ");
        if (add)
        {
            sb.append("(").append(col).append(" || ?)::tsvector");
        }
        else // remove
        {
            sb.append("to_tsvector(regexp_replace(").append(col).append("::text, ?, ''))");
        }
        sb.append(" WHERE ");
        if (PlaneMetaReadAccess.class.equals(ra) && !Plane.class.equals(asset))
            sb.append(getPrimaryKeyColumn(Plane.class)); // HACK: only works because column name is the same in all tables
        else      
            sb.append(getPrimaryKeyColumn(asset));
        sb.append(" = ?");

        return sb.toString();
    }
    
    @Override
    protected String literal(UUID value)
    {
        // backwards compat with Long id valued in main CAOM tables
        if (value.getMostSignificantBits() == 0L)
            return Long.toString(value.getLeastSignificantBits());
        
        // uuid datatype accepts a string with the standard hex string format
        return "'" + value.toString() + "'";
    }

    @Override
    protected void safeSetPoint(StringBuilder sb, PreparedStatement ps, int col, Point val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPoint] in: " + val);
            StringBuilder sval = new StringBuilder();
            sval.append("(");
            sval.append(val.cval1);
            sval.append("d,");
            sval.append(val.cval2);
            sval.append("d)");
            PGobject pgo = new PGobject();
            String spoint = sval.toString();
            pgo.setType("spoint");
            pgo.setValue(spoint);
            ps.setObject(col, pgo);
            if (sb != null)
            {
                sb.append(spoint);
                sb.append(",");
            }
        }
    }

    @Override
    protected void safeSetPolygon(StringBuilder sb, PreparedStatement ps, int col, Polygon val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetPolygon] in: " + val);
            // pg_sphere only supports simple polygons
            Polygon poly = PolygonUtil.getOuterHull(val);
            log.debug("[safeSetPolygon] hull: " + poly);
            if (poly == null)
            {
                ps.setObject(col, null);
                log.warn("failed to compute simple outer hull from " + val);
                if (sb != null)
                    sb.append("null,");
            }
            else
            {
                StringBuilder sval = new StringBuilder();
                sval.append("{");
                for (Vertex v : poly.getVertices())
                {
                    if ( !SegmentType.CLOSE.equals(v.getType()) )
                    {
                        sval.append("(");
                        sval.append(v.cval1);
                        sval.append("d,");
                        sval.append(v.cval2);
                        sval.append("d)");
                        sval.append(",");
                    }
                }
                sval.setCharAt(sval.length()-1, '}'); // replace last comma with closing }
                String spoly = sval.toString();
                PGobject pgo = new PGobject();
                pgo.setType("spoly");
                pgo.setValue(spoly);
                ps.setObject(col, pgo);
                if (sb != null)
                {
                    sb.append(spoly);
                    sb.append(",");
                }
            }
        }
    }
    
    @Override
    protected void safeSetInterval(StringBuilder sb, PreparedStatement ps, int col, Interval val)
        throws SQLException
    {
        if (val == null)
        {
            ps.setObject(col, null);
            if (sb != null)
                sb.append("null,");
        }
        else
        {
            log.debug("[safeSetInterval] in: " + val);
            PGpolygon poly = getPolygon2D(val);
            ps.setObject(col, poly);
            if (sb != null)
            {
                sb.append(poly.getValue());
                sb.append(",");
            }
        }
    }
    
    PGpolygon getPolygon2D(Interval val)
    {
        // a query will be a point or line segment at y=0
        double y1 = -2.0;  // bottom of comb
        double ym = -1.0;  // bottom of teeth
        double y2 = 1.0;   // top of teeth
        
        List<PGpoint> verts = new ArrayList<PGpoint>();
        
        // draw a 2D polygon that looks like a tooth-up-comb with each tooth having x-range that
        // corresponds to one (sub) interval... it is a simple box for an Interval with no sub-samples
        
        verts.add(new PGpoint(val.getLower(), y1));
        verts.add(new PGpoint(val.getUpper(), y1));

        if (val.getSamples().isEmpty())
        {
            verts.add(new PGpoint(val.getUpper(), y2));
            verts.add(new PGpoint(val.getLower(), y2));
        }
        else
        {
            LinkedList<SubInterval> samples = new LinkedList<SubInterval>(val.getSamples());
            Iterator<SubInterval> iter = samples.descendingIterator();
            SubInterval prev = null;
            while ( iter.hasNext() )
            {
                SubInterval si = iter.next();
                if (prev != null)
                {
                    verts.add(new PGpoint(prev.getLower(), ym));
                    verts.add(new PGpoint(si.getUpper(), ym));
                }
                verts.add(new PGpoint(si.getUpper(), y2));
                verts.add(new PGpoint(si.getLower(), y2));
                prev = si;
            }
        }
        
        return new PGpolygon(verts.toArray(new PGpoint[verts.size()]));
    }
}
