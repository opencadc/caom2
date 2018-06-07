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

package ca.nrc.cadc.tap.impl;

import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.postgresql.PgInterval;
import ca.nrc.cadc.tap.BasicUploadManager;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoint;
import ca.nrc.cadc.tap.parser.region.pgsphere.function.Spoly;
import java.sql.SQLException;
import org.postgresql.util.PGobject;

/**
 *
 * @author pdowler
 */
public class UploadManagerImpl extends BasicUploadManager
{
    /**
     * Default maximum number of rows allowed in the UPLOAD VOTable.
     */
    public static final int MAX_UPLOAD_ROWS = 10000;
    
    public UploadManagerImpl()
    {
        super(MAX_UPLOAD_ROWS);
    }
    
    // convert TAP-1.0 (STC-S) geometry values
    
    @Override
    protected Object getPointObject(ca.nrc.cadc.stc.Position pos)
        throws SQLException
    {
        Spoint sval = new Spoint(pos);
        PGobject pgo = new PGobject();
        String str = sval.toVertex();
        pgo.setType("spoint");
        pgo.setValue(str);
        return pgo;
    }

    @Override
    protected Object getRegionObject(ca.nrc.cadc.stc.Region reg)
        throws SQLException
    {
        if (reg instanceof ca.nrc.cadc.stc.Polygon)
        {
            ca.nrc.cadc.stc.Polygon poly = ( ca.nrc.cadc.stc.Polygon) reg;
            Spoly sval = new Spoly(poly);
            PGobject pgo = new PGobject();
            String str = sval.toVertexList();
            pgo.setType("spoly");
            pgo.setValue(str);
            return pgo;
        }
        throw new UnsupportedOperationException("cannot convert a " + reg.getClass().getSimpleName());
    }
    
    // convert DALI-1.1 geometry values

    @Override
    protected Object getPointObject(Point p) throws SQLException
    {
        Spoint sval = new Spoint(p);
        PGobject pgo = new PGobject();
        String str = sval.toVertex();
        pgo.setType("spoint");
        pgo.setValue(str);
        return pgo;
    }
    
    @Override
    protected Object getPolygonObject(Polygon poly) throws SQLException
    {
        Spoly sval = new Spoly(poly);
        PGobject pgo = new PGobject();
        String str = sval.toVertexList();
        pgo.setType("spoly");
        pgo.setValue(str);
        return pgo;
    }

    @Override
    protected Object getIntervalObject(DoubleInterval inter)
    {
        PgInterval gen = new PgInterval();
        return gen.generatePolygon2D(inter);
    }

    @Override
    protected Object getIntervalArrayObject(DoubleInterval[] inter)
    {
        PgInterval gen = new PgInterval();
        return gen.generatePolygon2D(inter);
    }
}
