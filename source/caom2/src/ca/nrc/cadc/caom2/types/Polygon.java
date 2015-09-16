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

package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.util.HexUtil;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pdowler
 */
public class Polygon implements Shape
{
    private static final long serialVersionUID = 201202081100L;
    
    private final List<Vertex> vertices = new ArrayList<Vertex>();

    // lazily computed
    private static int vertexStateCode = -1;
    private transient Point center;
    private transient Double area;
    private transient Double size;

    public Polygon() { }

    private void validate()
    {
        if (vertices.size() < 4) // triangle
            throw new IllegalStateException("invalid polygon: " + vertices.size() + " vertices");

        Vertex end = vertices.get(0);
        if ( !SegmentType.MOVE.equals(end.getType()) )
            throw new IllegalStateException("invalid polygon: first vertex must be MOVE, found " + end);
        end = vertices.get(vertices.size() - 1);
        if ( !SegmentType.CLOSE.equals(end.getType()) )
            throw new IllegalStateException("invalid polygon: last vertex must be CLOSE, found " + end);

        boolean openLoop = false;
        for (Vertex v : vertices)
        {
            if ( SegmentType.MOVE.equals(v.getType()) )
            {
                if (openLoop)
                    throw new IllegalStateException("invalid polygon: found MOVE when loop already open");
                openLoop = true;
            }
            else if (  SegmentType.CLOSE.equals(v.getType()) )
            {
                if (!openLoop)
                    throw new IllegalStateException("invalid polygon: found CLOSE without MOVE");
                openLoop = false;
            }
        }
    }

    public List<Vertex> getVertices()
    {
        return vertices;
    }

    public double getArea()
    {
        if (area == null)
            initProps();
        return area;
    }

    public Point getCenter()
    {
        if (center == null)
            initProps();
        return center;
    }

    public double getSize()
    {
        if (size == null)
            initProps();
        return size;
    }

    public boolean isSimple()
    {
        validate();
        for (int i=1; i<vertices.size() - 1; i++)
            if ( !SegmentType.LINE.equals(vertices.get(i).getType()) )
                return false;
        return true;
    }

    private void initProps()
    {
        validate();
        PolygonUtil.PolygonProperties pp = PolygonUtil.computePolygonProperties(this);
        this.area = pp.area;
        this.center = pp.center;
        this.size = pp.maxVertexSeparation;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Polygon[");
        sb.append(vertices.size());
        sb.append(",");
        for (int i=0; i<vertices.size(); i++)
        {
            sb.append(vertices.get(i));
            sb.append(",");
        }
        initProps();
        sb.append("c=").append(getCenter().cval1).append(",").append(getCenter().cval2).append(",");
        sb.append("a=").append(getArea());
        sb.append("]");
        return sb.toString();
    }


    /**
     * Decode a previously encoded polygon. This method is supplied to aid in
     * recreating a polygon from a previously encoded byte array.
     *
     * @param encoded byte[] of length 4 + 20 * number of vertices
     * @return the polygon
     * @throws IllegalArgumentException if the byte array does not start with Shape.MAGIC_POLYGON
     */
    public static Polygon decode(byte[] encoded)
    {
        int len = (encoded.length - 4 - 1) / 20; // extra 1 for trailing byte
        int magic = HexUtil.toInt(encoded, 0);
        if (magic != Shape.MAGIC_POLYGON)
            throw new IllegalArgumentException("encoded array does not start with Shape.MAGIC_POLYGON");

        Polygon ret = new Polygon();
        for (int i = 0; i < len; i++)
        {
            double x = Double.longBitsToDouble(HexUtil.toLong(encoded, 4 + i * 20));
            double y = Double.longBitsToDouble(HexUtil.toLong(encoded, 4 + i * 20 + 8));
            int s = HexUtil.toInt(encoded, 4 + i * 20 + 16);
            ret.getVertices().add(new Vertex(x, y, SegmentType.toValue(s)));
        }
        return ret;
    }

    /**
     * Encode a polygon as a byte array. This method is supplied to aid in
     * storing the polygon in binary format.
     *
     * @param poly the polygon to encode
     * @return byte[] of length 4 + 20 * number of vertices
     * @throws IllegalArgumentException if the vertex array is null or empty
     */
    public static byte[] encode(Polygon poly)
    {
        if (poly.vertices.isEmpty())
            throw new IllegalArgumentException("vertex array is empty");

        // need 4 bytes for the magic number
        // need 20 bytes per vertex: 4 for the segment type and 8 for each coord
        // extra 1 for trailing byte
        byte[] ret = new byte[4 + 20 * poly.vertices.size() + 1];
        byte[] b = HexUtil.toBytes(Shape.MAGIC_POLYGON);
        System.arraycopy(b, 0, ret, 0, 4);

        for (int i = 0; i < poly.vertices.size(); i++)
        {
            Vertex v = (Vertex) poly.vertices.get(i);
            b = HexUtil.toBytes(Double.doubleToLongBits(v.cval1));
            System.arraycopy(b, 0, ret, 4 + i * 20, 8);
            b = HexUtil.toBytes(Double.doubleToLongBits(v.cval2));
            System.arraycopy(b, 0, ret, 4 + i * 20 + 8, 8);
            b = HexUtil.toBytes(v.getType().getValue());
            System.arraycopy(b, 0, ret, 4 + i * 20 + 16, 4);
        }
        ret[ret.length-1] = (byte) 1; // trailing 1 so some broken DBs don't truncate
        return ret;
    }
}
