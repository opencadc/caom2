/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.types;


import ca.nrc.cadc.caom2.util.CaomValidator;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pdowler
 */
public class Polygon implements Shape
{
    private static final long serialVersionUID = 201708141600L;

    private final List<Point> points = new ArrayList<>();
    private MultiPolygon samples;
    
    // lazily computed
    private transient Point center;
    private transient Double area;
    private transient Circle minimumSpanningCircle;
    private transient Boolean ccw;
    
    /**
     * Construct new polygon. The input must provide at least 3 points
     * and a valid samples object. 
     * 
     * @param points
     * @param samples 
     */
    public Polygon(List<Point> points, MultiPolygon samples) 
    { 
        CaomValidator.assertNotNull(Polygon.class, "points", points);
        CaomValidator.assertNotNull(Polygon.class, "samples", samples);
        this.points.addAll(points);
        this.samples = samples;
        validate();
    }

    /**
     * Validate this polygon for conformance to IVOA DALI polygon rules.
     */
    public final void validate()
    {
        initProps();
    }
    
    /**
     * Access the coordinates for this polygon. If the coordinate list is modified, the caller
     * must call validate in order to enforce correctness and recompute the center, area, and
     * minimum spanning circle (size).
     * 
     * @return 
     */
    public List<Point> getPoints()
    {
        return points;
    }

    public MultiPolygon getSamples()
    {
        return samples;
    }

    @Override
    public Point getCenter()
    {
        if (center == null)
            initProps();
        return center;
    }

    @Override
    public double getArea()
    {
        if (area == null)
            initProps();
        return area;
    }

    @Override
    public double getSize()
    {
        if (minimumSpanningCircle == null)
            initProps();
        return minimumSpanningCircle.getSize();
    }

    public Circle getMinimumSpanningCircle()
    {
        if (minimumSpanningCircle == null)
            initProps();
        return minimumSpanningCircle;
    }
    
    private void initProps()
    {
        if (points.size() < 3)
            throw new IllegalPolygonException("polygon has " + points.size() + " points: minimum 3");
        
        MultiPolygon mp = new MultiPolygon();
        SegmentType t = SegmentType.MOVE;
        for (Point p : points)
        {
            Vertex v = new Vertex(p.cval1, p.cval2, t);
            mp.getVertices().add(v);
            t = SegmentType.LINE;
        }
        mp.getVertices().add(Vertex.CLOSE);
        
        this.area = mp.getArea();
        this.center = mp.getCenter();
        this.minimumSpanningCircle = mp.getMinimumSpanningCircle();
        this.ccw = mp.getCCW();
        
        // DALI polygons are always CCW so if we detect CW here it is equivalent
        // to the region outside with area = 4*pi - area and larger than half the 
        // sphere
        if (!ccw)
            throw new IllegalPolygonException("polygon too large or has clockwise winding direction");
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(Polygon.class.getSimpleName()).append("[");
        sb.append("points={");
        for (Point p : points)
            sb.append(p).append(",");
        sb.setCharAt(sb.length() - 1, '}'); // replace last comma
        sb.append(",samples=").append(samples);
        sb.append("]");
        return sb.toString();
    }
    
    
}
