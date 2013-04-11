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

import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.util.HexUtil;

/**
 *
 * @author pdowler
 */
public class Circle implements Shape
{
    private static final long serialVersionUID = 201202081100L;
    
    private Point center;
    private double radius;

    public Circle(Point center, double radius)
    {
        CaomValidator.assertNotNull(Circle.class, "center", center);
        CaomValidator.assertPositive(Circle.class, "radius", radius);
        this.center = center;
        this.radius = radius;
    }

    public double getArea()
    {
        // TODO: this is cartesian approximation, use spherical geom?
        return Math.PI * radius * radius;
    }

    public Point getCenter()
    {
        return center;
    }

    public double getRadius()
    {
        return radius;
    }

    public double getSize()
    {
        return 2.0 * radius;
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + center + "," + radius + "]";
    }

    public static byte[] encode(Circle c)
    {
        byte[] ret = new byte[29];
        byte[] b = HexUtil.toBytes(MAGIC_CIRCLE);
        System.arraycopy(b, 0, ret, 0, 4);
        b = HexUtil.toBytes(Double.doubleToLongBits(c.center.cval1));
        System.arraycopy(b, 0, ret, 4, 8);
        b = HexUtil.toBytes(Double.doubleToLongBits(c.center.cval2));
        System.arraycopy(b, 0, ret, 12, 8);
        b = HexUtil.toBytes(Double.doubleToLongBits(c.radius));
        System.arraycopy(b, 0, ret, 20, 8);
        ret[ret.length-1] = (byte) 1; // trailing 1 so some broken DBs don't truncate
        return ret;
    }

    public static Circle decode(byte[] encoded)
    {
        int magic = HexUtil.toInt(encoded, 0);
        if (magic != MAGIC_CIRCLE)
            throw new IllegalArgumentException("encoded array does not start with Shape.MAGIC_CIRCLE");
        if (encoded.length != 29)
            throw new IllegalStateException("encoded array is wrong length: " + encoded.length + ", expected 29");

        double x = Double.longBitsToDouble(HexUtil.toLong(encoded, 4));
        double y = Double.longBitsToDouble(HexUtil.toLong(encoded, 12));
        double r = Double.longBitsToDouble(HexUtil.toLong(encoded, 20));
        return new Circle(new Point(x, y), r);
    }
}
