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

package ca.nrc.cadc.caom2.util;

import ca.nrc.cadc.caom2.AbstractCaomEntity;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CaomUtil implements Serializable
{
    // this class is serialzable because it is non-final (TBD, see below)
	private static final long serialVersionUID = 201401101400L;
    
    private static Logger log = Logger.getLogger(CaomUtil.class);
    
    static final String STRING_SET_SEPARATOR = " ";
    static final String POL_STATE_SEPARATOR = "/"; // IVOA ObsCore-1.0 Data Model, B.6.6
    
    // other projects can subclass this so all Util their methods are in a single place
    // TODO: decide if this is a good idea or not and refactor if necessary
    protected CaomUtil() { }
    
    // methods to assign to private field in AbstractCaomEntity
    public static void assignID(Object ce, Long id)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField("id");
            f.setAccessible(true);
            f.set(ce, id);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }

    public static void assignLastModified(Object ce, Date d, String fieldName)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(ce, d);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }

    public static Date getLastModified(Object ce)
    {
        return getLastModified(ce, "lastModified");
    }
    
    public static Date getLastModified(Object ce, String fieldName)
    {
        try
        {
            Field f = AbstractCaomEntity.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            return (Date) f.get(ce);
        }
        catch(NoSuchFieldException fex) { throw new RuntimeException("BUG", fex); }
        catch(IllegalAccessException bug) { throw new RuntimeException("BUG", bug); }
    }
    
    // IVOA ObsCore-1.0 Data Model, B.6.6
    public static String encodeStates(List<PolarizationState> states)
    {
        if (states == null || states.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        
        // sort into canonical order
        List<PolarizationState> tmp = new ArrayList<PolarizationState>(states.size());
        tmp.addAll(states);
        Collections.sort(tmp, new PolarizationState.PolStateComparator());
        
        Iterator<PolarizationState> i = tmp.iterator();
        sb.append(POL_STATE_SEPARATOR); // leading
        while ( i.hasNext() )
        {
            PolarizationState s = i.next();
            sb.append(s.stringValue());
            sb.append(POL_STATE_SEPARATOR); // trailing
        }
        return sb.toString();
    }
    
    public static String encodeListString(List<String> strs)
    {
        if (strs == null || strs.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<String> i = strs.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodeListString(String val, List<String> out)
    {
        if (val == null)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
        {
            out.add(s);
        }
    }

    public static String encodeObservationURIs(Set<ObservationURI> set)
    {
        if (set.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<ObservationURI> i = set.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next().getURI().toASCIIString());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodeObservationURIs(String val, Set<ObservationURI> out)
        throws SQLException
    {
        if (val == null)
            return;
        val = val.trim();
        if (val.length() == 0)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
        {
            if (s.length() > 0)
                try
                {
                    URI uri = new URI(s);
                    ObservationURI puri = new ObservationURI(uri);
                    out.add(puri);
                }
                catch(URISyntaxException ex)
                {
                    throw new RuntimeException("failed to decode URI: " + s, ex);
                }
        }
    }
    public static String encodePlaneURIs(Set<PlaneURI> set)
    {
        if (set.isEmpty())
           return null;
        StringBuilder sb = new StringBuilder();
        Iterator<PlaneURI> i = set.iterator();
        while ( i.hasNext() )
        {
            sb.append(i.next().getURI().toASCIIString());
            if ( i.hasNext() )
                sb.append(STRING_SET_SEPARATOR);
        }
        return sb.toString();
    }
    public static void decodePlaneURIs(String val, Set<PlaneURI> out)
        throws SQLException
    {
        if (val == null)
            return;
        val = val.trim();
        if (val.length() == 0)
            return;
        String[] ss = val.split(STRING_SET_SEPARATOR);
        for (String s : ss)
        {
            try
            {
                URI uri = new URI(s);
                PlaneURI puri = new PlaneURI(uri);
                out.add(puri);
            }
            catch(URISyntaxException ex)
            {
                throw new RuntimeException("failed to decode URI: " + s, ex);
            }
        }
    }

    public static String encodeCoordRange1D(CoordRange1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getStart().pix);
        sb.append("/");
        sb.append(cr.getStart().val);
        sb.append("/");
        sb.append(cr.getEnd().pix);
        sb.append("/");
        sb.append(cr.getEnd().val);
        return sb.toString();
    }
    public static CoordRange1D decodeCoordRange1D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            RefCoord c1 = new RefCoord(Double.parseDouble(c[0]), Double.parseDouble(c[1]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            return new CoordRange1D(c1, c2);
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }
    public static String encodeCoordBounds1D(CoordBounds1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        Iterator<CoordRange1D> i = cr.getSamples().iterator();
        while ( i.hasNext() )
        {
            String s = encodeCoordRange1D(i.next());
            sb.append(s);
            if ( i.hasNext() )
                sb.append(",");
        }
        return sb.toString();
    }
    public static CoordBounds1D decodeCoordBounds1D(String s)
    {
        if (s == null)
            return null;
        s = s.trim();
        
        CoordBounds1D ret = new CoordBounds1D();
        if (s.length() == 0)
            return ret; // empty sample list
        
        String[] c = s.split(",");
        for (String r : c)
        {
            CoordRange1D cr = decodeCoordRange1D(r);
            ret.getSamples().add(cr);
        }
        return ret;
    }
    public static String encodeCoordFunction1D(CoordFunction1D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getNaxis());
        sb.append("/");
        sb.append(cr.getDelta());
        sb.append("/");
        sb.append(cr.getRefCoord().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().val);
        sb.append("/");
        return sb.toString();
    }
    public static CoordFunction1D decodeCoordFunction1D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            Long naxis = Long.parseLong(c[0]);
            Double delta = Double.parseDouble(c[1]);
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            return new CoordFunction1D(naxis, delta, c2);
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }

    public static String encodeCoordRange2D(CoordRange2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getStart().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getStart().getCoord1().val);
        sb.append("/");
        sb.append(cr.getStart().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getStart().getCoord2().val);
        sb.append("/");
        sb.append(cr.getEnd().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getEnd().getCoord1().val);
        sb.append("/");
        sb.append(cr.getEnd().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getEnd().getCoord2().val);
        return sb.toString();
    }
    public static CoordRange2D decodeCoordRange2D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            RefCoord c1 = new RefCoord(Double.parseDouble(c[0]), Double.parseDouble(c[1]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            RefCoord c3 = new RefCoord(Double.parseDouble(c[4]), Double.parseDouble(c[5]));
            RefCoord c4 = new RefCoord(Double.parseDouble(c[6]), Double.parseDouble(c[7]));
            return new CoordRange2D(new Coord2D(c1, c2), new Coord2D(c3, c4));
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordRange1D from " + s, bug);
        }
    }
    public static String encodeCoordBounds2D(CoordBounds2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        if (cr instanceof CoordCircle2D)
        {
            CoordCircle2D circ = (CoordCircle2D) cr;
            sb.append("C/");
            sb.append(circ.getCenter().coord1);
            sb.append("/");
            sb.append(circ.getCenter().coord2);
            sb.append("/");
            sb.append(circ.getRadius());
        }
        else
        {
            CoordPolygon2D poly = (CoordPolygon2D) cr;
            sb.append("P/");
            Iterator<ValueCoord2D> i = poly.getVertices().iterator();
            while ( i.hasNext() )
            {
                ValueCoord2D c = i.next();
                sb.append(c.coord1);
                sb.append(",");
                sb.append(c.coord2);
                if (i.hasNext())
                    sb.append("/");
            }
        }
        return sb.toString();
    }
    public static CoordBounds2D decodeCoordBounds2D(String s)
    {
        if (s == null)
            return null;
        s = s.trim();

        String[] c = s.split("/");

        if ("C".equals(c[0]))
        {
            try
            {
                double c1 = Double.parseDouble(c[1]);
                double c2 = Double.parseDouble(c[2]);
                double rad = Double.parseDouble(c[3]);
                return new CoordCircle2D(new ValueCoord2D(c1, c2), rad);
            }
            catch(NumberFormatException bug)
            {
                throw new RuntimeException("BUG: failed to decode CoordCircle2D from " + s, bug);
            }
        }
        if ("P".equals(c[0]))
        {
            CoordPolygon2D poly = new CoordPolygon2D();
            for (int i=1; i<c.length; i++)
            {
                String[] cc = c[i].split(",");
                try
                {
                    double c1 = Double.parseDouble(cc[0]);
                    double c2 = Double.parseDouble(cc[1]);
                    poly.getVertices().add(new ValueCoord2D(c1, c2));
                }
                catch(Exception bug)
                {
                    throw new RuntimeException("BUG: failed to decode CoordPolygon2D from " + s, bug);
                }
            }
            return poly;
        }
        throw new RuntimeException("BUG: failed to decode CoordBounds2D from " + s);
    }
    public static String encodeCoordFunction2D(CoordFunction2D cr)
    {
        if (cr == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(cr.getDimension().naxis1);
        sb.append("/");
        sb.append(cr.getDimension().naxis2);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord1().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord1().val);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord2().pix);
        sb.append("/");
        sb.append(cr.getRefCoord().getCoord2().val);
        sb.append("/");
        sb.append(cr.getCd11());
        sb.append("/");
        sb.append(cr.getCd12());
        sb.append("/");
        sb.append(cr.getCd21());
        sb.append("/");
        sb.append(cr.getCd22());

        return sb.toString();
    }
    public static CoordFunction2D decodeCoordFunction2D(String s)
    {
        if (s == null)
            return null;
        String[] c = s.split("/");
        try
        {
            Dimension2D dim = new Dimension2D(Long.parseLong(c[0]), Long.parseLong(c[1]));
            RefCoord c1 = new RefCoord(Double.parseDouble(c[2]), Double.parseDouble(c[3]));
            RefCoord c2 = new RefCoord(Double.parseDouble(c[4]), Double.parseDouble(c[5]));
            Coord2D rc = new Coord2D(c1, c2);
            return new CoordFunction2D(dim, rc,
                    Double.parseDouble(c[6]), Double.parseDouble(c[7]),
                    Double.parseDouble(c[8]), Double.parseDouble(c[9]));
        }
        catch(NumberFormatException bug)
        {
            throw new RuntimeException("BUG: failed to decode CoordFunction2D from " + s, bug);
        }

    }
}
