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

package org.opencadc.caom2.xml;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opencadc.caom2.EnergyBand;
import org.opencadc.caom2.PolarizationState;
import org.opencadc.caom2.vocab.CalibrationStatus;
import org.opencadc.caom2.wcs.Dimension2D;

/**
 * Static methods to extract values from JSONObject.
 * 
 * @author pdowler
 */
abstract class JsonUtil {
    private static final Logger log = Logger.getLogger(JsonUtil.class);

    // for set of strings (keywords)
    static void addStrings(JSONObject parent, String name, Set<String> dest) throws ObservationParsingException {
        if (parent.has(name)) {
            // array of string
            JSONArray arr = parent.getJSONArray(name);
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                String s = (String) i.next();
                dest.add(s);
            }
        }
    }

    // for set of groups, members, inputs
    static void addURIs(JSONObject parent, String name, Set<URI> dest) throws ObservationParsingException {
        if (parent.has(name)) {
            // array of string
            JSONArray arr = parent.getJSONArray(name);
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                String s = (String) i.next();
                try {
                    URI u = new URI(s);
                    dest.add(u);
                } catch (URISyntaxException ex) {
                    throw new ObservationParsingException("invalid uri value: " + name + " = " + s);
                }
            }
        }
    }
    
    static void addEnergyBands(JSONObject parent, String name, Set<EnergyBand> dest) throws ObservationParsingException {
        if (parent.has(name)) {
            // array of string
            JSONArray arr = parent.getJSONArray(name);
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                String s = (String) i.next();
                EnergyBand eb = EnergyBand.toValue(s);
                dest.add(eb);
            }
        }
    }

    static void addPolStates(JSONObject parent, String name, Set<PolarizationState> dest) throws ObservationParsingException {
        if (parent.has(name)) {
            // array of string
            JSONArray arr = parent.getJSONArray(name);
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                String s = (String) i.next();
                PolarizationState eb = PolarizationState.toValue(s);
                dest.add(eb);
            }
        }
    }
    
    static String getChildString(JSONObject parent, String name, boolean required) 
            throws ObservationParsingException {
        if (parent.has(name)) {
            return parent.getString(name);
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return  null;
    }

    static URI getChildURI(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        String s = getChildString(parent, name, required);
        if (s == null) {
            return null;
        }
        try {
            return new URI(s);
        } catch (URISyntaxException ex) {
            throw new ObservationParsingException("invalid uri value: " + name + " = " + s);
        }
    }

    static UUID getChildUUID(JSONObject parent, String name)
            throws ObservationParsingException {
        String s = getChildString(parent, name, false);
        if (s == null) {
            return null;
        }
        try {
            return UUID.fromString(s);
        } catch (Exception ex) {
            throw new ObservationParsingException("invalid uuid value: " + name + " = " + s);
        }
    }

    static Point getChildPoint(JSONObject parent, String name) throws ObservationParsingException {
        if (parent.has(name)) {
            JSONObject o = parent.getJSONObject(name);
            Double cval1 = getChildDouble(o, "cval1", true);
            Double cval2 = getChildDouble(o, "cval2", true);
            return new Point(cval1, cval2);
        }
        return null;
    }

    static Date getChildDate(JSONObject parent, String name, boolean required, DateFormat df)
            throws ObservationParsingException {
        String s = getChildString(parent, name, required);
        if (s == null) {
            return null;
        }
        try {
            return df.parse(s);
        } catch (ParseException ex) {
            throw new ObservationParsingException("invalid timestamp value: " + name + " = " + s);
        }
    }
    
    static Boolean getChildBoolean(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getBoolean(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid boolean value for: " + name);
            }
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }

    static Integer getChildInteger(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getInt(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid integer value for: " + name);
            }
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }

    static Long getChildLong(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getLong(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid long value for: " + name);
            }
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }

    static Double getChildDouble(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getDouble(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid double value for: " + name);
            }
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }

    static CalibrationStatus getCalibrationStatus(JSONObject parent, String name) throws ObservationParsingException {
        String s = getChildString(parent, name, false);
        if (s != null) {
            return CalibrationStatus.toValue(s);
        }
        return null;
    }

    static Interval<Double> getInterval(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            JSONObject o = parent.getJSONObject(name);
            Double lb = getChildDouble(o, "lower", true);
            Double ub = getChildDouble(o, "upper", true);
            return new Interval<>(lb, ub);
        }
        return null;
    }
    
    static void addIntervals(JSONObject parent, String name, List<Interval<Double>> dest)
            throws ObservationParsingException {
        if (parent.has(name)) {
            // array of interval
            JSONArray arr = parent.getJSONArray(name);
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                JSONObject o = (JSONObject) i.next();
                Double lb = getChildDouble(o, "lower", true);
                Double ub = getChildDouble(o, "upper", true);
                Interval<Double> val = new Interval<>(lb, ub);
                dest.add(val);
            }
        }
    }

    private static Shape toShape(JSONObject po, String name) throws ObservationParsingException {
        String type = po.getString("@type");
        if ("caom2:Circle".equalsIgnoreCase(type)) {
            Point c = getChildPoint(po, "center");
            Double r = getChildDouble(po, "radius", true);
            return new Circle(c, r);
        } else if ("caom2:Polygon".equalsIgnoreCase(type)) {
            Polygon ret = new Polygon();
            JSONArray arr = po.getJSONArray("points");
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                JSONObject pp = (JSONObject) i.next();
                Double c1 = getChildDouble(pp, "cval1", true);
                Double c2 = getChildDouble(pp, "cval2", true);
                ret.getVertices().add(new Point(c1, c2));
            }
            return ret;
        } else {
            throw new ObservationParsingException("unexpected @type = " + type + " in " + name);
        }
    }

    static Shape getShape(JSONObject parent, String name, boolean required) throws ObservationParsingException {
        if (parent.has(name)) {
            JSONObject po = parent.getJSONObject(name);
            return toShape(po, name);
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }
    
    static MultiShape getMultiShape(JSONObject parent, String name, boolean required) throws ObservationParsingException {
        if (parent.has(name)) {
            JSONArray arr = parent.getJSONArray(name);
            MultiShape ret = new MultiShape();
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                JSONObject o = (JSONObject) i.next();
                Shape s = toShape(o, name);
                ret.getShapes().add(s);
            }
            return ret;
        }
        if (required) {
            throw new ObservationParsingException("Missing required value: " + name);
        }
        return null;
    }
    
    static Dimension2D getDimension2D(JSONObject parent, String name) throws ObservationParsingException {
        if (parent.has(name)) {
            JSONObject po = parent.getJSONObject(name);
            Long naxis1 = getChildLong(po, "naxis1", true);
            Long naxis2 = getChildLong(po, "naxis2", true);
            return new Dimension2D(naxis1, naxis2);
        }
        return null;
    }
}
