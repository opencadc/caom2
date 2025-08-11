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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opencadc.caom2.Algorithm;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.CaomEntity;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.ObservationIntentType;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.SimpleObservation;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.vocab.DataLinkSemantics;

/**
 *
 * @author pdowler
 */
public class JsonReader {
    private static final Logger log = Logger.getLogger(JsonReader.class);

    public JsonReader() { 
    }

    private void init() {
        
    }

    public Observation read(InputStream in)
        throws ObservationParsingException, IOException {
        if (in == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        try {
            return read(StringUtil.readFromInputStream(in, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported");
        }
    }

    public Observation read(String json)
        throws ObservationParsingException, IOException {
        if (json == null) {
            throw new IllegalArgumentException("JSON must not be null");
        }

        init();

        JSONObject root = new JSONObject(json);
        log.debug("obs namespace uri: " + root.getString("@caom2"));

        String collection = root.getString("collection");
        String suri = root.getString("uri");
        String sub = root.getString("uriBucket");

        URI obsURI = null;
        if (suri != null) {
            try {
                obsURI = new URI(suri);
            } catch (URISyntaxException e) {
                String error = "Unable to parse " + suri + " because " + e.getMessage();
            }
        }

        // Algorithm.
        Algorithm algorithm = getAlgorithm(root);

        // Create the Observation.
        Observation obs;
        String simple = "caom2:" + SimpleObservation.class.getSimpleName();
        String derived = "caom2:" + DerivedObservation.class.getSimpleName();
        String tval = root.getString("@type");
        if (simple.equals(tval)) {
            obs = new SimpleObservation(collection, obsURI, algorithm);
        } else if (derived.equals(tval)) {
            obs = new DerivedObservation(collection, obsURI, algorithm);
        } else {
            throw new ObservationParsingException("unexpected observation type: " + tval);
        }
        
        // verify inline vs computed uriBucket
        if (sub != null && !obs.getUriBucket().equals(sub)) {
            throw new ObservationParsingException("Observation.uriBucket: " + sub + " != " + obs.getUriBucket() + " (computed)");
        }

        // Observation children.
        String intent = getChildText(root, "intent", false);
        if (intent != null) {
            obs.intent = ObservationIntentType.toValue(intent);
        }
        obs.type = getChildText(root, "type", false);

        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        
        obs.metaRelease = getChildTextAsDate(root, "metaRelease", false, df);
        addURIs(root, "metaReadGroups", obs.getMetaReadGroups());
        
        obs.sequenceNumber = getChildTextAsInteger(root, "sequenceNumber", false);
        //obs.proposal = getProposal(root);
        //obs.target = getTarget(root);
        //obs.targetPosition = getTargetPosition(root);
        //obs.requirements = getRequirements(root);
        //obs.telescope = getTelescope(root);
        //obs.instrument = getInstrument(root);
        //obs.environment = getEnvironment(root);

        addPlanes(root, obs.getPlanes(), df);

        if (obs instanceof DerivedObservation) {
            addURIs(root, "members", ((DerivedObservation) obs).getMembers());
        }

        assignEntityAttributes(root, obs, df);

        return obs;
    }
    
    private void assignEntityAttributes(JSONObject o, CaomEntity e, DateFormat df) throws ObservationParsingException {
        UUID id = getChildUUID(o, "@id");
        CaomUtil.assignID(e, id);
        
        Date lastModified = getChildTextAsDate(o, "@lastModified", false, df);
        CaomUtil.assignLastModified(e, lastModified, "lastModified");
        
        URI metaChecksum = getChildTextAsURI(o, "@metaChecksum", false);
        CaomUtil.assignMetaChecksum(e, metaChecksum, "metaChecksum");
        
        Date maxLastModified = getChildTextAsDate(o, "@maxLastModified", false, df);
        CaomUtil.assignLastModified(e, maxLastModified, "maxLastModified");
        
        URI accMetaChecksum = getChildTextAsURI(o, "@accMetaChecksum", false);
        CaomUtil.assignMetaChecksum(e, accMetaChecksum, "accMetaChecksum");
    }

    private void addPlanes(JSONObject par, Set<Plane> dest, DateFormat df) throws ObservationParsingException {
        if (par.has("planes")) {
            JSONArray a = par.getJSONArray("planes");
            Iterator i = a.iterator();
            while (i.hasNext()) {
                JSONObject po = (JSONObject) i.next();
                URI uri = getChildTextAsURI(po, "uri", true);
                Plane p = new Plane(uri);
                dest.add(p);
                
                // TODO: optional fields
                
                assignEntityAttributes(po, p, df);
                addArtifacts(po, p.getArtifacts(), df);
            }
        }
    }
    
    private void addArtifacts(JSONObject par, Set<Artifact> dest, DateFormat df) throws ObservationParsingException {
        if (par.has("artifacts")) {
            JSONArray arr = par.getJSONArray("artifacts");
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                JSONObject po = (JSONObject) i.next();
                URI uri = getChildTextAsURI(po, "uri", true);
                String pts = getChildText(po, "productType", true);
                DataLinkSemantics sem = DataLinkSemantics.toValue(pts);
                String rts = getChildText(po, "releaseType", true);
                ReleaseType rt = ReleaseType.toValue(rts);
                Artifact a = new Artifact(uri, sem, rt);
                dest.add(a);
                
                // TODO: optional fields
                
                assignEntityAttributes(po, a, df);
            }
        }
    }
    
    private JSONObject getChildObject(JSONObject parent, String name, boolean required) 
            throws ObservationParsingException {
        JSONObject ret = parent.getJSONObject(name);
        if (required && ret == null) {
            throw new ObservationParsingException("Missing required object: " + name);
        }
        return ret;
    }

    private String getChildText(JSONObject parent, String name, boolean required) 
            throws ObservationParsingException {
        if (parent.has(name)) {
            String s = parent.getString(name);
            if (required && s == null) {
                throw new ObservationParsingException("Missing required value: " + name);
            }
            return s;
        }
        return  null;
    }

    private URI getChildTextAsURI(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        String s = getChildText(parent, name, required);
        if (s == null) {
            return null;
        }
        try {
            return new URI(s);
        } catch (URISyntaxException ex) {
            throw new ObservationParsingException("invalid uri value: " + name + " = " + s);
        }
    }

    private UUID getChildUUID(JSONObject parent, String name)
            throws ObservationParsingException {
        String s = getChildText(parent, name, false);
        if (s == null) {
            return null;
        }
        try {
            return UUID.fromString(s);
        } catch (Exception ex) {
            throw new ObservationParsingException("invalid uuid value: " + name + " = " + s);
        }
    }

    private Date getChildTextAsDate(JSONObject parent, String name, boolean required, DateFormat df)
            throws ObservationParsingException {
        String s = getChildText(parent, name, required);
        if (s == null) {
            return null;
        }
        try {
            return df.parse(s);
        } catch (ParseException ex) {
            throw new ObservationParsingException("invalid timestamp value: " + name + " = " + s);
        }
    }
    
    private Integer getChildTextAsInteger(JSONObject parent, String name, boolean required)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getInt(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid integer value for: " + name);
            }
        }
        return null;
    }

    private Long getChildTextAsLong(JSONObject parent, String name, boolean required, DateFormat df)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getLong(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid long value for: " + name);
            }
        }
        return null;
    }

    private Double getChildTextAsDouble(JSONObject parent, String name, boolean required, DateFormat df)
            throws ObservationParsingException {
        if (parent.has(name)) {
            try {
                return parent.getDouble(name);
            } catch (JSONException ex) {
                throw new ObservationParsingException("invalid double value for: " + name);
            }
        }
        return null;
    }

    private Algorithm getAlgorithm(JSONObject obs) throws ObservationParsingException {
        JSONObject algo = getChildObject(obs, "algorithm", true);
        String name = getChildText(algo, "name", true);
        return new Algorithm(name);
    }
    
    private void addURIs(JSONObject parent, String name, Set<URI> dest)
            throws ObservationParsingException {
        
    }
}
