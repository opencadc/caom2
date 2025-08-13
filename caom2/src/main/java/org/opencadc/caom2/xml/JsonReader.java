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

import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opencadc.caom2.Algorithm;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.CalibrationLevel;
import org.opencadc.caom2.CaomEntity;
import org.opencadc.caom2.CustomAxis;
import org.opencadc.caom2.DataQuality;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Environment;
import org.opencadc.caom2.Instrument;
import org.opencadc.caom2.Metrics;
import org.opencadc.caom2.Observable;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.ObservationIntentType;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.Polarization;
import org.opencadc.caom2.Position;
import org.opencadc.caom2.Proposal;
import org.opencadc.caom2.Provenance;
import org.opencadc.caom2.Quality;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.Requirements;
import org.opencadc.caom2.SimpleObservation;
import org.opencadc.caom2.Status;
import org.opencadc.caom2.Target;
import org.opencadc.caom2.TargetPosition;
import org.opencadc.caom2.TargetType;
import org.opencadc.caom2.Telescope;
import org.opencadc.caom2.Time;
import org.opencadc.caom2.Visibility;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.vocab.DataProductType;
import org.opencadc.caom2.vocab.Tracking;
import org.opencadc.caom2.vocab.UCD;

/**
 *
 * @author pdowler
 */
public class JsonReader extends JsonUtil implements ObservationInput {
    private static final Logger log = Logger.getLogger(JsonReader.class);

    public JsonReader() { 
    }

    private void init() {
        
    }

    @Override
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

    @Override
    public Observation read(String json)
        throws ObservationParsingException {
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
        String intent = getChildString(root, "intent", false);
        if (intent != null) {
            obs.intent = ObservationIntentType.toValue(intent);
        }
        obs.type = getChildString(root, "type", false);

        final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        
        obs.metaRelease = getChildDate(root, "metaRelease", false, df);
        addURIs(root, "metaReadGroups", obs.getMetaReadGroups());
        
        obs.sequenceNumber = getChildInteger(root, "sequenceNumber", false);
        obs.proposal = getProposal(root);
        obs.target = getTarget(root);
        obs.targetPosition = getTargetPosition(root);
        obs.requirements = getRequirements(root);
        obs.telescope = getTelescope(root);
        obs.instrument = getInstrument(root);
        obs.environment = getEnvironment(root);

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
        
        Date lastModified = getChildDate(o, "@lastModified", false, df);
        CaomUtil.assignLastModified(e, lastModified, "lastModified");
        
        URI metaChecksum = getChildURI(o, "@metaChecksum", false);
        CaomUtil.assignMetaChecksum(e, metaChecksum, "metaChecksum");
        
        Date maxLastModified = getChildDate(o, "@maxLastModified", false, df);
        CaomUtil.assignLastModified(e, maxLastModified, "maxLastModified");
        
        URI accMetaChecksum = getChildURI(o, "@accMetaChecksum", false);
        CaomUtil.assignMetaChecksum(e, accMetaChecksum, "accMetaChecksum");
        
        e.metaProducer = getChildURI(o, "@metaProducer", false);
    }

    private void addPlanes(JSONObject par, Set<Plane> dest, DateFormat df) throws ObservationParsingException {
        if (par.has("planes")) {
            JSONArray a = par.getJSONArray("planes");
            Iterator i = a.iterator();
            while (i.hasNext()) {
                JSONObject o = (JSONObject) i.next();
                URI uri = getChildURI(o, "uri", true);
                Plane p = new Plane(uri);
                // optional fields
                Integer c = getChildInteger(o, "calibrationLevel", false);
                if (c != null) {
                    p.calibrationLevel = CalibrationLevel.toValue(c);
                }
                String s = getChildString(o, "dataProductType", false);
                if (s != null) {
                    p.dataProductType = DataProductType.toValue(s);
                }
                
                p.dataRelease = getChildDate(o, "dataRelease", false, df);
                p.metaRelease = getChildDate(o, "metaRelease", false, df);
                addURIs(o, "dataReadGroups", p.getDataReadGroups());
                addURIs(o, "metaReadGroups", p.getMetaReadGroups());
                
                p.metrics = getMetrics(o);
                p.quality = getDataQuality(o);
                p.provenance = getProvenance(o, df);
                
                p.observable = getObservableAxis(o);
                p.position = getPositionAxis(o);
                p.energy = getEnergyAxis(o);
                p.time = getTimeAxis(o);
                p.polarization = getPolarizationAxis(o);
                p.custom = getCustomAxis(o);
                p.visibility = getVisibility(o);
                
                assignEntityAttributes(o, p, df);
                addArtifacts(o, p.getArtifacts(), df);
                dest.add(p);
            }
        }
    }
    
    private void addArtifacts(JSONObject par, Set<Artifact> dest, DateFormat df) throws ObservationParsingException {
        if (par.has("artifacts")) {
            JSONArray arr = par.getJSONArray("artifacts");
            Iterator i = arr.iterator();
            while (i.hasNext()) {
                JSONObject po = (JSONObject) i.next();
                URI uri = getChildURI(po, "uri", true);
                String pts = getChildString(po, "productType", true);
                DataLinkSemantics sem = DataLinkSemantics.toValue(pts);
                String rts = getChildString(po, "releaseType", true);
                ReleaseType rt = ReleaseType.toValue(rts);
                Artifact a = new Artifact(uri, sem, rt);
                // optional fields
                a.contentChecksum = getChildURI(po, "contentChecksum", false);
                a.contentLength = getChildLong(po, "contentLength", false);
                a.contentRelease = getChildDate(po, "contentRelease", false, df);
                a.contentType = getChildString(po, "contentType", false);
                a.descriptionID = getChildURI(po, "descriptionID", false);
                addURIs(po, "contentReadGroups", a.getContentReadGroups());
                assignEntityAttributes(po, a, df);
                dest.add(a);
            }
        }
    }

    // observation structure
    private Algorithm getAlgorithm(JSONObject obs) throws ObservationParsingException {
        if (obs.has("algorithm")) {
            JSONObject algo = obs.getJSONObject("algorithm");
            String name = getChildString(algo, "name", true);
            return new Algorithm(name);
        }
        return null;
    }
    
    private Telescope getTelescope(JSONObject parent) throws ObservationParsingException {
        if (parent.has("telescope")) {
            JSONObject o = parent.getJSONObject("telescope");
            String name = getChildString(o, "name", true);
            Telescope ret = new Telescope(name);
            ret.geoLocationX = getChildDouble(o, "geoLocationX", false);
            ret.geoLocationY = getChildDouble(o, "geoLocationY", false);
            ret.geoLocationZ = getChildDouble(o, "geoLocationZ", false);
            String s = getChildString(o, "trackingMode", false);
            if (s != null) {
                ret.trackingMode = Tracking.toValue(s);
            }
            addStrings(o, "keywords", ret.getKeywords());
            return ret;
        }
        return null;
    }

    private Instrument getInstrument(JSONObject parent) throws ObservationParsingException {
        if (parent.has("instrument")) {
            JSONObject o = parent.getJSONObject("instrument");
            String name = getChildString(o, "name", true);
            Instrument ret = new Instrument(name);
            addStrings(o, "keywords", ret.getKeywords());
            return ret;
        }
        return null;
    }

    private Proposal getProposal(JSONObject parent) throws ObservationParsingException {
        if (parent.has("proposal")) {
            JSONObject o = parent.getJSONObject("proposal");
            String id = getChildString(o, "id", true);
            Proposal ret = new Proposal(id);
            ret.pi = getChildString(o, "pi", false);
            ret.project = getChildString(o, "project", false);
            ret.reference = getChildURI(o, "reference", false);
            ret.title = getChildString(o, "title", false);
            addStrings(o, "keywords", ret.getKeywords());
            return ret;
        }
        return null;
    }
    
    private Environment getEnvironment(JSONObject parent) throws ObservationParsingException {
        if (parent.has("environment")) {
            JSONObject o = parent.getJSONObject("environment");
            Environment ret = new Environment();
            ret.ambientTemp = getChildDouble(o, "ambientTemp", false);
            ret.elevation = getChildDouble(o, "elevation", false);
            ret.humidity = getChildDouble(o, "humidity", false);
            ret.photometric = getChildBoolean(o, "photometric", false);
            ret.seeing = getChildDouble(o, "seeing", false);
            ret.tau = getChildDouble(o, "tau", false);
            ret.wavelengthTau = getChildDouble(o, "wavelengthTau", false);
            return ret;
        }
        return null;
    }
    
    private Target getTarget(JSONObject parent) throws ObservationParsingException {
        if (parent.has("target")) {
            JSONObject o = parent.getJSONObject("target");
            String name = getChildString(o, "name", true);
            Target ret = new Target(name);
            ret.moving = getChildBoolean(o, "moving", false);
            ret.redshift =  getChildDouble(o, "redshift", false);
            ret.standard = getChildBoolean(o, "standard", false);
            ret.targetID = getChildURI(o, "targetID", false);
            String s = getChildString(o, "type", false);
            if (s != null) {
                ret.type = TargetType.toValue(s);
            }
            addStrings(o, "keywords", ret.getKeywords());
            return ret;
        }
        return null;
    }
    
    private TargetPosition getTargetPosition(JSONObject parent) throws ObservationParsingException {
        if (parent.has("targetPosition")) {
            JSONObject o = parent.getJSONObject("targetPosition");
            String coordsys = getChildString(o, "coordsys", true);
            Point coordinates = getChildPoint(o, "coordinates");
            TargetPosition ret = new TargetPosition(coordsys, coordinates);
            ret.equinox = getChildDouble(o, "equinox", false);
            return ret;
        }
        return null;
    }
    
    private Requirements getRequirements(JSONObject parent) throws ObservationParsingException {
        if (parent.has("requirements")) {
            JSONObject o = parent.getJSONObject("requirements");
            String s = getChildString(o, "flag", true);
            Status status = Status.toValue(s);
            Requirements ret = new Requirements(status);
            return ret;
        }
        return null;
    }

    // plane structure
    private Metrics getMetrics(JSONObject o) throws ObservationParsingException {
        if (o.has("metrics")) {
            JSONObject po = o.getJSONObject("metrics");
            Metrics ret = new Metrics();
            ret.background = getChildDouble(po, "background", false);
            ret.backgroundStddev = getChildDouble(po, "backgroundStddev", false);
            ret.fluxDensityLimit = getChildDouble(po, "fluxDensityLimit", false);
            ret.magLimit = getChildDouble(po, "magLimit", false);
            ret.sampleSNR = getChildDouble(po, "sampleSNR", false);
            ret.sourceNumberDensity = getChildDouble(po, "sourceNumberDensity", false);
            return ret;
        }
        return null;
    }
    
    private DataQuality getDataQuality(JSONObject o) throws ObservationParsingException {
        if (o.has("quality")) {
            JSONObject po = o.getJSONObject("quality");
            String s = getChildString(po, "flag", true);
            Quality q = Quality.toValue(s);
            DataQuality ret = new DataQuality(q);
            return ret;
        }
        return null;
    }
    
    private Provenance getProvenance(JSONObject o, DateFormat df) throws ObservationParsingException {
        if (o.has("provenance")) {
            JSONObject po = o.getJSONObject("provenance");
            String name = getChildString(po, "name", true);
            Provenance ret = new Provenance(name);
            ret.lastExecuted = getChildDate(po, "lastExecuted", false, df);
            ret.producer = getChildString(po, "producer", false);
            ret.project = getChildString(po, "project", false);
            ret.reference = getChildURI(po, "reference", false);
            ret.runID = getChildString(po, "runID", false);
            ret.version = getChildString(po, "version", false);
            addURIs(po, "inputs", ret.getInputs());
            addStrings(po, "keywords", ret.getKeywords());
            return ret;
        }
        return null;
    }
    
    private Observable getObservableAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("observable")) {
            JSONObject po = o.getJSONObject("observable");
            String s = getChildString(po, "ucd", true);
            UCD ucd = new UCD(s);
            Observable ret = new Observable(ucd);
            ret.calibration = getCalibrationStatus(po, "calibration");
            return ret;
        }
        return null;
    }

    private Position getPositionAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("position")) {
            JSONObject po = o.getJSONObject("position");
            Shape bounds = getShape(po, "bounds", true);
            MultiShape samples = getMultiShape(po, "samples", true);
            Position ret = new Position(bounds, samples);
            ret.calibration = getCalibrationStatus(po, "calibration");
            ret.dimension = getDimension2D(po, "dimension");
            ret.maxRecoverableScale = getInterval(po, "maxRecoverableScale", false);
            ret.minBounds = getShape(po, "minBounds", false);
            ret.resolution = getChildDouble(po, "resolution", false);
            ret.resolutionBounds = getInterval(po, "resolutionBounds", false);
            ret.sampleSize = getChildDouble(po, "sampleSize", false);
            return ret;
        }
        return null;
    }

    private Energy getEnergyAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("energy")) {
            JSONObject po = o.getJSONObject("energy");
            Interval<Double> bounds = getInterval(po, "bounds", true);
            Energy ret = new Energy(bounds);
            addIntervals(po, "samples", ret.getSamples());
            ret.bandpassName = getChildString(po, "bandpassName", false);
            ret.calibration = getCalibrationStatus(po, "calibration");
            ret.dimension = getChildLong(po, "dimension", false);
            ret.resolution = getChildDouble(po, "resolution", false);
            ret.resolutionBounds = getInterval(po, "resolutionBounds", false);
            ret.resolvingPower = getChildDouble(po, "resolvingPower", false);
            ret.resolvingPowerBounds = getInterval(po, "resolvingPowerBounds", false);
            ret.rest = getChildDouble(po, "rest", false);
            ret.sampleSize = getChildDouble(po, "sampleSize", false);
            if (po.has("transition")) {
                JSONObject eto = po.getJSONObject("transition");
                String species = getChildString(eto, "species", true);
                String transition = getChildString(eto, "transition", true);
                ret.transition = new EnergyTransition(species, transition);
            }
            addEnergyBands(po, "energyBands", ret.getEnergyBands());
            return ret;
        }
        return null;
    }

    private Time getTimeAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("time")) {
            JSONObject po = o.getJSONObject("time");
            Interval<Double> bounds = getInterval(po, "bounds", true);
            Time ret = new Time(bounds);
            addIntervals(po, "samples", ret.getSamples());
            ret.calibration = getCalibrationStatus(po, "calibration");
            ret.dimension = getChildLong(po, "dimension", false);
            ret.exposure = getChildDouble(po, "exposure", false);
            ret.exposureBounds = getInterval(po, "exposureBounds", false);
            ret.resolution = getChildDouble(po, "resolution", false);
            ret.resolutionBounds = getInterval(po, "resolutionBounds", false);
            ret.sampleSize = getChildDouble(po, "sampleSize", false);
            return ret;
        }
        return null;
    }

    private Polarization getPolarizationAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("polarization")) {
            JSONObject po = o.getJSONObject("polarization");
            Polarization ret = new Polarization();
            addPolStates(po, "states", ret.getStates());
            ret.dimension = getChildInteger(po, "dimension", false);
            return ret;
        }
        return null;
    }

    private CustomAxis getCustomAxis(JSONObject o) throws ObservationParsingException {
        if (o.has("custom")) {
            JSONObject po = o.getJSONObject("custom");
            String ctype = getChildString(po, "ctype", true);
            Interval<Double> bounds = getInterval(po, "bounds", true);
            CustomAxis ret = new CustomAxis(ctype, bounds);
            addIntervals(po, "samples", ret.getSamples());
            ret.dimension = getChildLong(po, "dimension", false);
            return ret;
        }
        return null;
    }

    private Visibility getVisibility(JSONObject o) throws ObservationParsingException {
        if (o.has("visibility")) {
            JSONObject po = o.getJSONObject("visibility");
            Interval<Double> bounds = getInterval(po, "distance", true);
            Double ecc = getChildDouble(po, "distributionEccentricity", true);
            Double fill = getChildDouble(po, "distributionFill", true);
            Visibility ret = new Visibility(bounds, ecc, fill);
            return ret;
        }
        return null;
    }
}
