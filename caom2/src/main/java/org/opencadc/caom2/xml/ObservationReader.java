/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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
*  $Revision: 4 $
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
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.DataConversionException;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.opencadc.caom2.Algorithm;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.CalibrationLevel;
import org.opencadc.caom2.CaomEntity;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.CustomAxis;
import org.opencadc.caom2.DataQuality;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyBand;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Environment;
import org.opencadc.caom2.Instrument;
import org.opencadc.caom2.Metrics;
import org.opencadc.caom2.Observable;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.ObservationIntentType;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.Polarization;
import org.opencadc.caom2.PolarizationState;
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
import org.opencadc.caom2.vocab.CalibrationStatus;
import org.opencadc.caom2.vocab.DataLinkSemantics;
import org.opencadc.caom2.vocab.DataProductType;
import org.opencadc.caom2.vocab.Tracking;
import org.opencadc.caom2.vocab.UCD;
import org.opencadc.caom2.wcs.Axis;
import org.opencadc.caom2.wcs.Coord2D;
import org.opencadc.caom2.wcs.CoordAxis1D;
import org.opencadc.caom2.wcs.CoordAxis2D;
import org.opencadc.caom2.wcs.CoordBounds1D;
import org.opencadc.caom2.wcs.CoordBounds2D;
import org.opencadc.caom2.wcs.CoordCircle2D;
import org.opencadc.caom2.wcs.CoordError;
import org.opencadc.caom2.wcs.CoordFunction1D;
import org.opencadc.caom2.wcs.CoordFunction2D;
import org.opencadc.caom2.wcs.CoordPolygon2D;
import org.opencadc.caom2.wcs.CoordRange1D;
import org.opencadc.caom2.wcs.CoordRange2D;
import org.opencadc.caom2.wcs.CustomWCS;
import org.opencadc.caom2.wcs.Dimension2D;
import org.opencadc.caom2.wcs.ObservableAxis;
import org.opencadc.caom2.wcs.PolarizationWCS;
import org.opencadc.caom2.wcs.RefCoord;
import org.opencadc.caom2.wcs.Slice;
import org.opencadc.caom2.wcs.SpatialWCS;
import org.opencadc.caom2.wcs.SpectralWCS;
import org.opencadc.caom2.wcs.TemporalWCS;
import org.opencadc.caom2.wcs.ValueCoord2D;

/**
 *
 * @author jburke
 */
public class ObservationReader {
    //private static final String CAOM20_SCHEMA_RESOURCE = "CAOM-2.0.xsd";
    //private static final String CAOM21_SCHEMA_RESOURCE = "CAOM-2.1.xsd";
    //private static final String CAOM22_SCHEMA_RESOURCE = "CAOM-2.2.xsd";
    //private static final String CAOM23_SCHEMA_RESOURCE = "CAOM-2.3.xsd";
    private static final String CAOM24_SCHEMA_RESOURCE = "CAOM-2.4.xsd";
    private static final String CAOM25_SCHEMA_RESOURCE = "CAOM-2.5.xsd";
    private static final int CURRENT_CAOM2_SCHEMA_LEVEL = 25;

    private static final String XLINK_SCHEMA_RESOURCE = "XLINK.xsd";

    private static final Logger log = Logger.getLogger(ObservationReader.class);

    protected boolean enableSchemaValidation;
    protected Map<String, String> schemaMap;
    protected Namespace xsiNamespace;

    private transient boolean initDone = false;

    /**
     * Constructor. XML Schema validation is enabled by default.
     */
    public ObservationReader() {
        this(true);
    }

    /**
     * Constructor. XML schema validation may be disabled, in which case the
     * client is likely to fail in horrible ways (e.g. NullPointerException) if
     * it receives invalid documents. However, performance may be improved.
     *
     * @param enableSchemaValidation
     */
    public ObservationReader(boolean enableSchemaValidation) {
        this.enableSchemaValidation = enableSchemaValidation;
    }

    private void init() {
        if (!initDone) {
            if (enableSchemaValidation) {
                /*
                String caom2SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM20_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.0 schema URL: " + caom2SchemaUrl);

                String caom21SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM21_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.1 schema URL: " + caom21SchemaUrl);

                String caom22SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM22_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.2 schema URL: " + caom22SchemaUrl);
              
                String caom23SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM23_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.3 schema URL: " + caom23SchemaUrl);
                */
                String caom24SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM24_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.4 schema URL: " + caom24SchemaUrl);
                
                String caom25SchemaUrl = XmlUtil.getResourceUrlString(
                        CAOM25_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("caom-2.5 schema URL: " + caom25SchemaUrl);

                String xlinkSchemaUrl = XmlUtil.getResourceUrlString(
                        XLINK_SCHEMA_RESOURCE, ObservationReader.class);
                log.debug("xlinkSchemaUrl: " + xlinkSchemaUrl);

                /*
                if (caom2SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM20_SCHEMA_RESOURCE + " from classpath");
                }
                if (caom21SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM21_SCHEMA_RESOURCE + " from classpath");
                }
                if (caom22SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM22_SCHEMA_RESOURCE + " from classpath");
                }
                if (caom23SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM23_SCHEMA_RESOURCE + " from classpath");
                }
                */
                if (caom24SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM24_SCHEMA_RESOURCE + " from classpath");
                }
                if (caom25SchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + CAOM25_SCHEMA_RESOURCE + " from classpath");
                }
                if (xlinkSchemaUrl == null) {
                    throw new RuntimeException("failed to load "
                            + XLINK_SCHEMA_RESOURCE + " from classpath");
                }

                schemaMap = new HashMap<String, String>();
                //schemaMap.put(XmlConstants.CAOM2_0_NAMESPACE, caom2SchemaUrl);
                //schemaMap.put(XmlConstants.CAOM2_1_NAMESPACE, caom21SchemaUrl);
                //schemaMap.put(XmlConstants.CAOM2_2_NAMESPACE, caom22SchemaUrl);
                //schemaMap.put(XmlConstants.CAOM2_3_NAMESPACE, caom23SchemaUrl);
                schemaMap.put(XmlConstants.CAOM2_4_NAMESPACE, caom24SchemaUrl);
                schemaMap.put(XmlConstants.CAOM2_5_NAMESPACE, caom25SchemaUrl);
                schemaMap.put(XmlConstants.XLINK_NAMESPACE, xlinkSchemaUrl);
                log.debug("schema validation enabled");
            } else {
                log.debug("schema validation disabled");
            }

            xsiNamespace = Namespace.getNamespace("xsi",
                    XmlConstants.XMLSCHEMA);
            this.initDone = true;
        }
    }

    private class ReadContext {
        DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        int docVersion = CURRENT_CAOM2_SCHEMA_LEVEL;

        // allow for missing milliseconds in timestamps
        Date parseTimestamp(String value) throws ParseException {
            try {
                return dateFormat.parse(value);
            } catch (ParseException pex1) {
                try {
                    // append missing milliseconds?
                    return dateFormat.parse(value + ".000");
                } catch (ParseException pex2) {
                    throw pex1; // original
                }
            } finally {
                // nothing to do
            }
        }
    }

    /**
     * Construct an Observation from an XML String source.
     *
     * @param xml
     *            String of the XML.
     * @return An Observation.
     * @throws ObservationParsingException
     *             if there is an error parsing the XML.
     */
    public Observation read(String xml) throws ObservationParsingException {
        if (xml == null) {
            throw new IllegalArgumentException("XML must not be null");
        }
        try {
            return read(new StringReader(xml));
        } catch (IOException ioe) {
            String error = "Error reading XML: " + ioe.getMessage();
            throw new ObservationParsingException(error, ioe);
        }
    }

    /**
     * Construct an Observation from a InputStream.
     *
     * @param in
     *            An InputStream.
     * @return An Observation.
     * @throws ObservationParsingException
     *             if there is an error parsing the XML.
     */
    public Observation read(InputStream in)
            throws ObservationParsingException, IOException {
        if (in == null) {
            throw new IllegalArgumentException("stream must not be null");
        }
        try {
            return read(new InputStreamReader(in, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported");
        }
    }

    /**
     * Construct an Observation from a Reader.
     *
     * @param reader
     *            A Reader.
     * @return An Observation.
     * @throws ObservationParsingException
     *             if there is an error parsing the XML.
     */
    public Observation read(Reader reader)
            throws ObservationParsingException, IOException {
        if (reader == null) {
            throw new IllegalArgumentException("reader must not be null");
        }

        init();

        Document document;
        try {
            document = XmlUtil.buildDocument(reader, schemaMap);
        } catch (JDOMException jde) {
            String error = "XML failed schema validation: " + jde.getMessage();
            throw new ObservationParsingException(error, jde);
        }

        // Root element and namespace of the Document
        Element root = document.getRootElement();
        Namespace namespace = root.getNamespace();
        log.debug("obs namespace uri: " + namespace.getURI());
        log.debug("obs namespace prefix: " + namespace.getPrefix());

        ReadContext rc = new ReadContext();
        /*
        if (XmlConstants.CAOM2_0_NAMESPACE.equals(namespace.getURI())) {
            rc.docVersion = 20;
        } else if (XmlConstants.CAOM2_1_NAMESPACE.equals(namespace.getURI())) {
            rc.docVersion = 21;
        } else if (XmlConstants.CAOM2_2_NAMESPACE.equals(namespace.getURI())) {
            rc.docVersion = 22;
        } else if (XmlConstants.CAOM2_3_NAMESPACE.equals(namespace.getURI())) {
            rc.docVersion = 23;
        }
        */
        if (XmlConstants.CAOM2_4_NAMESPACE.equals(namespace.getURI())) {
            rc.docVersion = 24;
        }

        // Simple or Composite
        Attribute type = root.getAttribute("type", xsiNamespace);
        String tval = type.getValue();

        String collection = getChildText("collection", root, namespace, false);
        String suri = null;
        final String sub;
        if (rc.docVersion == 24) {
            String observationID = getChildText("observationID", root, namespace, true);
            suri = "caom:" + collection + "/" + observationID;
            sub = null;
        } else {
            suri = getChildText("uri", root, namespace, true);
            sub = getChildText("uriBucket", root, namespace, true);
        }

        URI obsURI = null;
        if (suri != null) {
            try {
                obsURI = new URI(suri);
            } catch (URISyntaxException e) {
                String error = "Unable to parse " + suri
                        + " to URI in element " + root.getName()
                        + " because " + e.getMessage();
            }
        }
        
        // Algorithm.
        Algorithm algorithm = getAlgorithm(root, namespace, rc);

        // Create the Observation.
        Observation obs;
        String simple = namespace.getPrefix() + ":" + SimpleObservation.class.getSimpleName();
        String comp = namespace.getPrefix() + ":CompositeObservation"; // 2.3
        String derived = namespace.getPrefix() + ":" + DerivedObservation.class.getSimpleName();
        if (simple.equals(tval)) {
            obs = new SimpleObservation(collection, obsURI, algorithm);
        } else if (derived.equals(tval) || comp.equals(tval)) {
            obs = new DerivedObservation(collection, obsURI, algorithm);
        } else {
            throw new ObservationParsingException("unexpected observation type: " + tval);
        }
        // verify inline vs computed uriBucket
        if (sub != null && !obs.getUriBucket().equals(sub)) {
            throw new ObservationParsingException("Observation.uriBucket: " + sub + " != " + obs.getUriBucket() + " (computed)");
        }

        // Observation children.
        String intent = getChildText("intent", root, namespace, false);
        if (intent != null) {
            obs.intent = ObservationIntentType.toValue(intent);
        }
        obs.type = getChildText("type", root, namespace, false);

        obs.metaRelease = getChildTextAsDate("metaRelease", root, namespace, false, rc.dateFormat);
        addGroups(obs.getMetaReadGroups(), "metaReadGroups", root, namespace, rc);
        obs.sequenceNumber = getChildTextAsInteger("sequenceNumber", root,
                namespace, false);
        obs.proposal = getProposal(root, namespace, rc);
        obs.target = getTarget(root, namespace, rc);
        obs.targetPosition = getTargetPosition(root, namespace, rc);
        obs.requirements = getRequirements(root, namespace, rc);
        obs.telescope = getTelescope(root, namespace, rc);
        obs.instrument = getInstrument(root, namespace, rc);
        obs.environment = getEnvironment(root, namespace, rc);

        addPlanes(obs.getURI(), obs.getPlanes(), root, namespace, rc);

        if (obs instanceof DerivedObservation) {
            addMembers(((DerivedObservation) obs).getMembers(), root,
                    namespace, rc);
        }

        assignEntityAttributes(root, obs, rc);

        return obs;
    }
    
    private void assignEntityAttributes(Element e, CaomEntity ce,
            ReadContext rc) throws ObservationParsingException {
        Attribute aid = e.getAttribute("id", e.getNamespace());
        Attribute alastModified = e.getAttribute("lastModified",
                e.getNamespace());
        Attribute amaxLastModified = e.getAttribute("maxLastModified",
                e.getNamespace());
        Attribute mcs = e.getAttribute("metaChecksum", e.getNamespace());
        Attribute acc = e.getAttribute("accMetaChecksum", e.getNamespace());
        Attribute amp = e.getAttribute("metaProducer", e.getNamespace());
        try {
            UUID uuid;
            if (rc.docVersion == 20) {
                Long id = new Long(aid.getLongValue());
                uuid = new UUID(0L, id);
            } else {
                uuid = UUID.fromString(aid.getValue());
            }

            CaomUtil.assignID(ce, uuid);
            if (alastModified != null) {
                Date lastModified = rc.parseTimestamp(alastModified.getValue());
                CaomUtil.assignLastModified(ce, lastModified, "lastModified");
            }

            if (rc.docVersion >= 23) {
                if (amaxLastModified != null) {
                    Date lastModified = rc
                            .parseTimestamp(amaxLastModified.getValue());
                    CaomUtil.assignLastModified(ce, lastModified,
                            "maxLastModified");
                }
                if (mcs != null) {
                    URI metaCS = new URI(mcs.getValue());
                    CaomUtil.assignMetaChecksum(ce, metaCS, "metaChecksum");
                }
                if (acc != null) {
                    URI accCS = new URI(acc.getValue());
                    CaomUtil.assignMetaChecksum(ce, accCS, "accMetaChecksum");
                }
            }
            
            if (rc.docVersion >= 24) {
                if (amp != null) {
                    ce.metaProducer = new URI(amp.getValue());
                }
            }
        } catch (DataConversionException ex) {
            throw new ObservationParsingException(
                    "invalid id: " + aid.getValue());
        } catch (ParseException ex) {
            throw new ObservationParsingException(
                    "invalid lastModified: " + alastModified.getValue());
        } catch (URISyntaxException ex) {
            throw new ObservationParsingException(
                    "invalid uri: " + aid.getValue());
        }
    }

    protected void addGroups(Set<URI> uris, String name, Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        if (rc.docVersion < 24) {
            return;
        }
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return;
        }
        List<Element> els = element.getChildren("groupURI", namespace);
        for (Element e : els) {
            String s = e.getTextTrim();
            try {
                URI u = new URI(s);
                uris.add(u);
            } catch (URISyntaxException ex) {
                throw new ObservationParsingException("invalid " + name + " URI: " + s, ex);
            }
        }
    }
    
    /**
     * Build an Algorithm from a JDOM representation of an algorithm element.
     *
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Algorithm, or null if the document doesn't contain an
     *         algorithm element.
     * @throws ObservationParsingException
     */
    protected Environment getEnvironment(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("environment", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Environment env = new Environment();
        env.seeing = getChildTextAsDouble("seeing", element, namespace, false);
        env.humidity = getChildTextAsDouble("humidity", element, namespace,
                false);
        env.elevation = getChildTextAsDouble("elevation", element, namespace,
                false);
        env.tau = getChildTextAsDouble("tau", element, namespace, false);
        env.wavelengthTau = getChildTextAsDouble("wavelengthTau", element,
                namespace, false);
        env.ambientTemp = getChildTextAsDouble("ambientTemp", element,
                namespace, false);
        env.photometric = getChildTextAsBoolean("photometric", element,
                namespace, false);
        return env;
    }

    /**
     * Build an Algorithm from a JDOM representation of an algorithm element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Algorithm, or null if the document doesn't contain an
     *         algorithm element.
     * @throws ObservationParsingException
     */
    protected Algorithm getAlgorithm(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("algorithm", parent, namespace, true);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String name = getChildText("name", element, namespace, true);
        return new Algorithm(name);
    }

    /**
     * Build an Proposal from a JDOM representation of an proposal element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Proposal, or null if the document doesn't contain an proposal
     *         element.
     * @throws ObservationParsingException
     */
    protected Proposal getProposal(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("proposal", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String id = getChildText("id", element, namespace, true);
        // no check for null: Proposal will check and throw if illegal
        Proposal proposal = new Proposal(id);

        proposal.pi = getChildText("pi", element, namespace, false);
        proposal.project = getChildText("project", element, namespace, false);
        proposal.title = getChildText("title", element, namespace, false);
        String ref = getChildText("reference", element, namespace, false);
        if (ref != null) {
            try {
                proposal.reference = new URI(ref);
            } catch (URISyntaxException ex) {
                throw new ObservationParsingException("invalid Proposal.reference: " + ref, ex);
            }
        }
        if (rc.docVersion < 23) {
            addChildTextToStringList("keywords", proposal.getKeywords(),
                    element, namespace, false);
        } else {
            addKeywordsToList(proposal.getKeywords(), element, namespace);
        }

        return proposal;
    }

    /**
     * Build an Target from a JDOM representation of an target element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Target, or null if the document doesn't contain an target
     *         element.
     * @throws ObservationParsingException
     */
    protected Target getTarget(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("target", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String name = getChildText("name", element, namespace, true);

        Target target = new Target(name);
        
        String tid = getChildText("targetID", element, namespace, false);
        if (tid != null) {
            try {
                target.targetID = new URI(tid);
            } catch (URISyntaxException ex) {
                throw new ObservationParsingException("invalid targetID: " + tid, ex);
            }
        }

        String type = getChildText("type", element, namespace, false);
        if (type != null) {
            target.type = TargetType.toValue(type);
        }

        target.standard = getChildTextAsBoolean("standard", element, namespace,
                false);
        target.redshift = getChildTextAsDouble("redshift", element, namespace,
                false);
        target.moving = getChildTextAsBoolean("moving", element, namespace,
                false);
        if (rc.docVersion < 23) {
            addChildTextToStringList("keywords", target.getKeywords(), element,
                    namespace, false);
        } else {
            addKeywordsToList(target.getKeywords(), element, namespace);
        }

        return target;
    }

    /**
     * Build a TargetPosition from a JDOM representation of an targetPosition
     * element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return a TargetPosition, or null if the document doesn't contain an
     *         targetPosition element.
     * @throws ObservationParsingException
     */
    protected TargetPosition getTargetPosition(Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("targetPosition", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String coordsys = getChildText("coordsys", element, namespace, true);
        Double equinox = getChildTextAsDouble("equinox", element, namespace,
                false);
        Element coords = getChildElement("coordinates", element, namespace,
                true);
        double cval1 = getChildTextAsDouble("cval1", coords, namespace, true);
        double cval2 = getChildTextAsDouble("cval2", coords, namespace, true);

        TargetPosition tpos = new TargetPosition(coordsys,
                new Point(cval1, cval2));
        tpos.equinox = equinox;

        return tpos;
    }

    /**
     * 
     * @param parent
     * @param namespace
     * @param rc
     * @return
     * @throws ObservationParsingException
     */
    protected Requirements getRequirements(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("requirements", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String flag = getChildText("flag", element, namespace, true);
        Requirements req = new Requirements(Status.toValue(flag));

        return req;
    }

    /**
     * Build an Telescope from a JDOM representation of an telescope element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an TarTelescopeget, or null if the document doesn't contain an
     *         telescope element.
     * @throws ObservationParsingException
     */
    protected Telescope getTelescope(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("telescope", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String name = getChildText("name", element, namespace, true);
        Telescope telescope = new Telescope(name);

        telescope.geoLocationX = getChildTextAsDouble("geoLocationX", element,
                namespace, false);
        telescope.geoLocationY = getChildTextAsDouble("geoLocationY", element,
                namespace, false);
        telescope.geoLocationZ = getChildTextAsDouble("geoLocationZ", element,
                namespace, false);
        String track = getChildText("trackingMode", element, namespace, false);
        if (track != null) {
            telescope.trackingMode = new Tracking(track);
        }
        if (rc.docVersion < 23) {
            addChildTextToStringList("keywords", telescope.getKeywords(),
                    element, namespace, false);
        } else {
            addKeywordsToList(telescope.getKeywords(), element, namespace);
        }

        return telescope;
    }

    /**
     * Build an Instrument from a JDOM representation of an instrument element.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Instrument, or null if the document doesn't contain an
     *         instrument element.
     * @throws ObservationParsingException
     */
    protected Instrument getInstrument(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("instrument", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String name = getChildText("name", element, namespace, true);
        Instrument instrument = new Instrument(name);

        if (rc.docVersion < 23) {
            addChildTextToStringList("keywords", instrument.getKeywords(),
                    element, namespace, false);
        } else {
            addKeywordsToList(instrument.getKeywords(), element, namespace);
        }

        return instrument;
    }

    /**
     * Creates ObservationURI from the observationURI elements found in the
     * members element, and adds them to the given Set of ObservationURI's.
     * 
     * @param members
     *            Set of Member's from the Observation.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addMembers(Set<URI> members, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("members", parent, namespace, false);
        if (element != null) {
            List children = getChildrenElements("member", element,
                    namespace, false);
            Iterator it = children.iterator();
            while (it.hasNext()) {
                Element child = (Element) it.next();
                try {
                    members.add(new URI(child.getText()));
                } catch (URISyntaxException e) {
                    String error = "Unable to parse observationURI "
                            + child.getText()
                            + " in to an ObservationURI in element "
                            + element.getName() + " because " + e.getMessage();
                    throw new ObservationParsingException(error);
                }
            }
        }
    }

    /**
     * Creates Plane's from the plane elements found in the planes element, and
     * adds them to the given Set of Plane's.
     * 
     * @param planes
     *            the Set of Plane's from the Observation.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addPlanes(URI obsURI, Set<Plane> planes, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("planes", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return;
        }

        List planeElements = getChildrenElements("plane", element, namespace,
                false);
        Iterator it = planeElements.iterator();
        while (it.hasNext()) {
            Element planeElement = (Element) it.next();
            String suri = null;
            if (rc.docVersion == 24) {
                String productID = getChildText("productID", planeElement, namespace, true);
                suri = obsURI + "/" + productID;
                log.warn("addPlanes: " + obsURI + " + " + productID + " -> " + suri);
            } else {
                suri = getChildText("uri", planeElement, namespace, true);
            }
            
            
            URI planeURI = null;
            if (suri != null) {
                try {
                    planeURI = new URI(suri);
                } catch (URISyntaxException e) {
                    String error = "Unable to parse " + suri
                            + " to URI in element " + planeElement.getName()
                            + " because " + e.getMessage();
                }
            }
            Plane plane = new Plane(planeURI);
            
            plane.metaRelease = getChildTextAsDate("metaRelease", planeElement, namespace, false, rc.dateFormat);
            addGroups(plane.getMetaReadGroups(), "metaReadGroups", planeElement, namespace, rc);
            plane.dataRelease = getChildTextAsDate("dataRelease", planeElement, namespace, false, rc.dateFormat);
            addGroups(plane.getDataReadGroups(), "dataReadGroups", planeElement, namespace, rc);

            String dataProductType = getChildText("dataProductType", planeElement, namespace, false);
            if (dataProductType != null) {
                plane.dataProductType = DataProductType.toValue(dataProductType);
            }

            String calibrationLevel = getChildText("calibrationLevel", planeElement, namespace, false);
            if (calibrationLevel != null) {
                plane.calibrationLevel = CalibrationLevel.toValue(Integer.parseInt(calibrationLevel));
            }

            plane.provenance = getProvenance(planeElement, namespace, rc);
            plane.metrics = getMetrics(planeElement, namespace, rc);
            plane.quality = getQuality(planeElement, namespace, rc);

            plane.observable = getObservable(planeElement, namespace, rc);
            plane.position = getPosition(planeElement, namespace, rc);
            plane.energy = getEnergy(planeElement, namespace, rc);
            plane.time = getTime(planeElement, namespace, rc);
            plane.polarization = getPolarization(planeElement, namespace, rc);
            plane.custom = getCustom(planeElement, namespace, rc);
            plane.visibility = getVisibility(planeElement, namespace, rc);
            
            addArtifacts(plane.getArtifacts(), planeElement, namespace, rc);

            assignEntityAttributes(planeElement, plane, rc);

            boolean added = planes.add(plane);
            if (!added) {
                throw new IllegalArgumentException("Plane.uri = " +  suri + " is not unique");
            }
        }
    }

    protected Position getPosition(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("position", parent, namespace, false);
        if (element == null) {
            return null;
        }

        final String circleType = namespace.getPrefix() + ":" + Circle.class.getSimpleName();
        final String polyType = namespace.getPrefix() + ":" + Polygon.class.getSimpleName();
        Element cur = getChildElement("bounds", element, namespace, false);
        Shape bounds = null;
        MultiShape samples = new MultiShape();
        if (cur != null) {
            if (rc.docVersion < 23) {
                throw new UnsupportedOperationException(
                        "cannot convert version " + rc.docVersion
                                + " polygon to current version");
            }
            Attribute type = cur.getAttribute("type", xsiNamespace);
            String tval = type.getValue();
            bounds = getShape(cur, namespace);
            if (rc.docVersion == 24) {
                if (polyType.equals(tval)) {
                    // CAOM-2.4 MultiPolygon
                    Element se = cur.getChild("samples", namespace);
                    Element ves = se.getChild("vertices", namespace);
                    Polygon sp = new Polygon();
                    for (Element ve : ves.getChildren()) {
                        double cval1 = getChildTextAsDouble("cval1", ve, namespace,
                                true);
                        double cval2 = getChildTextAsDouble("cval2", ve, namespace,
                                true);
                        int sv = getChildTextAsInteger("type", ve, namespace, true);
                        if (sv == 0) {
                            // close
                            samples.getShapes().add(sp);
                            sp = new Polygon();
                        } else {
                            sp.getVertices().add(new Point(cval1, cval2));
                        }
                    }
                }
                if (circleType.equals(tval)) {
                    if (rc.docVersion == 24) {
                        // CAOM-2.4 had no samples for circle
                        samples.getShapes().add(bounds);
                    }
                }
            }
        }
        
        cur = getChildElement("samples", element, namespace, false);
        if (cur != null) {
            // CAOM-2.5
            List<Element> ses = cur.getChildren("shape", namespace);
            for (Element se : ses) {
                Shape s = getShape(se, namespace);
                samples.getShapes().add(s);
            }
        }
        
        Position ret = new Position(bounds, samples);
        
        Element mb = getChildElement("minBounds", element, namespace, false);
        if (mb != null) {
            ret.minBounds = getShape(mb, namespace);
        }
                
        cur = getChildElement("dimension", element, namespace, false);
        if (cur != null) {
            // Attribute type = cur.getAttribute("type", xsiNamespace);
            // String tval = type.getValue();
            // String extype = namespace.getPrefix() + ":" +
            // Dimension2D.class.getSimpleName();
            // if ( extype.equals(tval) )
            // {
            long naxis1 = getChildTextAsLong("naxis1", cur, namespace, true);
            long naxis2 = getChildTextAsLong("naxis2", cur, namespace, true);
            ret.dimension = new Dimension2D(naxis1, naxis2);
            // }
            // else
            // throw new ObservationParsingException("unsupported dimension
            // type: " + tval);
        }
        
        cur = getChildElement("maxRecoverableScale", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.maxRecoverableScale = new Interval<Double>(lb, ub);
        }

        ret.resolution = getChildTextAsDouble("resolution", element, namespace,
                false);
        cur = getChildElement("resolutionBounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.resolutionBounds = new Interval<Double>(lb, ub);
        }
        ret.sampleSize = getChildTextAsDouble("sampleSize", element, namespace,
                false);
        //pos.timeDependent = getChildTextAsBoolean("timeDependent", element,
        //        namespace, false);
        
        String c = getChildText("calibration", element, namespace, false);
        if (c != null) {
            ret.calibration = CalibrationStatus.toValue(c);
        }
        
        return ret;
    }
    
    private Shape getShape(Element se, Namespace namespace) throws ObservationParsingException {
        final String circleType = namespace.getPrefix() + ":" + Circle.class.getSimpleName();
        final String polyType = namespace.getPrefix() + ":" + Polygon.class.getSimpleName();
        
        Attribute type = se.getAttribute("type", xsiNamespace);
        String tval = type.getValue();
        if (polyType.equals(tval)) {
            Polygon poly = new Polygon();
            Element pes = se.getChild("points", namespace);
            for (Element pe : pes.getChildren()) {
                double cval1 = getChildTextAsDouble("cval1", pe, namespace, true);
                double cval2 = getChildTextAsDouble("cval2", pe, namespace, true);
                poly.getVertices().add(new Point(cval1, cval2));
            }
            return poly;
        }
        if (circleType.equals(tval)) {
            Element ce = se.getChild("center", namespace);
            double cval1 = getChildTextAsDouble("cval1", ce, namespace, true);
            double cval2 = getChildTextAsDouble("cval2", ce, namespace, true);
            Point c = new Point(cval1, cval2);
            double r = getChildTextAsDouble("radius", se, namespace, true);
            return new Circle(c, r);
        }
        throw new UnsupportedOperationException("unsupported shape: " + tval);
    }

    protected Energy getEnergy(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("energy", parent, namespace, false);
        if (element == null) {
            return null;
        }

        Interval<Double> bounds = null;
        Element cur = getChildElement("bounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, true);
            double ub = getChildTextAsDouble("upper", cur, namespace, true);
            bounds = new Interval<Double>(lb, ub);
            //addSamples(nrg.getSamples(), cur.getChild("samples", namespace),
            //        namespace, rc);
        }
        Energy ret = new Energy(bounds);
        Element se = getChildElement("samples", element, namespace, false);
        if (se != null) {
            addSamples(ret.getSamples(), se, namespace, rc);
        }

        cur = getChildElement("dimension", element, namespace, false);
        if (cur != null) {
            ret.dimension = getChildTextAsLong("dimension", element, namespace,
                    true);
        }

        ret.resolvingPower = getChildTextAsDouble("resolvingPower", element,
                namespace, false);
        cur = getChildElement("resolvingPowerBounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.resolvingPowerBounds = new Interval<Double>(lb, ub);
        }
        ret.resolution = getChildTextAsDouble("resolution", element,
                namespace, false);
        cur = getChildElement("resolutionBounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.resolutionBounds = new Interval<Double>(lb, ub);
        }
        ret.sampleSize = getChildTextAsDouble("sampleSize", element, namespace,
                false);

        ret.bandpassName = getChildText("bandpassName", element, namespace,
                false);

        // for 2.3 there is 0..1 EnergyBand
        // for 2.4 there are 0..* EnergyBand(s)
        if (rc.docVersion < 24) {
            String emb = getChildText("emBand", element, namespace, false);
            if (emb != null) {
                ret.getEnergyBands().add(EnergyBand.toValue(emb));
            }
        } else {
            cur = getChildElement("energyBands", element, namespace, false);
            if (cur != null) {
                List<Element> bes = cur.getChildren("emBand", namespace);
                for (Element b : bes) {
                    String emb = b.getTextTrim();
                    ret.getEnergyBands().add(EnergyBand.toValue(emb));
                }
            }
        }
        ret.rest = getChildTextAsDouble("rest", element, namespace, false);
        //nrg.restwav = getChildTextAsDouble("restwav", element, namespace, false);

        cur = getChildElement("transition", element, namespace, false);
        if (cur != null) {
            String species = getChildText("species", cur, namespace, true);
            String trans = getChildText("transition", cur, namespace, true);
            ret.transition = new EnergyTransition(species, trans);
        }
        
        String c = getChildText("calibration", element, namespace, false);
        if (c != null) {
            ret.calibration = CalibrationStatus.toValue(c);
        }

        return ret;
    }

    protected Time getTime(Element parent, Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("time", parent, namespace, false);
        if (element == null) {
            return null;
        }

        Interval<Double> bounds = null;
        Element cur = getChildElement("bounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, true);
            double ub = getChildTextAsDouble("upper", cur, namespace, true);
            bounds = new Interval<Double>(lb, ub);
            //addSamples(tim.getSamples(), cur.getChild("samples", namespace),
            //        namespace, rc);
        }
        Time ret = new Time(bounds);
        Element se = getChildElement("samples", element, namespace, false);
        if (se != null) {
            addSamples(ret.getSamples(), se, namespace, rc);
        }

        cur = getChildElement("dimension", element, namespace, false);
        if (cur != null) {
            ret.dimension = getChildTextAsLong("dimension", element, namespace, true);
        }

        ret.resolution = getChildTextAsDouble("resolution", element, namespace,
                false);
        cur = getChildElement("resolutionBounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.resolutionBounds = new Interval<Double>(lb, ub);
        }

        ret.sampleSize = getChildTextAsDouble("sampleSize", element, namespace,
                false);

        ret.exposure = getChildTextAsDouble("exposure", element, namespace,
                false);
        cur = getChildElement("exposureBounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, false);
            double ub = getChildTextAsDouble("upper", cur, namespace, false);
            ret.exposureBounds = new Interval<Double>(lb, ub);
        }

        String c = getChildText("calibration", element, namespace, false);
        if (c != null) {
            ret.calibration = CalibrationStatus.toValue(c);
        }
        return ret;
    }

    private CustomAxis getCustom(Element parent, Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("custom", parent, namespace, false);
        if (element == null) {
            return null;
        }

        String ctype = getChildText("ctype", element, namespace, true);
        Interval<Double> bounds = null;
        Element cur = getChildElement("bounds", element, namespace, false);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, true);
            double ub = getChildTextAsDouble("upper", cur, namespace, true);
            bounds = new Interval<Double>(lb, ub);
            //addSamples(cus.getSamples(), cur.getChild("samples", namespace), namespace, rc);
        }
        CustomAxis ret = new CustomAxis(ctype, bounds);
        Element se = getChildElement("samples", element, namespace, false);
        if (se != null) {
            addSamples(ret.getSamples(), se, namespace, rc);
        }
        
        cur = getChildElement("dimension", element, namespace, false);
        if (cur != null) {
            ret.dimension = getChildTextAsLong("dimension", element, namespace, true);
        }

        return ret;
    }
    
    private Visibility getVisibility(Element parent, Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("visibility", parent, namespace, false);
        if (element == null) {
            return null;
        }
        
        Interval<Double> distance = null;
        Element cur = getChildElement("distance", element, namespace, true);
        if (cur != null) {
            double lb = getChildTextAsDouble("lower", cur, namespace, true);
            double ub = getChildTextAsDouble("upper", cur, namespace, true);
            distance = new Interval<Double>(lb, ub);
        }
        Double de = getChildTextAsDouble("distributionEccentricity", element, namespace, true);
        Double df = getChildTextAsDouble("distributionFill", element, namespace, true);
        Visibility ret = new Visibility(distance, de, df);
        
        return  ret;
    }

    private void addSamples(List<Interval<Double>> samps, Element sampleElement,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        if (sampleElement != null) {
            List<Element> sse = sampleElement.getChildren("sample", namespace);
            for (Element se : sse) {
                double lb = getChildTextAsDouble("lower", se, namespace, true);
                double ub = getChildTextAsDouble("upper", se, namespace, true);
                samps.add(new Interval<Double>(lb, ub));
            }
        }
        //if (rc.docVersion < 23 && inter.getSamples().isEmpty()) {
            // backwards compat
        //    samps.add(new Interval(inter.getLower(), inter.getUpper()));
        //}
    }

    protected Polarization getPolarization(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("polarization", parent, namespace,
                false);
        if (element == null) {
            return null;
        }

        Polarization pol = new Polarization();
        Element cur = getChildElement("states", element, namespace, false);
        if (cur != null) {
            List<Element> ces = cur.getChildren();
            for (Element e : ces) {
                String ss = e.getTextTrim();
                PolarizationState ps = PolarizationState.valueOf(ss);
                pol.getStates().add(ps);
            }
        }

        cur = getChildElement("dimension", element, namespace, false);
        if (cur != null) {
            pol.dimension = getChildTextAsInteger("dimension", element, namespace, true);
        }

        return pol;
    }

    /**
     * Build a Provenance from a JDOM representation of an Provenance.
     * 
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @return an Provenance, or null if the document doesn't contain a
     *         provenance element.
     * @throws ObservationParsingException
     */
    protected Provenance getProvenance(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("provenance", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String name = getChildText("name", element, namespace, true);
        Provenance provenance = new Provenance(name);

        provenance.version = getChildText("version", element, namespace, false);
        provenance.project = getChildText("project", element, namespace, false);
        provenance.producer = getChildText("producer", element, namespace,
                false);
        provenance.runID = getChildText("runID", element, namespace, false);
        String reference = getChildText("reference", element, namespace, false);
        if (reference != null) {
            try {
                provenance.reference = new URI(reference);
            } catch (URISyntaxException e) {
                String error = "Unable to parse reference " + reference
                        + " to URI in element " + element.getName()
                        + " because " + e.getMessage();
                throw new ObservationParsingException(error);
            }
        }
        provenance.lastExecuted = getChildTextAsDate("lastExecuted", element,
                namespace, false, rc.dateFormat);
        if (rc.docVersion < 23) {
            addChildTextToStringList("keywords", provenance.getKeywords(),
                    element, namespace, false);
        } else {
            addKeywordsToList(provenance.getKeywords(), element, namespace);
        }
        addInputs(provenance.getInputs(), element, namespace, rc);

        return provenance;
    }

    protected Metrics getMetrics(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("metrics", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Metrics metrics = new Metrics();
        metrics.sourceNumberDensity = getChildTextAsDouble(
                "sourceNumberDensity", element, namespace, false);
        metrics.background = getChildTextAsDouble("background", element, namespace, false);
        metrics.backgroundStddev = getChildTextAsDouble("backgroundStddev", element, namespace, false);
        metrics.fluxDensityLimit = getChildTextAsDouble("fluxDensityLimit", element, namespace, false);
        metrics.magLimit = getChildTextAsDouble("magLimit", element, namespace, false);
        metrics.sampleSNR = getChildTextAsDouble("sampleSNR", element, namespace, false);
        return metrics;
    }

    /**
     * 
     * @param parent
     * @param namespace
     * @param rc
     * @return
     * @throws ObservationParsingException
     */
    protected DataQuality getQuality(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("quality", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String flag = getChildText("flag", element, namespace, true);
        DataQuality ret = new DataQuality(Quality.toValue(flag));

        return ret;
    }
    
    /**
     * 
     * @param parent
     * @param namespace
     * @param rc
     * @return
     * @throws ObservationParsingException
     */
    protected Observable getObservable(Element parent, Namespace namespace,
            ReadContext rc) throws ObservationParsingException {
        Element element = getChildElement("observable", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String ucd = getChildText("ucd", element, namespace, true);
        UCD t = new UCD(ucd);
        Observable ret = new Observable(t);
        String c = getChildText("calibration", element, namespace, false);
        if (c != null) {
            ret.calibration = CalibrationStatus.toValue(c);
        }
        
        return ret;
    }

    protected EnergyTransition getTransition(Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("transition", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String species = getChildText("species", element, namespace, true);
        String transition = getChildText("transition", element, namespace,
                true);
        return new EnergyTransition(species, transition);
    }

    /**
     * Creates PlaneURI's from the planeURI elements found in the inputs
     * element, and adds them to the given Set of PlaneURI's.
     * 
     * @param inputs
     *            the Set of PlaneURI from the Provenance.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addInputs(Set<URI> inputs, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("inputs", parent, namespace, false);
        if (element != null) {
            List children = getChildrenElements("input", element, namespace,
                    false);
            Iterator it = children.iterator();
            while (it.hasNext()) {
                Element child = (Element) it.next();
                try {
                    inputs.add(new URI(child.getText()));
                } catch (URISyntaxException e) {
                    String error = "Unable to parse "
                            + child.getText()
                            + " in to a URI in element "
                            + element.getName() + " because " + e.getMessage();
                    throw new ObservationParsingException(error);
                }
            }
        }
    }

    /**
     * Creates Artifact's from the artifact elements found in the artifacts
     * element, and adds them to the given Set of Artifact's.
     * 
     * @param artifacts
     *            the Set of Artifact's from the Plane.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addArtifacts(Set<Artifact> artifacts, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("artifacts", parent, namespace,
                false);
        if (element == null || element.getContentSize() == 0) {
            return;
        }

        List artifactElements = getChildrenElements("artifact", element,
                namespace, false);
        Iterator it = artifactElements.iterator();
        while (it.hasNext()) {
            Element artifactElement = (Element) it.next();
            String uri = getChildText("uri", artifactElement, namespace, true);
            String sub = getChildText("uriBucket", artifactElement, namespace, false); // 2.5+

            String pts = getChildText("productType", artifactElement, namespace, true);
            DataLinkSemantics productType = DataLinkSemantics.toValue(pts);

            String rts = getChildText("releaseType", artifactElement, namespace, false);
            ReleaseType releaseType = ReleaseType.toValue(rts);

            Artifact artifact;
            try {
                artifact = new Artifact(new URI(uri), productType, releaseType);
            } catch (URISyntaxException e) {
                String error = "invalid Artifact.uri " + uri + " reasson: " + e.getMessage();
                throw new ObservationParsingException(error, e);
            }
            // verify inline vs computed uriBucket
            if (sub != null && !artifact.getUriBucket().equals(sub)) {
                throw new ObservationParsingException("Artifact.uriBucket: " + sub + " != " + artifact.getUriBucket() + " (computed)");
            }

            artifact.contentType = getChildText("contentType", artifactElement,
                    namespace, false);
            artifact.contentLength = getChildTextAsLong("contentLength",
                    artifactElement, namespace, false);

            String contentChecksumStr = getChildText("contentChecksum",
                    artifactElement, namespace, false);
            if (contentChecksumStr != null) {
                try {
                    artifact.contentChecksum = new URI(contentChecksumStr);
                } catch (URISyntaxException e) {
                    String error = "Unable to parse contentChecksum " + uri
                            + " into a URI in element "
                            + artifactElement.getName() + " because "
                            + e.getMessage();
                    throw new ObservationParsingException(error, e);
                }
            }
            
            artifact.contentRelease = getChildTextAsDate("contentRelease", artifactElement,
                    namespace, false, rc.dateFormat);
            addGroups(artifact.getContentReadGroups(), "contentReadGroups", artifactElement, namespace, rc);

            String descStr = getChildText("descriptionID", artifactElement, namespace, false);
            if (descStr != null) {
                try {
                    artifact.descriptionID = new URI(descStr);
                } catch (URISyntaxException e) {
                    String error = "Unable to parse descriptionID " + uri
                            + " into a URI in element "
                            + artifactElement.getName() + " because "
                            + e.getMessage();
                    throw new ObservationParsingException(error, e);
                }
            }
            addParts(artifact.getParts(), artifactElement, namespace, rc);

            assignEntityAttributes(artifactElement, artifact, rc);

            boolean added = artifacts.add(artifact);
            if (!added) {
                throw new IllegalArgumentException("Artifact.uri = " +  uri + " is not unique");
            }
        }
    }

    /**
     * Creates Part's from the part elements found in the parts element, and
     * adds them to the given Set of Part's.
     * 
     * @param parts
     *            the Set of Part's from the Artifact.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addParts(Set<Part> parts, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("parts", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return;
        }

        List partElements = getChildrenElements("part", element, namespace,
                false);
        Iterator it = partElements.iterator();
        while (it.hasNext()) {
            Element partElement = (Element) it.next();
            String partName = getChildText("name", partElement, namespace,
                    true);

            Part part = new Part(partName);

            String productType = getChildText("productType", partElement,
                    namespace, false);
            if (productType != null) {
                part.productType = DataLinkSemantics.toValue(productType);
            }
            addChunks(part.getChunks(), partElement, namespace, rc);

            assignEntityAttributes(partElement, part, rc);
            
            boolean added = parts.add(part);
            if (!added) {
                throw new IllegalArgumentException("Part.name = " +  partName + " is not unique");
            }
        }
    }

    /**
     * Creates Chunk's from the chunk elements found in the chunks element, and
     * adds them to the given Set of Chunk's.
     * 
     * @param chunks
     *            the Set of Chunk's from the Part.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param rc
     * @throws ObservationParsingException
     */
    protected void addChunks(Set<Chunk> chunks, Element parent,
            Namespace namespace, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement("chunks", parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return;
        }

        List chunkElements = getChildrenElements("chunk", element, namespace,
                false);
        Iterator it = chunkElements.iterator();
        while (it.hasNext()) {
            Element chunkElement = (Element) it.next();

            Chunk chunk = new Chunk();

            String productType = getChildText("productType", chunkElement,
                    namespace, false);
            if (productType != null) {
                chunk.productType = DataLinkSemantics.toValue(productType);
            }
            chunk.naxis = getChildTextAsInteger("naxis", chunkElement,
                    namespace, false);
            chunk.observableAxis = getChildTextAsInteger("observableAxis",
                    chunkElement, namespace, false);
            chunk.positionAxis1 = getChildTextAsInteger("positionAxis1",
                    chunkElement, namespace, false);
            chunk.positionAxis2 = getChildTextAsInteger("positionAxis2",
                    chunkElement, namespace, false);
            chunk.energyAxis = getChildTextAsInteger("energyAxis", chunkElement,
                    namespace, false);
            chunk.timeAxis = getChildTextAsInteger("timeAxis", chunkElement,
                    namespace, false);
            chunk.polarizationAxis = getChildTextAsInteger("polarizationAxis",
                    chunkElement, namespace, false);
            chunk.customAxis = getChildTextAsInteger("customAxis",
                    chunkElement, namespace, false);

            chunk.observable = getObservableAxis("observable", chunkElement, namespace, false, rc);
            chunk.position = getSpatialWCS("position", chunkElement, namespace, false, rc);
            chunk.energy = getSpectralWCS("energy", chunkElement, namespace, false, rc);
            chunk.time = getTemporalWCS("time", chunkElement, namespace, false, rc);
            chunk.polarization = getPolarizationWCS("polarization", chunkElement, namespace, false, rc);
            
            chunk.custom = getCustomWCS("custom", chunkElement, namespace, false, rc);

            assignEntityAttributes(chunkElement, chunk, rc);

            chunks.add(chunk);
        }
    }

    /**
     * Build an ObservableAxis from a JDOM representation of an observable
     * element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @param rc
     * @return an ObservableAxis, or null if the document doesn't contain an
     *         observable element.
     * @throws ObservationParsingException
     */
    protected ObservableAxis getObservableAxis(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Slice dependent = getSlice("dependent", element, namespace, true);
        ObservableAxis observable = new ObservableAxis(dependent);
        observable.independent = getSlice("independent", element, namespace,
                false);
        return observable;
    }

    /**
     * Build an SpatialWCS from a JDOM representation of an position element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @param rc
     * @return an SpatialWCS, or null if the document doesn't contain an
     *         position element.
     * @throws ObservationParsingException
     */
    protected SpatialWCS getSpatialWCS(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordAxis2D axis = getCoordAxis2D("axis", element, namespace, true);
        SpatialWCS position = new SpatialWCS(axis);
        position.coordsys = getChildText("coordsys", element, namespace, false);
        position.equinox = getChildTextAsDouble("equinox", element, namespace,
                false);
        position.resolution = getChildTextAsDouble("resolution", element,
                namespace, false);
        return position;
    }

    /**
     * Build an SpectralWCS from a JDOM representation of an energy element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @param rc
     * @return an SpectralWCS, or null if the document doesn't contain an energy
     *         element.
     * @throws ObservationParsingException
     */
    protected SpectralWCS getSpectralWCS(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordAxis1D axis = getCoordAxis1D("axis", element, namespace, true);
        String specsys = getChildText("specsys", element, namespace, true);
        SpectralWCS energy = new SpectralWCS(axis, specsys);
        energy.ssysobs = getChildText("ssysobs", element, namespace, false);
        energy.ssyssrc = getChildText("ssyssrc", element, namespace, false);
        energy.restfrq = getChildTextAsDouble("restfrq", element, namespace,
                false);
        energy.restwav = getChildTextAsDouble("restwav", element, namespace,
                false);
        energy.velosys = getChildTextAsDouble("velosys", element, namespace,
                false);
        energy.zsource = getChildTextAsDouble("zsource", element, namespace,
                false);
        energy.velang = getChildTextAsDouble("velang", element, namespace,
                false);
        energy.bandpassName = getChildText("bandpassName", element, namespace,
                false);
        energy.resolvingPower = getChildTextAsDouble("resolvingPower", element,
                namespace, false);
        energy.transition = getTransition(element, namespace, rc);
        return energy;
    }

    /**
     * Build an TemporalWCS from a JDOM representation of an time element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @param rc
     * @return an TemporalWCS, or null if the document doesn't contain an time
     *         element.
     * @throws ObservationParsingException
     */
    protected TemporalWCS getTemporalWCS(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordAxis1D axis = getCoordAxis1D("axis", element, namespace, true);
        TemporalWCS time = new TemporalWCS(axis);
        time.timesys = getChildText("timesys", element, namespace, false);
        time.trefpos = getChildText("trefpos", element, namespace, false);
        time.mjdref = getChildTextAsDouble("mjdref", element, namespace, false);
        time.exposure = getChildTextAsDouble("exposure", element, namespace,
                false);
        time.resolution = getChildTextAsDouble("resolution", element, namespace,
                false);
        return time;
    }

    /**
     * Build an PolarizationWCS from a JDOM representation of an polarization
     * element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     * @param rc
     * @return an PolarizationWCS, or null if the document doesn't contain an
     *         polarization element.
     * @throws ObservationParsingException
     */
    protected PolarizationWCS getPolarizationWCS(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordAxis1D axis = getCoordAxis1D("axis", element, namespace, true);
        return new PolarizationWCS(axis);
    }
    
    /**
     * Build an CustomWCS from a JDOM representation of an custom
     * element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     * @param rc
     * @return an PolarizationWCS, or null if the document doesn't contain an
     *         polarization element.
     * @throws ObservationParsingException
     */
    protected CustomWCS getCustomWCS(String name, Element parent,
            Namespace namespace, boolean required, ReadContext rc)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordAxis1D axis = getCoordAxis1D("axis", element, namespace, true);
        return new CustomWCS(axis);
    }

    /**
     * Build an Axis from a JDOM representation of an axis element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an Axis, or null if the document doesn't contain an axis element.
     * @throws ObservationParsingException
     */
    protected Axis getAxis(String name, Element parent, Namespace namespace,
            boolean required) throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        String ctype = getChildText("ctype", element, namespace, true);
        Axis ret = new Axis(ctype);
        ret.cunit = getChildText("cunit", element, namespace, false);
        return ret;
    }

    /**
     * Build an Coord2D from a JDOM representation of an element named name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return a Coord2D, or null if the document doesn't contain element named
     *         name.
     * @throws ObservationParsingException
     */
    protected Coord2D getCoord2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        RefCoord coord1 = getRefCoord("coord1", element, namespace, true);
        RefCoord coord2 = getRefCoord("coord2", element, namespace, true);
        return new Coord2D(coord1, coord2);
    }

    /**
     * Build an ValueCoord2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return a ValueCoord2D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected ValueCoord2D getValueCoord2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        double coord1 = getChildTextAsDouble("coord1", element, namespace,
                true);
        double coord2 = getChildTextAsDouble("coord2", element, namespace,
                true);
        return new ValueCoord2D(coord1, coord2);
    }

    /**
     * Build an CoordAxis1D from a JDOM representation of element name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordAxis1D, or null if the document doesn't contain element
     *         called name.
     * @throws ObservationParsingException
     */
    protected CoordAxis1D getCoordAxis1D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Axis axis = getAxis("axis", element, namespace, true);
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        coordAxis1D.error = getCoordError("error", element, namespace, false);
        coordAxis1D.range = getCoordRange1D("range", element, namespace, false);
        coordAxis1D.bounds = getCoordBounds1D("bounds", element, namespace,
                false);
        coordAxis1D.function = getCoordFunction1D("function", element,
                namespace, false);
        return coordAxis1D;
    }

    /**
     * Build an CoordAxis2D from a JDOM representation of element name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordAxis2D, or null if the document doesn't contain element
     *         called name.
     * @throws ObservationParsingException
     */
    protected CoordAxis2D getCoordAxis2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Axis axis1 = getAxis("axis1", element, namespace, true);
        Axis axis2 = getAxis("axis2", element, namespace, true);

        CoordAxis2D axis = new CoordAxis2D(axis1, axis2);
        axis.error1 = getCoordError("error1", element, namespace, false);
        axis.error2 = getCoordError("error2", element, namespace, false);
        axis.range = getCoordRange2D("range", element, namespace, false);
        axis.bounds = getCoordBounds2D("bounds", element, namespace, false);
        axis.function = getCoordFunction2D("function", element, namespace,
                false);
        return axis;
    }

    /**
     * Build an CoordBounds1D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordBounds1D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected CoordBounds1D getCoordBounds1D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        CoordBounds1D coordBounds1D = new CoordBounds1D();
        Element samples = getChildElement("samples", element, namespace, false);
        if (samples != null) {
            addChildrenToCoordRange1DList("range", coordBounds1D.getSamples(),
                    samples, namespace, false);
        }
        return coordBounds1D;
    }

    /**
     * Build an CoordBounds2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordBounds2D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected CoordBounds2D getCoordBounds2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        // Look for a CoordCircle2D which has a center and a radius.
        CoordCircle2D circle = getCoordCircle2D("circle", element, namespace,
                false);
        if (circle != null) {
            return circle;
        }

        // Look for a CoordPolygon2D which has a list of Coord2D vertices.
        CoordPolygon2D polygon = getCoordPolygon2D("polygon", element,
                namespace, false);
        if (polygon != null) {
            return polygon;
        }

        // Unknown children.
        String error = "Unsupported element found in " + name + ": "
                + element.getText();
        throw new ObservationParsingException(error);
    }

    /**
     * Build an CoordCircle2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordCircle2D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected CoordCircle2D getCoordCircle2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null) {
            return null;
        }

        // Look for a CoordCircle2D which has a center and a radius.
        ValueCoord2D center = getValueCoord2D("center", element, namespace,
                true);
        Double radius = getChildTextAsDouble("radius", element, namespace,
                true);
        return new CoordCircle2D(center, radius);
    }

    /**
     * Build an CoordError from a JDOM representation of element name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordError, or null if the document doesn't contain element
     *         called name.
     * @throws ObservationParsingException
     */
    protected CoordError getCoordError(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Double syser = getChildTextAsDouble("syser", element, namespace, true);
        Double rnder = getChildTextAsDouble("rnder", element, namespace, true);
        return new CoordError(syser, rnder);
    }

    /**
     * Build an CoordFunction1D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordFunction1D, or null if the document doesn't contain
     *         element named name.
     * @throws ObservationParsingException
     */
    protected CoordFunction1D getCoordFunction1D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Long naxis = getChildTextAsLong("naxis", element, namespace, true);
        Double delta = getChildTextAsDouble("delta", element, namespace, true);
        RefCoord refCoord = getRefCoord("refCoord", element, namespace, true);
        return new CoordFunction1D(naxis, delta, refCoord);
    }

    /**
     * Build an CoordFunction2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordFunction2D, or null if the document doesn't contain
     *         element named name.
     * @throws ObservationParsingException
     */
    protected CoordFunction2D getCoordFunction2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Dimension2D dimension = getDimension2D("dimension", element, namespace,
                true);
        Coord2D refCoord = getCoord2D("refCoord", element, namespace, true);
        double cd11 = getChildTextAsDouble("cd11", element, namespace, true);
        double cd12 = getChildTextAsDouble("cd12", element, namespace, true);
        double cd21 = getChildTextAsDouble("cd21", element, namespace, true);
        double cd22 = getChildTextAsDouble("cd22", element, namespace, true);
        return new CoordFunction2D(dimension, refCoord, cd11, cd12, cd21, cd22);
    }

    /**
     * Build an CoordPolygon2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordPolygon2D, or null if the document doesn't contain
     *         element named name.
     * @throws ObservationParsingException
     */
    protected CoordPolygon2D getCoordPolygon2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null) {
            return null;
        }

        Element vertices = getChildElement("vertices", element, namespace,
                true);
        List children = getChildrenElements("vertex", vertices, namespace,
                true);
        // Vertices must have a minimum of 3 vertexes.
        if (children.size() < 3) {
            String error = "CoordPolygon2D must have a minimum of 3 vertexes, found "
                    + children.size();
            throw new ObservationParsingException(error);
        }

        CoordPolygon2D polygon = new CoordPolygon2D();
        Iterator it = children.iterator();
        while (it.hasNext()) {
            Element vertexElement = (Element) it.next();
            double coord1 = getChildTextAsDouble("coord1", vertexElement,
                    namespace, true);
            double coord2 = getChildTextAsDouble("coord2", vertexElement,
                    namespace, true);
            polygon.getVertices().add(new ValueCoord2D(coord1, coord2));
        }
        return polygon;
    }

    /**
     * Build an CoordRange1D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordRange1D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected CoordRange1D getCoordRange1D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        RefCoord start = getRefCoord("start", element, namespace, true);
        RefCoord end = getRefCoord("end", element, namespace, true);
        return new CoordRange1D(start, end);
    }

    /**
     * Build an CoordRange2D from a JDOM representation of an element named
     * name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an CoordRange2D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected CoordRange2D getCoordRange2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Coord2D start = getCoord2D("start", element, namespace, true);
        Coord2D end = getCoord2D("end", element, namespace, true);
        return new CoordRange2D(start, end);
    }

    /**
     * Build an Dimension2D from a JDOM representation of an element named name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an Dimension2D, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected Dimension2D getDimension2D(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Long naxis1 = getChildTextAsLong("naxis1", element, namespace, true);
        Long naxis2 = getChildTextAsLong("naxis2", element, namespace, true);
        return new Dimension2D(naxis1, naxis2);
    }

    /**
     * Build an RefCoord from a JDOM representation of an element named name.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an RefCoord, or null if the document doesn't contain element
     *         named name.
     * @throws ObservationParsingException
     */
    protected RefCoord getRefCoord(String name, Element parent,
            Namespace namespace, boolean required)
            throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, false);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Double pix = getChildTextAsDouble("pix", element, namespace, true);
        Double val = getChildTextAsDouble("val", element, namespace, true);
        return new RefCoord(pix, val);
    }

    /**
     * Build an Slice from a JDOM representation of an slice element.
     * 
     * @param name
     *            the name of the Element.
     * @param parent
     *            the parent Element.
     * @param namespace
     *            of the document.
     * @param required
     *            is the element expected to be found.
     * @return an Slice, or null if the document doesn't contain an slice
     *         element.
     * @throws ObservationParsingException
     */
    protected Slice getSlice(String name, Element parent, Namespace namespace,
            boolean required) throws ObservationParsingException {
        Element element = getChildElement(name, parent, namespace, required);
        if (element == null || element.getContentSize() == 0) {
            return null;
        }

        Axis axis = getAxis("axis", element, namespace, true);
        Long bin = getChildTextAsLong("bin", element, namespace, true);
        return new Slice(axis, bin);
    }

    // protected String getAttributeValue(String name, Element element, boolean
    // required)
    // throws ObservationParsingException
    // {
    // String value = element.getAttributeValue(name);
    // if (required && value == null)
    // {
    // String error = "Required attribute " + name + " not found in element " +
    // element.getName();
    // throw new ObservationParsingException(error);
    // }
    // return value;
    // }

    protected Element getChildElement(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        Element child = element.getChild(name, ns);
        if (required && child == null) {
            String error = name + " element not found in " + element.getName();
            throw new ObservationParsingException(error);
        }
        return child;
    }

    protected String getChildText(String name, Element element, Namespace ns,
            boolean required) throws ObservationParsingException {
        Element child = getChildElement(name, element, ns, required);
        if (child != null) {
            return cleanWhitespace(child.getText());
        }
        return null;
    }

    protected Boolean getChildTextAsBoolean(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        Element child = getChildElement(name, element, ns, required);
        if (child != null) {
            return Boolean.valueOf(child.getText());
        }
        return null;
    }

    protected Integer getChildTextAsInteger(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        Element child = getChildElement(name, element, ns, required);
        if (child != null) {
            return Integer.valueOf(child.getText());
        }
        return null;
    }

    protected Double getChildTextAsDouble(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        Element child = getChildElement(name, element, ns, required);
        if (child != null) {
            return Double.valueOf(child.getText());
        }
        return null;
    }

    protected Long getChildTextAsLong(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        Element child = getChildElement(name, element, ns, required);
        if (child != null) {
            return Long.valueOf(child.getText());
        }
        return null;
    }

    protected void addChildTextToStringList(String name,
            Collection<String> list, Element element, Namespace ns,
            boolean required) throws ObservationParsingException {
        String child = getChildText(name, element, ns, required);
        if (child == null) {
            return;
        }

        String[] tokens = child.split("[\\s]+");
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            token = cleanWhitespace(token);
            if (!token.isEmpty()) {
                list.add(token);
            }
        }
    }

    protected void addKeywordsToList(Collection<String> list, Element element,
            Namespace ns) throws ObservationParsingException {
        Element kwe = element.getChild("keywords", ns);
        //log.debug("addKeywordsToList: " + kwe);
        if (kwe == null) {
            return;
        }

        List kws = kwe.getChildren("keyword", ns);
        //log.debug("addKeywordsToList: " + kws.size());
        Iterator it = kws.iterator();
        while (it.hasNext()) {
            Element k = (Element) it.next();
            String s = k.getTextTrim();
            //log.debug("addKeywordsToList: " + s);
            list.add(s);
        }
    }

    protected void addChildrenToCoordRange1DList(String name,
            List<CoordRange1D> list, Element element, Namespace ns,
            boolean required) throws ObservationParsingException {
        List children = getChildrenElements(name, element, ns, required);
        Iterator it = children.iterator();
        while (it.hasNext()) {
            Element child = (Element) it.next();
            RefCoord start = getRefCoord("start", child, ns, true);
            RefCoord end = getRefCoord("end", child, ns, true);
            list.add(new CoordRange1D(start, end));
        }
    }

    protected Date getChildTextAsDate(String name, Element element,
            Namespace ns, boolean required, DateFormat dateFormat)
            throws ObservationParsingException {
        String child = getChildText(name, element, ns, required);
        if (child != null) {
            try {
                return DateUtil.flexToDate(child, dateFormat);
            } catch (ParseException ex) {
                String error = "Unable to parse " + name + " in "
                        + element.getName() + " to a date because "
                        + ex.getMessage();
                throw new ObservationParsingException(error, ex);
            }
        }
        return null;
    }

    protected List getChildrenElements(String name, Element element,
            Namespace ns, boolean required) throws ObservationParsingException {
        List children = element.getChildren(name, ns);
        if (required && children.isEmpty()) {
            String error = name + " element not found in " + element.getName();
            throw new ObservationParsingException(error);
        }
        return children;
    }

    protected String cleanWhitespace(String s) {
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.isEmpty()) {
            return null;
        }
        return s;
    }
}
