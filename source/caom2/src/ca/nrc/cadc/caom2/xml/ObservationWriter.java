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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.caom2.xml;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringBuilderWriter;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.ProcessingInstruction;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 *
 * @author jburke
 */
public class ObservationWriter implements Serializable
{
    private static final long serialVersionUID = 201209071030L;
    
    private static Logger log = Logger.getLogger(ObservationWriter.class);

    private String stylesheetURL;
    private boolean writeEmptyCollections;

    private String caom2NamespacePrefix;
    protected transient Namespace caom2Namespace;
    protected transient Namespace xsiNamespace;
    private transient boolean initDone = false;

    /**
     * Default constructor. This uses a standard prefix <code>caom2</code> and does
     * not include empty elements for empty collections.
     */
    public ObservationWriter()
    {
        this("caom2", false);
    }

    /**
     * Constructor. This uses the specified CAOM namespace prefix (null is not allowed).
     * If writeEmptyCollections is true, empty elements will be included for any collections
     * that are empty; this is not necessary but is valid in the schema so is useful for
     * testing.
     *
     * @param caom2NamespacePrefix
     * @param writeEmptyCollections
     */
    public ObservationWriter(String caom2NamespacePrefix, boolean writeEmptyCollections)
    {
        this.writeEmptyCollections = writeEmptyCollections;
        this.caom2NamespacePrefix = caom2NamespacePrefix;
        if ( !StringUtil.hasText(caom2NamespacePrefix))
            throw new IllegalArgumentException("null or 0-length namespace prefix is not allowed: " + caom2NamespacePrefix);
    }

    private void init()
    {
        if (!initDone)
        {
            this.caom2Namespace = Namespace.getNamespace(caom2NamespacePrefix, XmlConstants.CAOM2_NAMESPACE);
            this.xsiNamespace = Namespace.getNamespace("xsi", XmlConstants.XMLSCHEMA);
            this.initDone = true;
        }
    }
    
    /**
     * 
     * @param stylesheetURL 
     */
    public void setStylesheetURL(String stylesheetURL)
    {
        this.stylesheetURL = stylesheetURL;
    }
    
    /**
     * 
     * @return 
     */
    public String getStylesheetURL()
    {
        return stylesheetURL;
    }
    
    /**
     * Set whether to write an empty Element when a java Collection is empty.
     * The default behavior is not to not write empty Collection elements.
     * 
     * @param b true to write elements for empty Collections,
     *          false to not write empty Collections.
     */
    public void setWriteEmptyCollections(boolean b)
    {
        writeEmptyCollections = b;
    }
    
    public boolean getWriteEmptyCollections()
    {
        return writeEmptyCollections;
    }
    
    /**
     * Write an Observation to an OutputStream using UTF-8 encoding.
     *
     * @param obs Observation to write.
     * @param out OutputStream to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Observation obs, OutputStream out)
        throws IOException
    {
        OutputStreamWriter outWriter;
        try
        {
            outWriter = new OutputStreamWriter(out, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("UTF-8 encoding not supported", e);
        }
        write(obs, outWriter);
    }

    /**
     * Write an Observation to a StringBuilder.
     * @param obs Observation to write.
     * @param builder StringBuilder to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Observation obs, StringBuilder builder)
        throws IOException
    {
        write(obs, new StringBuilderWriter(builder));
    }

    /**
     * Write an Observation to a Writer.
     * @param obs Observation to write.
     * @param builder Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Observation obs, Writer writer)
        throws IOException
    {
        init();
        long start = System.currentTimeMillis();
        Element root = getRootElement(obs);
        write(root, writer);
        long end = System.currentTimeMillis();
        log.debug("Write elapsed time: " + (end - start) + "ms");
    }
    
    /**
     *  Build the root Element of a Observation.
     *
     * @param obs Observation.
     * @return root an Observation Element.
     */
    protected Element getRootElement(Observation obs)
    {
        // Create the root element (observation).
        Element root = getObservationElement(obs);
        root.addNamespaceDeclaration(caom2Namespace);
        root.addNamespaceDeclaration(xsiNamespace);
        return root;
    }

    private void addEntityAttributes(CaomEntity ce, Element el, DateFormat df)
    {
        el.setAttribute("id", CaomUtil.uuidToLong(ce.getID()).toString(), caom2Namespace);
        if (ce.getLastModified() != null)
            el.setAttribute("lastModified", df.format(ce.getLastModified()), el.getNamespace());
    }
    
    /**
     * Builds a JDOM representation of an Observation.
     *
     * @param obs The Observation.
     * @return a JDOM Element representing the Observation.
     */
    protected Element getObservationElement(Observation obs)
    {
        // IVOA DateFormat.
        DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            
        Element element = getCaom2Element("Observation");
        String type = caom2Namespace.getPrefix() + ":" + obs.getClass().getSimpleName();
        element.setAttribute("type", type, xsiNamespace);

        addEntityAttributes(obs, element, dateFormat);
        
        // collection and observationID.
        addElement("collection", obs.getCollection(), element);
        addElement("observationID", obs.getObservationID(), element);

        // Observation elements.
        addDateElement("metaRelease", obs.metaRelease, element, dateFormat);
        addNumberElement("sequenceNumber", obs.sequenceNumber, element);
        addAlgorithmElement(obs.getAlgorithm(), element, dateFormat);
        addElement("type", obs.type, element);
        if (obs.intent != null)
            addElement("intent", obs.intent.getValue(), element);
        addProposalElement(obs.proposal, element, dateFormat);
        addTargetElement(obs.target, element, dateFormat);
        addTargetPositionElement(obs.targetPosition, element, dateFormat);
        addTelescopeElement(obs.telescope, element, dateFormat);
        addInstrumentElement(obs.instrument, element, dateFormat);
        addEnvironmentElement(obs.environment, element, dateFormat);
        addPlanesElement(obs.getPlanes(), element, dateFormat);
        
        // Members must be the last element.
        if (obs instanceof CompositeObservation)
        {
            addMembersElement(((CompositeObservation) obs).getMembers(), element, dateFormat);
        }

        return element;
    }
    
    /**
     * Builds a JDOM representation of an Algorithm and adds it to the
     * parent element.
     *
     * @param algorithm The Algorithm to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addAlgorithmElement(Algorithm algorithm, Element parent, DateFormat dateFormat)
    {
        if (algorithm == null)
            return;
        
        Element element = getCaom2Element("algorithm");
        addElement("name", algorithm.getName(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an Proposal and adds it to the
     * parent element.
     *
     * @param proposal The Proposal to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addProposalElement(Proposal proposal, Element parent, DateFormat dateFormat)
    {
        if (proposal == null)
            return;
        
        Element element = getCaom2Element("proposal");
        addElement("id", proposal.getID(), element);
        addElement("pi", proposal.pi, element);
        addElement("project", proposal.project, element);
        addElement("title", proposal.title, element);
        addStringListElement("keywords", proposal.getKeywords(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an Target and adds it to the
     * parent element.
     *
     * @param target The Target to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addTargetElement(Target target, Element parent, DateFormat dateFormat)
    {
        if (target == null)
            return;
        
        Element element = getCaom2Element("target");
        addElement("name", target.getName(), element);
        if (target.type != null)
            addElement("type", target.type.getValue(), element);
        addBooleanElement("standard", target.standard, element);
        addNumberElement("redshift", target.redshift, element);
        addBooleanElement("moving", target.moving, element);
        addStringListElement("keywords", target.getKeywords(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an TargetPosition and adds it to the
     * parent element.
     *
     * @param target The Target to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addTargetPositionElement(TargetPosition targetPosition, Element parent, DateFormat dateFormat)
    {
        if (targetPosition == null)
            return;
        
        Element element = getCaom2Element("targetPosition");
        Element coordsys = getCaom2Element("coordsys");
        coordsys.addContent(targetPosition.getCoordsys());
        element.addContent(coordsys);
        if (targetPosition.equinox != null)
        {
            Element equinox = getCaom2Element("equinox");
            equinox.addContent(targetPosition.equinox.toString());
            element.addContent(equinox);
        }
        Element coords = getCaom2Element("coordinates");
        addNumberElement("cval1", targetPosition.getCoordinates().cval1, coords);
        addNumberElement("cval2", targetPosition.getCoordinates().cval2, coords);
        element.addContent(coords);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an Telescope and adds it to the
     * parent element.
     *
     * @param telescope The Telescope to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addTelescopeElement(Telescope telescope, Element parent, DateFormat dateFormat)
    {
        if (telescope == null)
            return;
        
        Element element = getCaom2Element("telescope");
        addElement("name", telescope.getName(), element);
        addNumberElement("geoLocationX", telescope.geoLocationX, element);
        addNumberElement("geoLocationY", telescope.geoLocationY, element);
        addNumberElement("geoLocationZ", telescope.geoLocationZ, element);
        addStringListElement("keywords", telescope.getKeywords(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an Instrument and adds it to the
     * parent element.
     *
     * @param instrument The Instrument to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addInstrumentElement(Instrument instrument, Element parent, DateFormat dateFormat)
    {
        if (instrument == null)
            return;
        
        Element element = getCaom2Element("instrument");
        addElement("name", instrument.getName(), element);
        addStringListElement("keywords", instrument.getKeywords(), element);
        parent.addContent(element);
    }

    /**
     * Builds a JDOM representation of an Environment and adds it to the
     * parent element.
     *
     * @param environment The Environment to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addEnvironmentElement(Environment environment, Element parent, DateFormat dateFormat)
    {
        if (environment == null)
            return;

        Element element = getCaom2Element("environment");
        addNumberElement("seeing", environment.seeing, element);
        addNumberElement("humidity", environment.humidity, element);
        addNumberElement("elevation", environment.elevation, element);
        addNumberElement("tau", environment.tau, element);
        addNumberElement("wavelengthTau", environment.wavelengthTau, element);
        addNumberElement("ambientTemp", environment.ambientTemp, element);
        addBooleanElement("photometric", environment.photometric, element);
        parent.addContent(element);
    }

    /**
     * Builds a JDOM representation of a Set of ObservationURI and adds it to the
     * parent element.
     *
     * @param members The Set of ObservationURI to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addMembersElement(Set<ObservationURI> members, Element parent, DateFormat dateFormat)
    {        
        if (members == null || (members.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("members");
        for (ObservationURI member : members)
        {
            addURIElement("observationURI", member.getURI(), element);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Set of Plane's and adds it to the
     * parent element.
     *
     * @param planes The Set of Plane's to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addPlanesElement(Set<Plane> planes, Element parent, DateFormat dateFormat)
    {
        if (planes == null || (planes.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("planes");
        for (Plane plane : planes)
        {
            Element planeElement = getCaom2Element("plane");
            addEntityAttributes(plane, planeElement, dateFormat);
            addElement("productID", plane.getProductID(), planeElement);
            addDateElement("metaRelease", plane.metaRelease, planeElement, dateFormat);
            addDateElement("dataRelease", plane.dataRelease, planeElement, dateFormat);
            if (plane.dataProductType != null)
            {
                addElement("dataProductType", plane.dataProductType.getValue(), planeElement);
            }
            if (plane.calibrationLevel != null)
            {
                addElement("calibrationLevel", String.valueOf(plane.calibrationLevel.getValue()), planeElement);
            }
            addProvenanceElement(plane.provenance, planeElement, dateFormat);
            addMetricsElement(plane.metrics, planeElement, dateFormat);
            addArtifactsElement(plane.getArtifacts(), planeElement, dateFormat);
            element.addContent(planeElement);
        }
        parent.addContent(element);
    }

    /**
     * Builds a JDOM representation of an Telescope and adds it to the
     * parent element.
     *
     * @param telescope The Telescope to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addProvenanceElement(Provenance provenance, Element parent, DateFormat dateFormat)
    {
        if (provenance == null)
            return;
    
        Element element = getCaom2Element("provenance");
        addElement("name", provenance.getName(), element);
        addElement("version", provenance.version, element);
        addElement("project", provenance.project, element);
        addElement("producer", provenance.producer, element);
        addElement("runID", provenance.runID, element);
        addURIElement("reference", provenance.reference, element);
        addDateElement("lastExecuted", provenance.lastExecuted, element, dateFormat);
        addStringListElement("keywords", provenance.getKeywords(), element);
        addInputsElement(provenance.getInputs(), element, dateFormat);
        parent.addContent(element);
    }

    protected void addMetricsElement(Metrics metrics, Element parent, DateFormat dateFormat)
    {
        if (metrics == null)
            return;

        Element element = getCaom2Element("metrics");
        addNumberElement("sourceNumberDensity", metrics.sourceNumberDensity, element);
        addNumberElement("background", metrics.background, element);
        addNumberElement("backgroundStddev", metrics.backgroundStddev, element);
        addNumberElement("fluxDensityLimit", metrics.fluxDensityLimit, element);
        addNumberElement("magLimit", metrics.magLimit, element);
        parent.addContent(element);
    }

    protected void addTransitionElement(EnergyTransition transition, Element parent, DateFormat dateFormat)
    {
        if (transition == null)
            return;

        Element element = getCaom2Element("transition");
        addElement("species", transition.getSpecies(), element);
        addElement("transition", transition.getTransition(), element);
        parent.addContent(element);
    }

    /**
     * Builds a JDOM representation of a Set of PlaneURI and adds it to the
     * parent element.
     *
     * @param members The Set of PlaneURI to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addInputsElement(Set<PlaneURI> inputs, Element parent, DateFormat dateFormat)
    {
        if (inputs == null || (inputs.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("inputs");
        for (PlaneURI input : inputs)
        {
            addURIElement("planeURI", input.getURI(), element);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Set of Artifact's and adds it to the
     * parent element.
     *
     * @param artifacts The Set of Artifact's to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addArtifactsElement(Set<Artifact> artifacts, Element parent, DateFormat dateFormat)
    {
        if (artifacts == null || (artifacts.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("artifacts");
        for (Artifact artifact : artifacts)
        {
            Element artifactElement = getCaom2Element("artifact");
            addEntityAttributes(artifact, artifactElement, dateFormat);
            addURIElement("uri", artifact.getURI(), artifactElement);
            addElement("contentType", artifact.contentType, artifactElement);
            addNumberElement("contentLength", artifact.contentLength, artifactElement);
            if (artifact.productType != null)
            {
                addElement("productType", artifact.productType.getValue(), artifactElement);
            }
            addBooleanElement("alternative", artifact.alternative, artifactElement);
            addPartsElement(artifact.getParts(), artifactElement, dateFormat);
            element.addContent(artifactElement);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Set of Part's and adds it to the
     * parent element.
     *
     * @param parts The Set of Part's to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addPartsElement(Set<Part> parts, Element parent, DateFormat dateFormat)
    {
        if (parts == null || (parts.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("parts");
        for (Part part : parts)
        {
            Element partElement = getCaom2Element("part");
            addEntityAttributes(part, partElement, dateFormat);
            addElement("name", part.getName(), partElement);
            if (part.productType != null)
            {
                addElement("productType", part.productType.getValue(), partElement);
            }
            addChunksElement(part.getChunks(), partElement, dateFormat);
            element.addContent(partElement);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Set of Chunk's and adds it to the
     * parent element.
     *
     * @param chunks The Set of Chunk's to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addChunksElement(Set<Chunk> chunks, Element parent, DateFormat dateFormat)
    {
        if (chunks == null || (chunks.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = getCaom2Element("chunks");
        for (Chunk chunk : chunks)
        {
            Element chunkElement = getCaom2Element("chunk");
            addEntityAttributes(chunk, chunkElement, dateFormat);
            if (chunk.productType != null)
            {
                addElement("productType", chunk.productType.getValue(), chunkElement);
            }
            addNumberElement("naxis", chunk.naxis, chunkElement);
            addNumberElement("observableAxis", chunk.observableAxis, chunkElement);
            addNumberElement("positionAxis1", chunk.positionAxis1, chunkElement);
            addNumberElement("positionAxis2", chunk.positionAxis2, chunkElement);
            addNumberElement("energyAxis", chunk.energyAxis, chunkElement);
            addNumberElement("timeAxis", chunk.timeAxis, chunkElement);
            addNumberElement("polarizationAxis", chunk.polarizationAxis, chunkElement);
            
            addObservableAxisElement(chunk.observable, chunkElement, dateFormat);
            addSpatialWCSElement(chunk.position, chunkElement, dateFormat);
            addSpectralWCSElement(chunk.energy, chunkElement, dateFormat);
            addTemporalWCSElement(chunk.time, chunkElement, dateFormat);
            addPolarizationWCSElement(chunk.polarization, chunkElement, dateFormat);
            
            element.addContent(chunkElement);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of an ObservableAxis and adds it to the
     * parent element.
     *
     * @param observable The ObservableAxis to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addObservableAxisElement(ObservableAxis observable, Element parent, DateFormat dateFormat)
    {
        if (observable == null)
            return;
    
        Element element = getCaom2Element("observable");
        addSliceElement("dependent", observable.getDependent(), element);
        addSliceElement("independent", observable.independent, element);

        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a SpatialWCS and adds it to the
     * parent element.
     *
     * @param position The SpatialWCS to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addSpatialWCSElement(SpatialWCS position, Element parent, DateFormat dateFormat)
    {
        if (position == null)
            return;
    
        Element element = getCaom2Element("position");
        addCoordAxis2DElement("axis", position.getAxis(), element);
        addElement("coordsys", position.coordsys, element);
        addNumberElement("equinox", position.equinox, element);
        addNumberElement("resolution", position.resolution, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a SpectralWCS and adds it to the
     * parent element.
     *
     * @param energy The SpectralWCS to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addSpectralWCSElement(SpectralWCS energy, Element parent, DateFormat dateFormat)
    {
        if (energy == null)
            return;
    
        Element element = getCaom2Element("energy");
        addCoordAxis1DElement("axis", energy.getAxis(), element);
        addElement("specsys", energy.getSpecsys(), element);
        addElement("ssysobs", energy.ssysobs, element);
        addElement("ssyssrc", energy.ssyssrc, element);
        addNumberElement("restfrq", energy.restfrq, element);
        addNumberElement("restwav", energy.restwav, element);
        addNumberElement("velosys", energy.velosys, element);
        addNumberElement("zsource", energy.zsource, element);
        addNumberElement("velang", energy.velang, element);
        addElement("bandpassName", energy.bandpassName, element);
        addNumberElement("resolvingPower", energy.resolvingPower, element);
        addTransitionElement(energy.transition, element, dateFormat);
        parent.addContent(element);
    }

    /**
     * Builds a JDOM representation of a TemporalWCS and adds it to the
     * parent element.
     *
     * @param time The TemporalWCS to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addTemporalWCSElement(TemporalWCS time, Element parent, DateFormat dateFormat)
    {
        if (time == null)
            return;
    
        Element element = getCaom2Element("time");
        addCoordAxis1DElement("axis", time.getAxis(), element);
        addElement("timesys", time.timesys, element);
        addElement("trefpos", time.trefpos, element);
        addNumberElement("mjdref", time.mjdref, element);
        addNumberElement("exposure", time.exposure, element);
        addNumberElement("resolution", time.resolution, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a PolarizationWCS and adds it to the
     * parent element.
     *
     * @param polarization The PolarizationWCS to add to the parent.
     * @param parent The parent element for this child element.
     * @param the IVOA DateFormat.
     */
    protected void addPolarizationWCSElement(PolarizationWCS polarization, Element parent, DateFormat dateFormat)
    {
        if (polarization == null)
            return;
    
        Element element = getCaom2Element("polarization");
        addCoordAxis1DElement("axis", polarization.getAxis(), element);
        parent.addContent(element);
    }
    
    /*
     * WCS Types
     */

    /**
     * Builds a JDOM representation of a Axis and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param axis The Axis to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addAxisElement(String name, Axis axis, Element parent)
    {
        if (axis == null)
            return;
        
        Element element = getCaom2Element(name);
        addElement("ctype", axis.getCtype(), element);
        addElement("cunit", axis.getCunit(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Coord2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param coord The Coord2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoord2DElement(String name, Coord2D coord, Element parent)
    {
        if(coord == null)
            return;
        
        Element element = getCaom2Element(name);
        addRefCoordElement("coord1", coord.getCoord1(), element);
        addRefCoordElement("coord2", coord.getCoord2(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a ValueCoord2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param coord The ValueCoord2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addValueCoord2DElement(String name, ValueCoord2D coord, Element parent)
    {
        if(coord == null)
            return;
        
        Element element = getCaom2Element(name);
        addNumberElement("coord1", coord.coord1, element);
        addNumberElement("coord2", coord.coord2, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordAxis1D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param axis The CoordAxis1D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordAxis1DElement(String name, CoordAxis1D axis, Element parent)
    {
        if (axis == null)
            return;
        
        Element element = getCaom2Element(name);
        addAxisElement("axis", axis.getAxis(), element);
        addCoordErrorElement("error", axis.error, element);
        addCoordRange1DElement("range", axis.range, element);
        addCoordBounds1DElement("bounds", axis.bounds, element);
        addCoordFunction1DElement("function", axis.function, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordAxis2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param axis The CoordAxis2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordAxis2DElement(String name, CoordAxis2D axis, Element parent)
    {
        if (axis == null)
            return;
        
        Element element = getCaom2Element(name);
        addAxisElement("axis1", axis.getAxis1(), element);
        addAxisElement("axis2", axis.getAxis2(), element);
        addCoordErrorElement("error1", axis.error1, element);
        addCoordErrorElement("error2", axis.error2, element);
        addCoordRange2DElement("range", axis.range, element);
        addCoordBounds2DElement("bounds", axis.bounds, element);
        addCoordFunction2DElement("function", axis.function, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordBounds1D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param bounds The CoordBounds1D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordBounds1DElement(String name, CoordBounds1D bounds, Element parent)
    {
        if (bounds == null)
            return;
        
        Element element = getCaom2Element(name);
        addCoordRange1DListElement("samples", bounds.getSamples(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordBounds2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param bounds The CoordBounds2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordBounds2DElement(String name, CoordBounds2D bounds, Element parent)
    {
        if (bounds == null)
            return;
        
        Element element = getCaom2Element(name);
        if (bounds instanceof CoordCircle2D)
            addCoordCircle2DElement("circle", (CoordCircle2D) bounds, element);
        else if (bounds instanceof CoordPolygon2D)
            addCoordPolygon2DElement("polygon", (CoordPolygon2D) bounds, element);
        else
            throw new IllegalStateException("BUG: unsupported CoordBounds2D type " + bounds.getClass().getSimpleName());
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordCircle2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param circle The CoordCircle2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordCircle2DElement(String name, CoordCircle2D circle, Element parent)
    {
        if (circle == null)
            return;
        
        Element element = getCaom2Element(name);
        addValueCoord2DElement("center", circle.getCenter(), element);
        addNumberElement("radius", circle.getRadius(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordError and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param error The CoordError to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordErrorElement(String name, CoordError error, Element parent)
    {
        if (error == null)
            return;
        
        Element element = getCaom2Element(name);
        addNumberElement("syser", error.syser, element);
        addNumberElement("rnder", error.rnder, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordFunction1D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param function The CoordFunction1D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordFunction1DElement(String name, CoordFunction1D function, Element parent)
    {
        if (function == null)
            return;
        
        Element element = getCaom2Element(name);
        addNumberElement("naxis", function.getNaxis(), element);
        addNumberElement("delta", function.getDelta(), element);
        addRefCoordElement("refCoord", function.getRefCoord(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordFunction2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param function The CoordFunction2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordFunction2DElement(String name, CoordFunction2D function, Element parent)
    {
        if (function == null)
            return;
        
        Element element = getCaom2Element(name);
        addDimension2DElement("dimension", function.getDimension(), element);
        addCoord2DElement("refCoord", function.getRefCoord(), element);
        addNumberElement("cd11", function.getCd11(), element);
        addNumberElement("cd12", function.getCd12(), element);
        addNumberElement("cd21", function.getCd21(), element);
        addNumberElement("cd22", function.getCd22(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordPolygon2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param polygon The CoordPolygon2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordPolygon2DElement(String name, CoordPolygon2D polygon, Element parent)
    {
        if (polygon == null)
            return;
        
        Element element = getCaom2Element(name);
        if (!polygon.getVertices().isEmpty())
        {
            Element verticesElement = getCaom2Element("vertices");
            element.addContent(verticesElement);
            for (ValueCoord2D vertex : polygon.getVertices())
            {
                addValueCoord2DElement("vertex", vertex, verticesElement);
            }
        }
        parent.addContent(element);
    }   
    
    /**
     * Builds a JDOM representation of a CoordRange1D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param range The CoordRange1D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordRange1DElement(String name, CoordRange1D range, Element parent)
    {
        if (range == null)
            return;
        
        Element element = getCaom2Element(name);
        addRefCoordElement("start", range.getStart(), element);
        addRefCoordElement("end", range.getEnd(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a CoordRange2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param range The CoordRange2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addCoordRange2DElement(String name, CoordRange2D range, Element parent)
    {
        if (range == null)
            return;
        
        Element element = getCaom2Element(name);
        addCoord2DElement("start", range.getStart(), element);
        addCoord2DElement("end", range.getEnd(), element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Dimension2D and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param dimension The Dimension2D to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addDimension2DElement(String name, Dimension2D dimension, Element parent)
    {
        if (dimension == null)
            return;
        
        Element element = getCaom2Element(name);
        addNumberElement("naxis1", dimension.naxis1, element);
        addNumberElement("naxis2", dimension.naxis2, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a RefCoord and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param refCoord The RefCoord to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addRefCoordElement(String name, RefCoord refCoord, Element parent)
    {
        if (refCoord == null)
            return;
        
        Element element = getCaom2Element(name);
        addNumberElement("pix", refCoord.pix, element);
        addNumberElement("val", refCoord.val, element);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of a Slice and adds it to the
     * parent element.
     * 
     * @param name The name of the element.
     * @param slice The Slice to add to the parent.
     * @param parent The parent element for this child element.
     */
    protected void addSliceElement(String name, Slice slice, Element parent)
    {
        if (slice == null)
            return;
        
        Element element = getCaom2Element(name);
        addAxisElement("axis", slice.getAxis(), element);
        addNumberElement("bin", slice.getBin(), element);
        parent.addContent(element);
    }
    
    /*
     * General methods for adding Elements.
     */

    /**
     * Builds a JDOM representation Element with the given name and sets
     * the text to the given value, then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param value The value for the element.
     * @param parent The parent element for this child element.
     */
    protected void addElement(String name, String value, Element parent)
    {
        if (value == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        element.setText(value);
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation Element with the given name and sets
     * the text to the given value, then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param number The value for the element.
     * @param parent The parent element for this child element.
     */
    protected void addNumberElement(String name, Number number, Element parent)
    {
        if (number == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        element.setText(number.toString());
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation Element with the given name and sets
     * the text to true if the value is true, else sets the text to false,
     * then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param value The boolean value for the element.
     * @param parent The parent element for this child element.
     */
    protected void addBooleanElement(String name, Boolean value, Element parent)
    {
        if (value == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        element.setText(value ? "true" : "false");
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation Element with the given name and sets
     * the text to the given value, then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param uri The URI value for the element.
     * @param parent The parent element for this child element.
     */
    protected void addURIElement(String name, URI uri, Element parent)
    {
        if (uri == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        element.setText(uri.toString());
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation Element with the given name and adds
     * space delimited List values as the text, then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param values The List of Strings for the element.
     * @param parent The parent element for this child element.
     */
    protected void addStringListElement(String name, List<String> values, Element parent)
    {
        if (values == null || (values.isEmpty() && !writeEmptyCollections))
            return;
        
        Element element = new Element(name, caom2Namespace);
        StringBuilder sb = new StringBuilder();
        for (String value : values)
        {
            sb.append(value).append(" ");
        }
        element.setText(sb.toString().trim());
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation Element with the given name and adds
     * space delimited List values as the text, then adds the element to the parent.
     * 
     * @param name The name of the element.
     * @param values The List of CoordRange1D for the element.
     * @param parent The parent element for this child element.
     */
    protected void addCoordRange1DListElement(String name, List<CoordRange1D> values, Element parent)
    {
        if (values == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        for (CoordRange1D range : values)
        {
            addCoordRange1DElement("range", range, element);
        }
        parent.addContent(element);
    }
    
    /**
     * Builds a JDOM representation of the Date in IVOA format and adds
     * it to the Observation element.
     * 
     * @param name The name of the element.
     * @param date The Date for the element.
     * @param parent The parent element for this child element.
     * @param dateFormat  IVOA DateFormat.
     */
    protected void addDateElement(String name, Date date, Element parent, DateFormat dateFormat)
    {
        if (date == null)
            return;
        
        Element element = new Element(name, caom2Namespace);
        element.setText(dateFormat.format(date));
        parent.addContent(element);
    }
    
    private Element getCaom2Element(String name)
    {
        return new Element(name, caom2Namespace);
    }
    
    /**
     * Write the root Element to a writer.
     *
     * @param root Root Element to write.
     * @param writer Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    @SuppressWarnings("unchecked")
    protected void write(Element root, Writer writer) throws IOException
    {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        if (stylesheetURL != null)
        {
            Map<String, String> instructionMap = new HashMap<String, String>(2);
            instructionMap.put("type", "text/xsl");
            instructionMap.put("href", stylesheetURL);
            ProcessingInstruction pi = new ProcessingInstruction("xml-stylesheet", instructionMap);
            document.getContent().add(0, pi);
        }
        outputter.output(document, writer);
    }
}
