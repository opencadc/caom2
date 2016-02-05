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
package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.fits.FitsMapper;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.IngestableFile;
import ca.nrc.cadc.caom2.fits.exceptions.IngestException;
import ca.nrc.cadc.caom2.fits.exceptions.MapperException;
import ca.nrc.cadc.caom2.fits.exceptions.PartialWCSException;
import ca.nrc.cadc.caom2.fits.wcs.Energy;
import ca.nrc.cadc.caom2.fits.wcs.Observable;
import ca.nrc.cadc.caom2.fits.wcs.Polarization;
import ca.nrc.cadc.caom2.fits.wcs.Position;
import ca.nrc.cadc.caom2.fits.wcs.Time;
import ca.nrc.cadc.caom2.fits.wcs.Wcs;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.FitsUtil;
import nom.tam.fits.Header;
import nom.tam.fits.TruncatedFileException;
import nom.tam.util.ArrayDataInput;
import org.apache.log4j.Logger;

/**
 * 
 * 
 * @author jburke
 */
public class Ingest implements Runnable
{
    private static Logger log = Logger.getLogger(Ingest.class);

    private String collection;
    private String observationID;
    private String productID;
    private URI[] uris;
    private File[] localFiles;
    private Map<String, String> config;
    private FitsMapping mapping;
    private File in;
    private File out;
    private boolean sslEnabled;

    private boolean dryrun;
    private boolean keepFiles;
    private boolean strictFitsParse = false;
    private boolean ignorePartialWCS = false;
    
    public Ingest(String collection, String observationID, String productID, URI[] uris, Map<String, String> config)
    {
        this.collection = collection;
        this.observationID = observationID;
        this.productID = productID;
        this.uris = uris;
        this.config = config;
        if (collection == null)
            throw new IllegalArgumentException(Argument.COLLECTION + " cannot be null");
        if (observationID == null)
            throw new IllegalArgumentException(Argument.OBSERVATION_ID + " cannot be null");
        if (productID == null)
            throw new IllegalArgumentException(Argument.PRODUCT_ID + " cannot be null");
        if (uris == null)
            throw new IllegalArgumentException(Argument.URI + " cannot be null");
        if (config == null)
            throw new IllegalArgumentException("BUG: config cannot be null");
    }

    public void setDryrun(boolean dryrun)
    {
        this.dryrun = dryrun;
    }
    
    public void setKeepFiles(boolean keepFiles)
    {
        this.keepFiles = keepFiles;
    }

    public void setMapping(FitsMapping mapping)
    {
        this.mapping = mapping;
    }

    public void setStructFitsParse(boolean structFitsParse)
    {
        this.strictFitsParse = structFitsParse;
    }
    
    public void setLocalFiles(File[] localFiles)
    {
        this.localFiles = localFiles;
    }

    public void setInFile(File in)
    {
        this.in = in;
    }

    public void setOutFile(File out)
    {
        this.out = out;
    }

    public void setSSLEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
    }

    public void setIgnorePartialWCS(boolean ignorePartialWCS)
    {
        this.ignorePartialWCS = ignorePartialWCS;
    }

    /**
     * Based on the command line arguments, ingest the files into the database. 
     * 
     */
    public void run()
    {   
        List<IngestableFile> ingestFiles = getIngestFiles();
        
        process(ingestFiles);
    }
        
    /**
     * For each of the uri command line arguments, create an Ingestable File.
     * 
     * @return a List of IngestableFile's.
     */
    protected List<IngestableFile> getIngestFiles()
    {
        List<IngestableFile> ingestFiles = new ArrayList<IngestableFile>();

        for (int i = 0; i < uris.length; i++)
        {
            if (localFiles != null)
                ingestFiles.add(new IngestableFile(uris[i], localFiles[i], sslEnabled));
            else
                ingestFiles.add(new IngestableFile(uris[i], null, sslEnabled));
        }
        return ingestFiles;
    }
    
    /**
     * From the command line arguments, the files to be ingested, and the 
     * FITS mapping, create and persist an Observation.
     * 
     * @param ingestFiles FITS files to be ingested.
     * @throws IngestException 
     */
    protected void process(List<IngestableFile> ingestFiles)
        throws IngestException
    {
        try
        {
            long start = System.currentTimeMillis();

            // Composite or Simple Observation
            String members = mapping.getMapping("CompositeObservation.members");
            boolean simple = members == null ? true : false;
            log.debug("simple " + simple);
            
            // First check the persistence for an existing Observation.
            Observation observation = null;
            if (in != null)
            {
                ObservationReader r = new ObservationReader();
                observation = r.read(new FileReader(in));
            }
            
            if (observation == null)
            {
                if (simple)
                {
                    observation = new SimpleObservation(collection, observationID);
                }
                else
                {
                    String algorithmName = mapping.getMapping("Observation.algorithm.name");
                    Algorithm algorithm = new Algorithm(algorithmName);
                    CompositeObservation co = new CompositeObservation(collection, observationID, algorithm);
                    observation = co;
                }
            }
            
            populateObservation(observation, ingestFiles);
            
            if (!dryrun)
            {
                ObservationWriter w = new ObservationWriter();
                w.write(observation, new FileWriter(out));
            }
            
            long duration = System.currentTimeMillis() - start;
            log.info("Wrote Observation[" + collection + "/" + observationID + "/" + productID + "] to " + out.getName() + " in " + duration + "ms");
        }
        catch (FitsException fe)
        {
            throw new IngestException("Unable to open FITS file", fe);
        }
        catch (MapperException me)
        {
            throw new IngestException(me.getMessage() , me);
        }
        catch (UnsupportedOperationException uoe)
        {
            throw new IngestException(uoe.getMessage(), uoe);
        }
        catch (Throwable t)
        {
            throw new IngestException(t);
        }
    }
            
    /**
     * Populate an Observation including all child classes.
     * 
     * @param observation the Observation to be populated.
     * @param ingestFiles FITS files to be ingested.
     * @throws MapperException
     * @throws FitsException
     * @throws InterruptedException 
     * @throws RuntimeException 
     * @throws IOException 
     * @throws URISyntaxException 
     */
    protected void populateObservation(Observation observation, List<IngestableFile> ingestFiles)
        throws MapperException, FitsException, URISyntaxException, IOException, RuntimeException, InterruptedException
    {
        FitsMapper fitsMapper = new FitsMapper(mapping);
        ListIterator<IngestableFile> iter = ingestFiles.listIterator();
        while ( iter.hasNext() )
        {                
            IngestableFile ingestFile = iter.next();
            iter.remove(); // allow garbage collection when done with this file
            
            // Update the mapping with the URI.
            mapping.uri = ingestFile.getURI().toString();

            // Get the file.
            File file = ingestFile.get();
            if (file == null)
            {
                throw new IngestException("Unable to download file for uri " + ingestFile.getURI());
            }
            log.debug("ingestFile: " + ingestFile);
            // List of FITS header objects.
            List<Header> headers = new ArrayList<Header>();

            // Is this SIMPLE or MEF FITS file.
            boolean isSimpleFITS = true;

            // Load the file into the nom.tam FITS class.
            Fits fits = new Fits(file);

            try
            {
                readHeaders(fits, headers);
                if (headers.isEmpty())
                {
                    throw new IngestException("No headers found in FITS file " + ingestFile.getURI().toString());
                }
                if (headers.get(0) == null)
                {
                    throw new IngestException("Primary header is null in " + file.getAbsolutePath());
                }

                // Save the primary header.
                mapping.primary = headers.get(0);
                mapping.header = headers.get(0);

                // Check if this is a simple FITS file or a MEF.
                isSimpleFITS = isSimpleFITS(headers);
            }
            catch (FitsException fe)
            {
                if (strictFitsParse && isFITSContentType(ingestFile, mapping))
                    throw new IngestException("failed to parse " + file, fe);

                log.info("Not a FITS file: " + file.getAbsolutePath() + "... skipping");

            }

            // Populate the Observation.
            fitsMapper.populate(Observation.class, observation, "Observation");
            if (observation instanceof CompositeObservation)
                fitsMapper.populate(CompositeObservation.class, observation, "CompositeObservation");
            log.debug("Observation.environment: " + observation.environment);

            // Populate an existing or new Plane.
            Plane plane = new Plane(productID);
            for (Plane p : observation.getPlanes())
            {
                if (p.equals(plane))
                {
                    plane = p;
                    break;
                }
            }
            fitsMapper.populate(Plane.class, plane, "Plane");
            boolean insert = observation.getPlanes().add(plane);
            log.debug(insert ? "adding "  + plane : "updating " + plane);

            // Populate an existing or new Artifact.
            Artifact artifact = new Artifact(ingestFile.getURI());
            for (Artifact a : plane.getArtifacts())
            {
                if (a.equals(artifact))
                {
                    artifact = a;
                    break;
                }
            }
            fitsMapper.populate(Artifact.class, artifact, "Artifact");
            insert = plane.getArtifacts().add(artifact);
            log.debug(insert ? "adding " + artifact : "updating " + artifact);

            setContentLength(artifact, ingestFile);
            setContentType(artifact, ingestFile);

            // If FITS file, get details about parts and chunks
            // TODO: we maye have to combine this with the read loop to avoid excessive
            // memory usage
            Integer extension = 0;
            ListIterator<Header> hiter = headers.listIterator();
            while ( hiter.hasNext() )
            {                       
                Header header = hiter.next();
                hiter.remove(); // allow garbage collection asap
                
                
                // Update the mapping with the extension.
                mapping.extension = extension;
                log.debug("ingest: " + artifact.getURI() + "[" + extension + "]");

                // Get the header and update the mapping.
                if (header == null)
                {
                    extension++;
                    continue;
                }
                mapping.header = header;

                // Populate an existing or new Part.
                Part part = buildPart(mapping, extension, isSimpleFITS);
                for (Part p : artifact.getParts())
                {
                    if (p.equals(part))
                    {
                        part = p;
                        break;
                    }
                }
                fitsMapper.populate(Part.class, part, "Part");
                insert = artifact.getParts().add(part);
                log.debug(insert ? "adding "  + part : "updating " + part);


                if ( hasData(mapping) )
                {
                    // Populate an existing or new Chunk.
                    Chunk chunk = new Chunk();
                    if (!part.getChunks().isEmpty())
                    {
                        chunk = part.getChunks().iterator().next();
                        
                        // If more than one Chunk, clear the set.
                        if (part.getChunks().size() > 1)
                        {
                            part.getChunks().clear();
                        }
                    }
                    populateChunk(chunk, mapping);
                    insert = part.getChunks().add(chunk);
                    log.debug(insert ? "adding "  + chunk : "updating " + chunk);
                }
                else
                    log.debug("part " + extension + ": no data");
                
                extension++;
            }

            // Delete the file if it is stored locally.
            if ( !keepFiles )
                ingestFile.delete();

            log.debug("Observation.environment: " + observation.environment);
        }
        log.debug("Observation.environment: " + observation.environment);
    }
    
    /**
     * 
     * @param mapping FITS mapping.
     * @return a Part.
     */
    protected Part buildPart(FitsMapping mapping, Integer extensionNumber, boolean isSimpleFITS)
        throws IngestException
    {
        Part part = null;
        
        // Create Part from partName.
        String partName = mapping.getMapping("Part.name");
        if (partName != null)
        {
            part = new Part(partName);
        }

        // If it's a simple FITS file the name is 0, else use the extension number for a MEF.
        if (part == null)
        {
            if(isSimpleFITS)
            {
                part = new Part("0");
            }
            else if (extensionNumber != null)
            {
                part = new Part(extensionNumber);
            }
        }
        if (part == null)
        {
            throw new IngestException("Unable to build Part");
        }
        return part;
    }
    
    protected void populateChunk(Chunk chunk, FitsMapping mapping)
    {
        // re-initialize the chunk.
        chunk.productType = null;
        chunk.naxis = null;
        chunk.positionAxis1 = null;
        chunk.positionAxis2 = null;
        chunk.energyAxis = null;
        chunk.timeAxis = null;
        chunk.polarizationAxis = null;
        chunk.observableAxis = null;
        chunk.position = null;
        chunk.energy = null;
        chunk.time = null;
        chunk.polarization = null;
        chunk.observable = null;
        
        // ProductType.
        String value = mapping.getMapping("Chunk.productType");
        if (value != null)
        {
            chunk.productType = ProductType.toValue(value);
        }
        
        // naxis.
        value = mapping.getMapping("Chunk.naxis");
        if (value != null)
        {
            chunk.naxis = Integer.valueOf(value);
        }
        Integer wcsaxes = chunk.naxis;
        String s = mapping.getKeywordValue("WCSAXES");
        if (s != null)
        {
            wcsaxes = Integer.parseInt(s);
            log.debug("found WCSAXES = " + wcsaxes + " for " + mapping.uri + "[" + mapping.extension + "]");
        }
        
        Integer[] positionAxis = Wcs.getPositionAxis(wcsaxes, mapping);
        chunk.positionAxis1 = positionAxis[0];
        chunk.positionAxis2 = positionAxis[1];
        chunk.energyAxis = Wcs.getEnergyAxis(wcsaxes, mapping);
        chunk.timeAxis = Wcs.getTimeAxis(wcsaxes, mapping);
        chunk.polarizationAxis = Wcs.getPolarizationAxis(wcsaxes, mapping);
        chunk.observableAxis = Wcs.getObservableAxis(wcsaxes, mapping);
        log.debug("Chunk axes: position="
                + chunk.positionAxis1 + "," + chunk.positionAxis2
                + " energy=" + chunk.energyAxis
                + " time="+chunk.timeAxis
                + " polarization="+chunk.polarizationAxis
                + " observable="+chunk.observableAxis);

        // Update mapping with axes values.
        mapping.positionAxis1 = chunk.positionAxis1;
        mapping.positionAxis2 = chunk.positionAxis2;
        mapping.energyAxis = chunk.energyAxis;
        mapping.timeAxis = chunk.timeAxis;
        mapping.polarizationAxis = chunk.polarizationAxis;
        mapping.observableAxis = chunk.observableAxis;
        
        // Populate the WCS.
        try
        {
            log.debug("ingest: chunk.position for " + mapping.uri + "[" + mapping.extension + "]");
            chunk.position = Position.getPosition("Chunk.position", mapping);
        }
        catch (PartialWCSException e)
        {
            if (ignorePartialWCS)
            {
                chunk.position = null;
                log.info("ignoring partial Position WCS");
            }
            else
            {
                throw new IllegalArgumentException(e);
            }
        }

        try
        {
            log.debug("ingest: chunk.energy for " + mapping.uri + "[" + mapping.extension + "]");
            chunk.energy = Energy.getEnergy("Chunk.energy", mapping);
        }
        catch (PartialWCSException e)
        {
            if (ignorePartialWCS)
            {
                chunk.energy = null;
                log.info("ignoring partial Energy WCS");
            }
            else
            {
                throw new IllegalArgumentException(e);
            }
        }

        try
        {
            log.debug("ingest: chunk.time for " + mapping.uri + "[" + mapping.extension + "]");
            chunk.time = Time.getTime("Chunk.time", mapping);
        }
        catch (PartialWCSException e)
        {
            if (ignorePartialWCS)
            {
                chunk.time = null;
                log.info("ignoring partial Time WCS");
            }
            else
            {
                throw new IllegalArgumentException(e);
            }
        }

        try
        {
            log.debug("ingest: chunk.polarization for " + mapping.uri + "[" + mapping.extension + "]");
            chunk.polarization = Polarization.getPolarization("Chunk.polarization", mapping);
        }
        catch (PartialWCSException e)
        {
            if (ignorePartialWCS)
            {
                chunk.polarization = null;
                log.info("ignoring partial Polarization WCS");
            }
            else
            {
                throw new IllegalArgumentException(e);
            }
        }

        try
        {
            log.debug("ingest: chunk.observable for " + mapping.uri + "[" + mapping.extension + "]");
            chunk.observable = Observable.getObservable("Chunk.observable", mapping);
        }
        catch (PartialWCSException e)
        {
            if (ignorePartialWCS)
            {
                chunk.observable = null;
                log.info("ignoring partial Observable WCS");
            }
            else
            {
                throw new IllegalArgumentException(e);
            }
        }
        log.debug("ingest: chunk DONE");
    }
    
    protected boolean isSimpleFITS(List<Header> headers)
    {
        return headers != null && headers.size() == 1;
    }

    protected boolean hasData(FitsMapping mapping)
    {
        // Only want to process cfitsio compressed images.
        String xtension = mapping.getKeywordValue("XTENSION");
        if (xtension != null && xtension.equals("BINTABLE"))
        {
            String zimage = mapping.getKeywordValue("ZIMAGE");
            if (zimage == null || (zimage != null && !zimage.equals("T")))
            {
                return false;
            }
        }

        long naxis = stringToNum(mapping.getKeywordValue("NAXIS"), 0);
        if (naxis == 0)
        {
            log.debug("hasData: NAXIS=0");
            return false;
        }

        // numBits of data = BITPIX * naxis1 * ... naxisN
        long bitpix = stringToNum(mapping.getKeywordValue("BITPIX"), 0);
        if (bitpix == 0)
        {
            log.debug("hasData: BITPIX=0");
            return false;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("hasData: NAXIS=").append(naxis);
        sb.append(" BITPIX=").append(bitpix);
        long numBits = bitpix;
        if (numBits < 0)
            numBits *= -1;
        for (int i=1; i<=naxis; i++)
        {
            String ni = "NAXIS"+i;
            long naxisI = stringToNum(mapping.getKeywordValue(ni), 0);
            sb.append(" ").append(ni).append("=").append(naxisI);
            numBits *= naxisI;
        }
        log.debug(sb);
        return (numBits > 0L);
    }
    private long stringToNum(String s, long def)
    {
        if (s == null)
            return def;
        s = s.trim();
        try { return Long.parseLong(s); }
        catch(NumberFormatException ignore) { }
        return def;
    }

    /**
     * Checks if the Content-Type of the file is the FITS MIME type
     * application/fits.
     * 
     * @param file the FITS file currently being processed.
     * @param mapping the FitsMapping.
     * @return true is the file is a FITS file, false otherwise.
     */
    protected boolean isFITSContentType(IngestableFile file, FitsMapping mapping)
    {
        boolean isFITS = false;
        
        // See if explicitly set.
        String contentType = mapping.getMapping("Artifact.contentType");
        if (contentType == null)
        {
            // If not check file for Content-Type.
            contentType = file.getContentType();
        }
        
        if (contentType != null && contentType.equals(IngestableFile.FITS_MIME_TYPE))
        {
            isFITS = true;
        }
        log.debug("isFits[" + contentType + "] " + isFITS);
        return isFITS;
    }
    
    /**
     * Sets the contentLength field of the Artifact from the IngestableFile.
     * 
     * @param artifact the Artifact being populated.
     * @param file the FITS file currently being processed.
     */
    protected void setContentLength(Artifact artifact, IngestableFile file)
    {
        artifact.contentLength = null;
        if (file.getContentLength() != -1)
            artifact.contentLength = new Long(file.getContentLength());
        log.debug("Artifact.contentLength = " + artifact.contentLength);
    }
    
    /**
     * Sets the contentType field of the Artifact from the IngestableFile.
     * 
     * @param artifact the Artifact being populated.
     * @param file  the FITS file currently being processed.
     */
    protected void setContentType(Artifact artifact, IngestableFile file)
    {
        artifact.contentType = file.getContentType();
        log.debug("Artifact.contentType = " + artifact.contentType);
    }
    
    protected void readHeaders(Fits fits, List<Header> headers)
        throws FitsException
    {
        ArrayDataInput dataStr = fits.getStream();

        try
        {
            Header header;
            while ((header = Header.readHeader(dataStr)) != null)
            {                
                headers.add(header);
                try
                {
                    dataStr.skipAllBytes(header.getDataSize());
                }
                catch (EOFException ignore) {
                    break;
                }
            }
        }
        catch (TruncatedFileException tfe)
        {
            throw new FitsException("Unexpected end to file", tfe);
        }
        catch (IOException ioe)
        {
            throw new FitsException("Error reading file", ioe);
        }
        finally
        {
            try
            {
                if (dataStr != null)
                    dataStr.close();
            }
            catch (IOException ignore) { }
        }
    }
    
}
