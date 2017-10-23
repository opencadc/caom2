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

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2ops.mapper.ArtifactMapper;
import ca.nrc.cadc.caom2ops.mapper.ChunkMapper;
import ca.nrc.cadc.caom2ops.mapper.ObservationMapper;
import ca.nrc.cadc.caom2ops.mapper.PartMapper;
import ca.nrc.cadc.caom2ops.mapper.PlaneMapper;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.caom2ops.mapper.UnexpectedContentException;
import ca.nrc.cadc.caom2ops.mapper.VOTableUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Performs a TAP query based on a Plane URI, optionally filters the query,
 * and produces a list of tuples.
 * @author pdowler
 */
public class CaomTapQuery 
{
    private static final Logger log = Logger.getLogger(CaomTapQuery.class);
    
    static final String VOTABLE_FORMAT = VOTableWriter.CONTENT_TYPE; // from cadcDALI

    private final URI tapServiceID;
    private final String runID;

    /**
     * LinkQuery constructor.
     * @param runID ID of job invoking this constructor
     * @param tapServiceID resourceID of the TAP service to use
     */
    public CaomTapQuery(URI tapServiceID, String runID)
    {
    	ArgValidator.assertNotNull(getClass(), "tapServiceID", tapServiceID);
        this.tapServiceID = tapServiceID;
    	this.runID = runID;
    }

    /**
     * Get an observation.
     * 
     * @param uri
     * @return an observation
     * @throws IOException
     * @throws UnexpectedContentException
     * @throws AccessControlException
     * @throws CertificateException 
     */
    // use by caom2-meta-server
    public Observation performQuery(final ObservationURI uri)
        throws IOException, UnexpectedContentException, 
            AccessControlException, CertificateException
    {
        
    	log.debug("performing query on observation URI = " + uri.toString());
        
        AdqlQueryGenerator gen = new AdqlQueryGenerator();
        String adql = gen.getADQL(uri);
        log.debug("observation query: " + adql);
        
        VOTableDocument doc = execQuery(uri.getURI().toASCIIString(), adql);
        return buildObservation(doc);
    }
    
    /**
     * 
     * @param uri
     * @param artifactOnly
     * @return artifact query result
     * @throws IOException
     * @throws UnexpectedContentException
     * @throws AccessControlException
     * @throws CertificateException 
     */
    public ArtifactQueryResult performQuery(final PublisherID uri, boolean artifactOnly)
        throws IOException, UnexpectedContentException, 
            AccessControlException, CertificateException
    {
    	log.debug("performing query on plane URI = " + uri.toString() + " artifactOnly=" + artifactOnly);
    	
        // generate query, do not follow redirect
        AdqlQueryGenerator gen = new AdqlQueryGenerator();
        String adql = gen.getADQL(uri, artifactOnly);
        log.debug("link query: " + adql);

        VOTableDocument doc = execQuery(uri.getURI().toASCIIString(), adql);
        ArtifactQueryResult ret = buildArtifacts(doc);
        return ret;
    }
    
    /**
     * Get all artifacts for a plane.
     * 
     * @param uri
     * @param artifactOnly
     * @return artifact query result
     * @throws IOException
     * @throws UnexpectedContentException
     * @throws AccessControlException
     * @throws CertificateException 
     */
    // used by caom2-datalink-server
    public ArtifactQueryResult performQuery(final PlaneURI uri, boolean artifactOnly)
        throws IOException, UnexpectedContentException, 
            AccessControlException, CertificateException
    {
    	log.debug("performing query on plane URI = " + uri.toString() + ", artifactOnly=" + artifactOnly);
    	
        // generate query, do not follow redirect
        AdqlQueryGenerator gen = new AdqlQueryGenerator();
        String adql = gen.getADQL(uri, artifactOnly);
        log.debug("link query: " + adql);

        VOTableDocument doc = execQuery(uri.getURI().toASCIIString(), adql);
    	return buildArtifacts(doc);
    }
    

    /**
     *  Get a single artifact.
     * 
     * @param uri
     * @return an artifact
     * @throws IOException
     * @throws UnexpectedContentException
     * @throws AccessControlException
     * @throws CertificateException 
     */
    // used by caom2-soda-server
    public Artifact performQuery(final URI uri)
        throws IOException, UnexpectedContentException, 
            AccessControlException, CertificateException
    {
    	log.debug("query uri: " + uri.toString());

        AdqlQueryGenerator gen = new AdqlQueryGenerator();
        String adql = gen.getArtifactADQL(uri);
        log.debug("cutout query: " + adql);

        VOTableDocument doc = execQuery(uri.toASCIIString(), adql);
        ArtifactQueryResult ar = buildArtifacts(doc);
        if (ar.getArtifacts().isEmpty())
            return null;
        Artifact a = ar.getArtifacts().get(0);
        log.debug("found: " + a.getURI());
        return a;
    }
    
    private VOTableDocument execQuery(String uri, String adql)
        throws IOException, UnexpectedContentException, 
            AccessControlException, CertificateException
    {
        // obtain credentials fropm CDP if the user is authorized
        AuthMethod queryAuthMethod = AuthMethod.ANON;
        if ( CredUtil.checkCredentials() )
            queryAuthMethod = AuthMethod.CERT;
        

        RegistryClient reg = new RegistryClient();
        URL tapURL = reg.getServiceURL(tapServiceID, Standards.TAP_SYNC_11, queryAuthMethod);
            
        log.debug("post: " + uri + " " + tapURL);
        HttpPost httpPost = new HttpPost(tapURL, getQueryParameters(VOTABLE_FORMAT, adql), false);
        httpPost.run();
        if (httpPost.getThrowable() != null)
            throw new TransientFault("query failed: " + uri, httpPost.getResponseCode(), httpPost.getThrowable());

        log.debug("redirect: " + httpPost.getRedirectURL());
        
        // get
        VOTableStreamReader reader = new VOTableStreamReader(new VOTableReader());
        HttpDownload httpDownload = new HttpDownload(httpPost.getRedirectURL(), (InputStreamWrapper) reader);
        httpDownload.run();
        if (httpDownload.getThrowable() != null)
        {
            throw new TransientFault("query failed: " + uri, httpDownload.getResponseCode(), httpDownload.getThrowable());
        }
        return reader.getVOTable();
    }
    
    private void logHeader(VOTableDocument vot)
    {
        if ( !log.isDebugEnabled() )
            return;
        
        StringBuilder sb = new StringBuilder();
        VOTableResource vr = vot.getResourceByType("results");
        VOTableTable vt = vr.getTable();
        for (VOTableField tf : vt.getFields())
        {
            sb.append("[").append(tf.utype).append("]");
        }
        log.debug("votable header: " + sb.toString());
    }
    
    private void logRow(List<Object> row)
    {
        if ( !log.isDebugEnabled() )
            return;
        
        StringBuilder sb = new StringBuilder();
        for (Object o : row)
        {
            sb.append("[").append(o).append("]");
        }
        log.debug("votable row: " + sb.toString());
    }
    
    private Observation buildObservation(final VOTableDocument vot)
    {
        log.debug("building observation from VOTable");
        VOTableResource vr = vot.getResourceByType("results");
        // TODO: check QUERY_STATUS to be careful and avoid about NPEs below
        VOTableTable vt = vr.getTable();
    	TableData data = vt.getTableData();
    	Iterator<List<Object>> rowIterator = data.iterator();

        Map<String,Integer> utypeMap = VOTableUtil.buildUTypeMap(vt.getFields());
        ObservationMapper om = new ObservationMapper(utypeMap);
        PlaneMapper planeM = new PlaneMapper(utypeMap);
        ArtifactMapper am = new ArtifactMapper(utypeMap);
        PartMapper pm = new PartMapper(utypeMap);
        ChunkMapper cm = new ChunkMapper(utypeMap);
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        
        List<Observation> obs = new ArrayList<Observation>();
        
        Observation curObservation = null;
        Plane curPlane = null;
        Artifact curArtifact = null;
        Part curPart = null;
        
        while (rowIterator.hasNext())
    	{
            List<Object> row = rowIterator.next();
            
            Observation o = om.mapRow(row, df);
            if (o != null)
            {
                if (curObservation == null || !curObservation.getID().equals(o.getID())) // new observation
                {
                    if (curObservation != null) // found first row of next obs
                        log.debug("END observation: " + curObservation.getID());
                    curObservation = o;
                    obs.add(curObservation);
                    log.debug("START observation: " + curObservation.getID());
                }
            }
            else
            {
                log.debug("no observations");
                curObservation = null;
            }
            if (curObservation != null)
            {
                Plane p1 = planeM.mapRow(row, df);
                if (p1 != null)
                {
                    if (curPlane == null || !curPlane.getID().equals(p1.getID())) // new plane
                    {
                        if (curPlane != null)
                            log.debug("END plane: " + curPlane.getID());
                        curPlane = p1;
                        curObservation.getPlanes().add(curPlane);
                        log.debug("START plane: " + curPlane.getID());
                    }
                }
                else
                {
                    log.debug("no planes");
                    curPlane = null;
                }
                if (curPlane != null)
                {
                    Artifact a = am.mapRow(row, df);
                    if (a != null)
                    {
                        if (curArtifact == null || !curArtifact.getID().equals(a.getID())) // new artifact
                        {
                            if (curArtifact != null) // found first row of next artifact in same plane
                                log.debug("END artifact: " + curArtifact.getID());
                            curArtifact = a;
                            curPlane.getArtifacts().add(curArtifact);
                            log.debug("START artifact: " + curArtifact.getID());
                        }
                        // else a == curArtifact
                    }
                    else
                    {
                        log.debug("no artifacts");
                        curArtifact = null;
                    }
                    if (curArtifact != null)
                    {
                        Part p = pm.mapRow(row, df);
                        log.debug("artifact " + curArtifact.getClass() + " part " + p);
                        if (p != null)
                        {
                            if (curPart == null || !curPart.getID().equals(p.getID())) // new part
                            {
                                if (curPart != null) // found first row of next artifact in same plane
                                    log.debug("END part: " + curPart.getID());
                                curPart = p;
                                curArtifact.getParts().add(curPart);
                                log.debug("START part: " + curPart.getID());
                            }
                            // else p == curPart
                        }
                        else
                        {
                            log.debug("artifact: " + curArtifact.getID() + ": no parts");
                            curPart = null;
                        }
                        if (curPart != null)
                        {
                            Chunk c = cm.mapRow(row, df);
                            if (c != null) // always a new chunk since last join
                            {
                                log.debug("START chunk: " + c.getID());
                                curPart.getChunks().add(c);
                            }
                            else
                            {
                                log.debug("part: " + curPart.getID() + ": no chunks");
                            }
                        }                
                    }
                } // end curPlane != null
            }
        }
        if (obs.size() > 1)
            throw new UnexpectedContentException("BUG: found " + obs.size() + " observations, expected 1");
        if (obs.isEmpty())
            return null;
        return obs.get(0);
    }
    
    private ArtifactQueryResult buildArtifacts(final VOTableDocument vot) 
    {
    	log.debug("building artifacts from VOTable");
        //logHeader(votable);

        VOTableResource vr = vot.getResourceByType("results");
        VOTableTable vt = vr.getTable();
    	TableData data = vt.getTableData();
    	Iterator<List<Object>> rowIterator = data.iterator();
    	Artifact curArtifact = null;
        Part curPart = null;

        Map<String,Integer> utypeMap = VOTableUtil.buildUTypeMap(vt.getFields());
        ArtifactMapper am = new ArtifactMapper(utypeMap);
        PartMapper pm = new PartMapper(utypeMap);
        ChunkMapper cm = new ChunkMapper(utypeMap);
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    	
        
        
        ArtifactQueryResult ret = new ArtifactQueryResult();
    	while (rowIterator.hasNext())
    	{
            List<Object> row = rowIterator.next();
            //logRow(row);
            
            Integer mrCol = utypeMap.get("column-name:metaReadable");
            Integer drCol = utypeMap.get("column-name:dataReadable");
            if (mrCol != null)
            {
                Object o = row.get(mrCol);
                ret.metaReadable = (o != null);
            }
            if (drCol != null)
            {
                Object o = row.get(drCol);
                ret.dataReadable = (o != null);
            }
            log.debug("found metaReadable: " + ret.metaReadable + " dataReadable: " + ret.dataReadable);
            
            Artifact a = am.mapRow(row, df);
            if (a != null)
            {
                if (curArtifact == null || !curArtifact.getID().equals(a.getID())) // new artifact
                {
                    if (curArtifact != null) // found first row of next artifact in same plane
                        log.debug("END artifact: " + curArtifact.getID());
                    curArtifact = a;
                    ret.getArtifacts().add(curArtifact);
                    log.debug("START artifact: " + curArtifact.getID());
                }
                // else a == curArtifact
            }
            else
            {
                log.debug("no artifacts");
                curArtifact = null;
            }
            if (curArtifact != null)
            {
                Part p = pm.mapRow(row, df);
                log.debug("artifact " + curArtifact.getClass() + " part " + p);
                if (p != null)
                {
                    if (curPart == null || !curPart.getID().equals(p.getID())) // new part
                    {
                        if (curPart != null) // found first row of next artifact in same plane
                            log.debug("END part: " + curPart.getID());
                        curPart = p;
                        curArtifact.getParts().add(curPart);
                        log.debug("START part: " + curPart.getID());
                    }
                    // else p == curPart
                }
                else
                {
                    log.debug("artifact: " + curArtifact.getID() + ": no parts");
                    curPart = null;
                }
                if (curPart != null)
                {
                    Chunk c = cm.mapRow(row, df);
                    if (c != null) // always a new chunk since last join
                    {
                        log.debug("START chunk: " + c.getID());
                        curPart.getChunks().add(c);
                    }
                    else
                    {
                        log.debug("part: " + curPart.getID() + ": no chunks");
                    }
                }                
            }
    	}
    	    	
    	return ret;
    }
    
    private Map<String, Object> getQueryParameters(final String format, final String adql)
    {
    	Map<String,Object> map = new HashMap<String,Object>();
    	map.put("REQUEST", "doQuery");
    	map.put("LANG", "ADQL");
    	map.put("FORMAT", format);
    	map.put("QUERY", adql);
        if (runID != null)
            map.put("RUNID", runID);
    	return map;
    }
    
    private class VOTableStreamReader implements InputStreamWrapper
    {
    	private VOTableDocument votable;
    	private VOTableReader wrappedReader;
    	
    	
    	public VOTableStreamReader(VOTableReader reader)
    	{
            this.wrappedReader = reader;
    	}
    	
    	public VOTableDocument getVOTable()
    	{
            return this.votable;
    	}

        @Override
        public void read(InputStream inputStream) throws IOException 
        {
            this.votable = this.wrappedReader.read(inputStream);
        }
    }

}
