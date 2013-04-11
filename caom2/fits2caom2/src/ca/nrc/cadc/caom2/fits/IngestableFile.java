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
package ca.nrc.cadc.caom2.fits;

import ca.nrc.cadc.ad.AdSchemeHandler;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class IngestableFile
{
    private static Logger log = Logger.getLogger(IngestableFile.class);
    
    // 
    public static final String FITS_MIME_TYPE = "application/fits";
    
    // Temporary directory path.
    public static final String TEMP_DEFAULT = "/tmp";

    private URI uri;
    private File localFile;
    private boolean sslEnabled;

    private File file;
    private String tmpDirPath;
    private String contentType;
    private String contentEncoding;
    private long contentLength = -1;

    /**
     * Default constructor takes a URI, which is either an AD fileID,
     * or a path to a local file, and a boolean indicating the file
     * is on the local file system.
     *
     * @param uri URI to the file
     * @param just use local file
     * @param sslEnabled certificates are available for a secure download.
     * @throws ca.nrc.cadc.fits2caom2.exceptions.IngestException
     */
    public IngestableFile(URI uri, File localFile, boolean sslEnabled)
    {
        this.uri = uri;
        this.localFile = localFile;
        this.sslEnabled = sslEnabled;
        this.tmpDirPath = TEMP_DEFAULT;
    }

    /**
     * Get the URI for this file.
     * 
     * @return URI
     */
    public URI getURI()
    {        
        return uri;
    }
    
    /**
     * Set the path to the directory used to store files downloaded from AD.
     * 
     * @param dirPath path to the temporary directory.
     */
    public void setTempDirPath(String dirPath)
    {
        tmpDirPath = dirPath;
    }
    
    /**
     * Get the path to the temporary directory for downloaded files.
     * 
     * @return path to temporary directory.
     */
    public String getTempDirPath()
    {
        return tmpDirPath;
    }
    
    /**
     * Deletes the file if it was stored locally.
     */
    public void delete()
    {
        if (localFile == null && file != null) // downloaded
        {
            file.delete();
        }
    }

    /**
     * If the URI is local, return a File object for the fileID,
     * else get the file from either AD or VOSpace to the local 
     * file system and return a File object.
     * 
     * @return File given by the URI.
     * @throws InterruptedException 
     * @throws RuntimeException 
     * @throws IOException 
     * @throws URISyntaxException 
     * @throws ca.nrc.cadc.fits2caom.exceptions.IngestException
     */
    public File get() 
    	throws URISyntaxException, IOException, RuntimeException, InterruptedException
    {
        if (file != null)
        {
            return file;
        }
        
        String op = "download ";
        boolean headOnly = false;
        if (localFile != null)
        {
            op = "metadata check ";
            headOnly = true;
        }

        URL url = null;
        AdSchemeHandler schemeHandler = null;
        if (uri.getScheme().equalsIgnoreCase("vos"))
            // use VOSpace API to get the URL
            url = getFromVOSpace();
        else
        {
            // Anonymous HTTP download
            schemeHandler = new AdSchemeHandler();
            url = schemeHandler.getURL(uri, false);
        }
        
        log.info(op + uri + " -- trying " + url);
        HttpDownload download = doDownload(url, op, headOnly);
        
        if (download.getThrowable() != null && 
            download.getThrowable() instanceof AccessControlException)
        {
            if (sslEnabled && uri.getScheme().equalsIgnoreCase("ad"))
            {
                // Authenticated HTTPS download
                url = schemeHandler.getURL(uri, true);
            }
            
            log.info(op + uri + " -- trying " + url);
            download = doDownload(url, op, headOnly);            
        }

        if (download.getThrowable() != null)
        {
            throw new RuntimeException(op + url + " failed", download.getThrowable());
        }

        this.contentType = download.getContentType();
        this.contentLength = download.getContentLength();
        this.contentEncoding = download.getContentEncoding();
        log.debug("contentType = " + contentType);
        log.debug("contentLength = " + contentLength);
        log.debug("contentEncoding = " + contentEncoding);

        if (localFile != null)
            this.file = localFile;
        else
            this.file = download.getFile();

        return file;
    }
  
    /**
     * Get the content-type returned by the server.
     * @return
     */
    public String getContentType()
    {
        return contentType;
    }

    /**
     * Get the size of the download (the Content-Length).
     *
     * @return the content-length or -1 if unknown
     */
    public long getContentLength()
    {
        return contentLength;
    }
    
    /**
     * Get the content-encoding returned by the server.
     * @return
     */
    public String getContentEncoding()
    {
        return contentEncoding;
    }

    protected HttpDownload doDownload(URL url, String op, boolean headOnly)
    {
         // Start timer.
        long start = System.currentTimeMillis();

        // Download the file from data.
        HttpDownload download = new HttpDownload(url, new File(tmpDirPath));
        download.setOverwrite(true);
        download.setHeadOnly(headOnly);
        download.run();

        // Log time.
        long duration = System.currentTimeMillis() - start;
        log.debug(op + " " + url.toString() + " (" + duration + "ms)");
        return download;
    }

    /**
     * Transfer the file from VOSpace to the local file system
     * and return a File object.
     * 
     * @return URL.
     * @throws URISyntaxException 
     * @throws IOException 
     * @throws RuntimeException 
     * @throws InterruptedException 
     */
    protected URL getFromVOSpace() 
    	throws URISyntaxException, IOException, RuntimeException, InterruptedException
    {
        List<Protocol> protocols = new ArrayList<Protocol>();
        URL baseURL = null;
        VOSURI src = new VOSURI(uri);
        URI serverUri = src.getServiceURI();
        RegistryClient reg = new RegistryClient();
        
        try
        {
	        if (this.sslEnabled)
	        {
	            protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
	            baseURL = reg.getServiceURL(serverUri, "https");
	        }
	        else
	        {
	            protocols.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
	            baseURL = reg.getServiceURL(serverUri, "http");
	        }
	
	        if (baseURL == null)
	        {
	            log.error("failed to find service URL for " + serverUri);
	            throw new RuntimeException("BUG: failed to find CADC VOSpace service URL");
	        }
        }
        catch (MalformedURLException e)
        {
            log.error("failed to find service URL for " + serverUri);
            log.error("reason: " + e.getMessage());
            throw new RuntimeException("BUG: failed to find CADC VOSpace service URL", e);
        }

        log.info("server uri: " + serverUri);
        log.info("base url: " + baseURL.toString());

        // schema validation is always enabled
        VOSpaceClient client = new VOSpaceClient(baseURL.toString(), true);
        View view = new View(new URI(VOS.VIEW_DEFAULT));        
        Transfer transfer = new Transfer(src, Direction.pullFromVoSpace, view, protocols);
        ClientTransfer clientTransfer = client.createTransfer(transfer);
        protocols = clientTransfer.getTransfer().getProtocols();
        URL url = null;
        for (Protocol protocol: protocols)
        {
            String uriString = protocol.getEndpoint();
            if (( uriString != null) && (uriString.length() > 0))
            {
                url = new URI(uriString).toURL();
                break;
            }
        }

        return url;
    }
}
