/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.artifactsync;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;

/**
 * CADC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the CADC archive data web service
 * to perform the artifact operations defined in ArtifactStore.
 *
 * @author majorb
 */
public class CADCArtifactStore implements ArtifactStore
{
    private static final Logger log = Logger.getLogger(CADCArtifactStore.class);

    private static final URI DATA_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/data");

	String dataURL;

	public CADCArtifactStore()
	{
	    try
	    {
    		RegistryClient rc = new RegistryClient();
    		Subject subject = AuthenticationUtil.getCurrentSubject();
    		AuthMethod authMethod = AuthenticationUtil.getAuthMethod(subject);
    		if (authMethod == null)
    		{
    		    authMethod = AuthMethod.ANON;
    		}
    		Capabilities caps = rc.getCapabilities(DATA_RESOURCE_ID);
    		Capability dataCap = caps.findCapability(Standards.DATA_10);
    		URI securityMethod = Standards.getSecurityMethod(authMethod);
    		Interface ifc = dataCap.findInterface(securityMethod);
    		if (ifc == null)
    		{
    		    throw new IllegalArgumentException("No interface for security method " + securityMethod);
    		}
    		dataURL = ifc.getAccessURL().getURL().toString();
	    }
	    catch (Throwable t)
	    {
	        String message = "Failed to initialize data URL";
	        throw new RuntimeException(message, t);
	    }

	}

	public boolean contains(URI artifactURI, URI checksum)
			throws TransientException
	{
	    CADCDataURI dataURI = new CADCDataURI(artifactURI);
		String expectedMD5 = getMD5Sum(checksum);
		URL url = createDataURL(dataURI);
		log.debug("Making HTTP HEAD request to: " + url);

		HttpDownload httpHead = new HttpDownload(url, new ByteArrayOutputStream());
		httpHead.setHeadOnly(true);
		httpHead.setFollowRedirects(false);
		httpHead.run();
		int respCode = httpHead.getResponseCode();
		log.debug("Response code: " + respCode);
		if (httpHead.getThrowable() == null && respCode == 200)
		{
			String contentMD5 = httpHead.getContentMD5();
			log.debug("Found matching artifact with md5 " + contentMD5);
			return expectedMD5.equalsIgnoreCase(contentMD5);
		}

		if (httpHead.getResponseCode() == 404)
		{
			log.debug("Artifact not found");
			return false;
		}

		if (httpHead.getThrowable() != null)
		{
    		if (httpHead.getThrowable() instanceof TransientException)
    		{
    		    log.debug("Transient Exception");
    			throw (TransientException) httpHead.getThrowable();
    		}

    		throw new RuntimeException("Unexpected", httpHead.getThrowable());
		}

		throw new RuntimeException("unexpected response code " + respCode);

	}

	public void store(URI artifactURI, URI checksum, InputStream data)
			throws TransientException
	{
	    CADCDataURI dataURI = new CADCDataURI(artifactURI);
	    String contentMD5 = getMD5Sum(checksum);
	    URL url = createDataURL(dataURI);

		HttpUpload httpPut = new HttpUpload(data, url);
		if (contentMD5 != null)
		{
		    httpPut.setContentMD5(contentMD5);
		}
		httpPut.run();
		int respCode = httpPut.getResponseCode();

        if (httpPut.getThrowable() == null && (respCode == 200 || respCode == 201))
        {
            log.debug("Succussfully uploaded artifact");
            return;
        }

        if (respCode == 409)
        {
            // conflict
            throw new IllegalStateException(artifactURI + " already exists");
        }

        if (httpPut.getThrowable() != null)
        {

            if (httpPut.getThrowable() instanceof TransientException)
            {
                log.debug("Transient Exception");
                throw (TransientException) httpPut.getThrowable();
            }

    		throw new RuntimeException("Unexpected", httpPut.getThrowable());
        }

		throw new RuntimeException("unexpected response code " + respCode);
	}

	private String getMD5Sum(URI checksum) throws UnsupportedOperationException
	{
	    if (checksum == null)
	    {
	        return null;
	    }

		if (checksum.getScheme().equalsIgnoreCase("MD5"))
		{
			return checksum.getSchemeSpecificPart();
		}
		else
		{
			throw new UnsupportedOperationException(
				"Checksum algorithm " + checksum.getScheme() + " not suported.");
		}
	}

	private URL createDataURL(CADCDataURI dataURI)
	{
       try
        {
            return new URL(dataURL + "/" + dataURI.archive + "/" + dataURI.fileID);
        }
        catch (MalformedURLException e)
        {
            throw new IllegalStateException("BUG in forming data URL", e);
        }
	}

	/**
	 * Class to help with the parsing and validation of CADC-specific
	 * data URIs.
	 */
	class CADCDataURI
	{
	    String archive;
	    String fileID;

	    CADCDataURI(URI uri) throws UnsupportedOperationException
	    {
	        validate(uri);
	    }

        private void validate(URI uri)
        {

            if (!uri.getScheme().equalsIgnoreCase("ad"))
            {
                throw new UnsupportedOperationException("Scheme in data " + uri + " not supported.");
            }

            if (uri.getAuthority() != null)
                throw new IllegalArgumentException("Authority not allowed in artifact ID");
            if (uri.getQuery() != null)
                throw new IllegalArgumentException("Authority not allowed in artifact ID");
            if (uri.getFragment() != null)
                throw new IllegalArgumentException("Authority not allowed in artifact ID");

            String parts = uri.getRawSchemeSpecificPart();
            int i = parts.indexOf('/');
            if (i > 0)
            {
                this.archive = parts.substring(0,i);
                parts = parts.substring(i+1); // namespace+fileID
            }

            i = parts.lastIndexOf('/');
            if (i > 0)
            {
                this.fileID = parts.substring(i+1);
                parts = parts.substring(0, i); // namespace
            }
            else
            {
                this.fileID = parts;
                parts = null;
            }
            if ( !StringUtil.hasText(this.archive) )
                throw new IllegalArgumentException("Cannot extract archive from " + uri);
            if ( !StringUtil.hasText(this.fileID) )
                throw new IllegalArgumentException("Cannot extract fileID from " + uri);

            sanitize(this.archive);
            sanitize(this.fileID);

            if (parts != null)
                throw new UnsupportedOperationException("Artifact namespace not supported");

            // TODO: Remove this trim when AD supports longer archive names
            if (archive.length() > 6)
                archive = archive.substring(0, 6);
        }

        private void sanitize(String s)
        {
            Pattern regex = Pattern.compile("^[a-zA-Z 0-9\\_\\.\\-\\+\\@]*$");
            Matcher matcher = regex.matcher(s);
            if (!matcher.find())
                throw new IllegalArgumentException("Invalid dataset characters.");
        }
	}

}
