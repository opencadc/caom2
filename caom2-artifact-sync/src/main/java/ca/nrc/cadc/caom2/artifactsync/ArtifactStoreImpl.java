package ca.nrc.cadc.caom2.artifactsync;

import java.io.InputStream;
import java.net.URI;

public class ArtifactStoreImpl implements ArtifactStore
{
	String dataURL;
	
	public ArtifactStoreImpl()
	{
		RegistryClient rc = new RegistryClient();
		AuthMethod authMethod = AuthenticationUtil.getAuthMethodForSubject();
		URL svcURL = rc.getServiceURL(Standards.DATA_WS, "ivo://cadc.nrc.ca/data", authMethod);
		dataURL = svcURL.toString();
	}
	

	public boolean contains(URI artifactURI, URI checksum)
			throws TransientException
	{
		ADURI adURI = getAdURI(artifactURI);
		String expectedMD5 = getMD5Sum(checksum);
		
		URL url = dataURL + adURI.getArchive() + "/" + adURI.getFileID();
		HttpDownload httpHead = new HttpDownload(dataURL);
		httpHead.setHeadRequest(true);
		httpHead.setFollowRedirects(false);
		httpHead.run();
		if (httpHead.getThrowable() == null)
		{
			String contentMD5 = httpHead.getHeader("Content-MD5");
			return expectedMD5.equalsIgnoreCase(contentMD5);
		}
		
		if (httpHead.getResponseCode() == 404)
		{
			// not found
			return false;
		}
		
		if (httpHead.getThrowable() instanceof TransientException)
		{
			throw (TransientException) httpHead.getThrowable();
		}
			
		throw IllegalStateException("Unexpected", httpHead.getThrowable());
		
	}

	public void store(URI artifactURI, URI checksum, InputStream data)
			throws TransientException
	{
		ADURI adURI = getAdURI(artifactURI);
		String contentMD5 = getMD5Sum(checksum);
		
		URL url = dataURL + adURI.getArchive() + "/" + adURI.getFileID();
		HttpUpload httpPut = new HttpUpload(dataURL, data);
		httpPut.setHeader("Content-MD5", contentMD5);
		httpPut.run();
		if (httpPut.getThrowable() == null)
		{
			return;
		}
		
		if (httpPut.getThrowable() instanceof TransientException)
		{
			throw (TransientException) httpHead.getThrowable();
		}
			
		throw IllegalStateException("Unexpected", httpPut.getThrowable());
	}
	
	private ADURI getAdURI(URI uri) throws UnsupportedOperationException
	{
		ADURI adURI = null;
		String expectedMD5 = null;
		try
		{
		    return new ADURI(uri);
		}
		catch (IllegalArgumentException e)
		{
			throw new UnsupportedOperationException(
				"URI " + uri + " not supported.", e);
		}
	}
	
	private String getMD5Sum(URI checksum) throws UnsupportedOperationException
	{
		
		if (checksum.getScheme().equalsIgnoreCase("MD5"))
		{
			return checksum.getPath();
		}
		else
		{
			throw new UnsupportedOperationException(
				"Checksum algorithm " + checksum.getScheme() + " not suported.");
		}
	}

}
