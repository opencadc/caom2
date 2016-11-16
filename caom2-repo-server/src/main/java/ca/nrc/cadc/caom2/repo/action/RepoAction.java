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

package ca.nrc.cadc.caom2.repo.action;

import ca.nrc.cadc.ac.UserNotFoundException;
import ca.nrc.cadc.ac.client.GMSClient;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.cert.CertificateException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.dao.TransientDataAccessResourceException;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.caom2.repo.CaomRepoConfig;
import ca.nrc.cadc.caom2.repo.SyncInput;
import ca.nrc.cadc.caom2.repo.SyncOutput;
import ca.nrc.cadc.caom2.xml.ObservationParsingException;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.io.ByteCountReader;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import java.io.File;

/**
 *
 * @author pdowler
 */
public abstract class RepoAction implements PrivilegedExceptionAction<Object>
{
    private static final Logger log = Logger.getLogger(RepoAction.class);

    public static final String MODE_KEY = RepoAction.class.getName() + ".state";
    public static final String OFFLINE = "Offline";
    public static final String OFFLINE_MSG = "System is offline for maintainence";
    public static final String READ_ONLY = "ReadOnly";
    public static final String READ_ONLY_MSG = "System is in read-only mode for maintainence";
    public static final String READ_WRITE = "ReadWrite";
    private boolean readable = true;
    private boolean writable  = true;
    
    public static final String CAOM_MIMETYPE = "text/x-caom+xml";
    public static final String ERROR_MIMETYPE = "text/plain";
    
    // 20MB XML Doc size limit
    private static final long DOCUMENT_SIZE_MAX = 20971520L;

    private final URI CADC_GROUP_URI  = URI.create("ivo://cadc.nrc.ca/gms#CADC");

    protected SyncInput syncInput;
    protected SyncOutput syncOutput;
    protected WebServiceLogInfo logInfo;
    
    private final String serviceName = "sc2repo"; // HACK until merge and get cadc-rest to provide this
    
    private String path;
    private ObservationURI uri;
        
    private transient CaomRepoConfig.Item repoConfig;
    private transient ObservationDAO dao;

    protected RepoAction() { }

    // this method will only downgrade the state to !readable and !writable
    // and will never restore them to true - that is intentional
    private void initState()
    {
        String key = RepoAction.MODE_KEY;
        String val = System.getProperty(key);
        if (OFFLINE.equals(val))
        {
            readable = false;
            writable = false;
        }
        else if (READ_ONLY.equals(val))
        {
            writable = false;
        }
    }
    
    public void setLogInfo(WebServiceLogInfo logInfo)
    {
        this.logInfo = logInfo;
    }

    public void setSyncInput(SyncInput syncInput)
    {
        this.syncInput = syncInput;
    }

    public void setSyncOutput(SyncOutput syncOutput)
    {
        this.syncOutput = syncOutput;
    }
    
    public void setPath(String path)
    {
        if (path.charAt(0) == '/')
            path = path.substring(1);
        this.path = path;
    }

    public Object run()
        throws Exception
    {
        try
        {
            this.uri = getURI(path);
            log.debug("URI: " + uri);
            logInfo.setSuccess(false);
            doAction();
            logInfo.setSuccess(true);
        }
        catch(AccessControlException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 403, "permission denied: " + uri.getURI().toASCIIString(), false);
        }
        catch(CertificateException ex)
        {
            handleException(ex, 403, "permission denied -- reason: invalid proxy certficate", true);
        }
        catch(IllegalArgumentException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 400, "invalid input: caom:" + path, true);
        }
        catch(CollectionNotFoundException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 404, "collection not found: " + uri.getCollection(), false);
        }
        catch(ObservationNotFoundException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 404, "not found: " + uri.getURI().toASCIIString(), false);
        }
        catch(ObservationAlreadyExistsException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 409, "already exists: " + uri.getURI().toASCIIString(), false);
        }
        catch(ByteLimitExceededException ex)
        {
            logInfo.setSuccess(true);
            handleException(ex, 413, "too large: " + uri.getURI().toASCIIString(), false);
        }
        catch(TransientDataAccessResourceException ex)
        {
            String err = ex.toString();
            String lowerr = err.toLowerCase();
            if (lowerr.contains("attempt to insert duplicate key"))
            {
                logInfo.setSuccess(true);
                handleException(ex, 400, "duplicate entity: " + uri.getURI().toASCIIString(), true);
            }
            else
                handleException(ex, 500, "unexpected failure: " + path, true);
        }
        catch(RuntimeException unexpected)
        {
            handleException(unexpected, 500, "unexpected failure: " + path + " " + uri.getURI().toASCIIString(), true);
        }
        catch(Error unexpected)
        {
            handleException(unexpected, 500, "unexpected error: " + path + " " + uri.getURI().toASCIIString(), true);
        }
        
        return null;
    }
    
    private void handleException(Throwable ex, int code, String message, boolean showExceptions)
            throws IOException
    {
        logInfo.setMessage(message);
        log.debug(message, ex);
        if (!syncOutput.isOpen())
        {
            syncOutput.setCode(code); // too large
            syncOutput.setHeader("Content-Type", ERROR_MIMETYPE);
            PrintWriter w = syncOutput.getWriter();
            w.println(message);

            if (showExceptions)
            {
                w.println(ex.toString());
                Throwable cause = ex.getCause();
                while (cause != null)
                {
                    w.print("cause: ");
                    w.println(cause.toString());
                    cause = cause.getCause();
                }
            }

            w.flush();
        }
        else
            log.error("unexpected situation: SyncOutput is open", ex);
    }
    
    public abstract void doAction()
        throws Exception;

    protected ObservationURI getURI()
    {
        return uri;
    }

    protected ObservationDAO getDAO()
        throws IOException
    {
        if (dao == null)
            dao = getDAO(uri.getCollection());
        return dao;
    }

    // read the input stream (POST and PUT) and extract the observation from the XML document
    protected Observation getInputObservation()
        throws IOException
    {
        /*
        // check content-type of input once we have a client that can set it
        List<String> types = syncInput.getHeaders("Content-Type");
        if (types.isEmpty())
            throw new IllegalArgumentException("no Content-Type found");
        String contentType = types.get(0);
        if (!CAOM_MIMETYPE.equalsIgnoreCase(contentType))
            throw new IllegalArgumentException("unexpected Content-Type found: " + contentType);
        */

        ObservationReader r = new ObservationReader();
        try
        {
            Reader reader = syncInput.getReader();
            ByteCountReader byteCountReader = new ByteCountReader(reader, DOCUMENT_SIZE_MAX);
            Observation ret = r.read(byteCountReader);
            logInfo.setBytes(byteCountReader.getByteCount());
            return ret;
        }
        catch(ObservationParsingException ex)
        {
            throw new IllegalArgumentException("invalid observation content", ex);
        }
    }

    /**
     * Check if the caller can read the specified resource.
     * 
     * @param uri
     * @throws AccessControlException
     * @throws java.security.cert.CertificateException
     * @throws ca.nrc.cadc.caom2.repo.action.CollectionNotFoundException
     * @throws java.io.IOException
     */
    protected void checkReadPermission(ObservationURI uri)
        throws AccessControlException, CertificateException, 
               CollectionNotFoundException, IOException
    {
        initState();
        if (!readable)
        {
            if (!writable)
                throw new IllegalStateException(OFFLINE_MSG);
            throw new IllegalStateException(READ_ONLY_MSG);
        }

        CaomRepoConfig.Item i = getConfig(uri.getCollection());
        if (i == null)
            throw new CollectionNotFoundException(uri.getCollection());
        
        try
        {
            if ( CredUtil.checkCredentials() )
            {
                LocalAuthority loc = new LocalAuthority();
                URI gmsURI = loc.getServiceURI(Standards.GMS_SEARCH_01.toASCIIString());
                GMSClient gms = new GMSClient(gmsURI);
                URI guri;

                guri = i.getReadWriteGroup();
                if (gms.isMember(guri.getFragment()))
                    return;

                guri = i.getReadOnlyGroup();
                if (gms.isMember(guri.getFragment()))
                    return;

                if (gms.isMember(CADC_GROUP_URI.getFragment()))
                    return;
            }
        }
        catch(AccessControlException ex)
        {
            throw new AccessControlException("read permission denied (credentials not found): " + getURI());
        }
        catch(UserNotFoundException ex)
        {
            throw new AccessControlException("read permission denied (user not found): " + getURI());
        }
        throw new AccessControlException("read permission denied: " + getURI());
    }

    /**
     * Check if the caller can create or modify the specified resource.
     *
     * @param uri
     * @throws AccessControlException
     * @throws java.security.cert.CertificateException
     * @throws ca.nrc.cadc.caom2.repo.action.CollectionNotFoundException
     * @throws java.io.IOException
     */
    protected void checkWritePermission(ObservationURI uri)
        throws AccessControlException, CertificateException, 
               CollectionNotFoundException, IOException
    {
        initState();
        if (!writable)
        {
            if (readable)
                throw new IllegalStateException(READ_ONLY_MSG);
            throw new IllegalStateException(OFFLINE_MSG);
        }
        
        CaomRepoConfig.Item i = getConfig(uri.getCollection());
        if (i == null)
            throw new CollectionNotFoundException(uri.getCollection());

        try
        {
            if ( CredUtil.checkCredentials() )
            {
                LocalAuthority loc = new LocalAuthority();
                URI gmsURI = loc.getServiceURI(Standards.GMS_SEARCH_01.toASCIIString());
                GMSClient gms = new GMSClient(gmsURI);
                URI guri = i.getReadWriteGroup();
                if (gms.isMember(guri.getFragment()))
                    return;
            }
        }
        catch(AccessControlException ex)
        {
            throw new AccessControlException("read permission denied (credentials not found): " + getURI());
        }
        catch(UserNotFoundException ex)
        {
            throw new AccessControlException("read permission denied (user not found): " + getURI());
        }

        throw new AccessControlException("write permission denied: " + getURI());
    }


    // extract the URI from the path
    private ObservationURI getURI(String path)
    {
        try
        {
            URI u = new URI("caom", path, null);
            ObservationURI ret = new ObservationURI(u);
            return ret;
        }
        catch(URISyntaxException ex)
        {
            throw new IllegalArgumentException("invalid path for URI: " + path);
        }
    }

    // read configuration
    private CaomRepoConfig.Item getConfig(String collection)
        throws IOException
    {
        // TODO: if this fails, fall back to last known good config (static)

        if (repoConfig != null)
            return repoConfig;

        File config = new File(System.getProperty("user.home") + "/config", 
                this.serviceName + ".properties");
        
        //File config = new File(System.getProperty("user.home") + "/config", 
        //        CaomRepoConfig.class.getSimpleName() + ".properties");
        
        CaomRepoConfig rc = new CaomRepoConfig(config);

        if (rc.isEmpty())
            throw new IllegalStateException("no RepoConfig.Item(s)found");

        this.repoConfig = rc.getConfig(collection);
        return repoConfig;
    }

    // create DAO
    private ObservationDAO getDAO(String collection)
        throws IOException
    {
        CaomRepoConfig.Item i = getConfig(collection);
        if (i != null)
        {
            ObservationDAO ret = new DatabaseObservationDAO();
            Map<String,Object> props = new HashMap<String,Object>();
            props.put("jndiDataSourceName", i.getDataSourceName());
            props.put("database", i.getDatabase());
            props.put("schema", i.getSchema());
            props.put(SQLGenerator.class.getName(), i.getSqlGenerator());
            ret.setConfig(props);
            return ret;
        }
        throw new IllegalArgumentException("unknown collection: " + collection);
    }
}
