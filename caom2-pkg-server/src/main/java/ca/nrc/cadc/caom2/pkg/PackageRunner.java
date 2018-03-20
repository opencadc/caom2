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

package ca.nrc.cadc.caom2.pkg;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CaomArtifactResolver;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.SchemeHandler;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.caom2ops.TransientFault;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.server.SyncOutput;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class PackageRunner implements JobRunner
{
    private static final Logger log = Logger.getLogger(PackageRunner.class);

    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private ByteCountOutputStream counter;
    private WebServiceLogInfo logInfo;
    
    private final URI tapID;

    public PackageRunner() 
    { 
        ServiceConfig sc = new ServiceConfig();
        this.tapID = sc.getTapServiceID();
    }

    @Override
    public void setJob(Job job)
    {
        this.job = job;
    }

    @Override
    public void setJobUpdater(JobUpdater ju)
    {
        this.jobUpdater = ju;
    }

    @Override
    public void setSyncOutput(SyncOutput so)
    {
        this.syncOutput = so;
    }

    @Override
    public void run()
    {
        logInfo = new JobLogInfo(job);
        log.info(logInfo.start());
        long start = System.currentTimeMillis();

        doIt();

        if (counter != null)
        {
            logInfo.setBytes(counter.getByteCount());
        }
        logInfo.setElapsedTime(System.currentTimeMillis() - start);
        log.info(logInfo.end());
    }

    private void doIt()
    {

        ExecutionPhase ep;
        TarWriter w = null;
        try
        {
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if ( !ExecutionPhase.EXECUTING.equals(ep) )
            {
                ep = jobUpdater.getPhase(job.getID());
                log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- DONE");
                logInfo.setSuccess(false);
                logInfo.setMessage("Could not set job phase to executing, was: " + ep);
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");
            
            // obtain credentials from CDP if the user is authorized
            AccessControlContext accessControlContext = AccessController.getContext();
            Subject subject = Subject.getSubject(accessControlContext);
            AuthMethod authMethod = AuthenticationUtil.getAuthMethod(subject);
            AuthMethod proxyAuthMethod = authMethod;
            if ( CredUtil.checkCredentials() )
            {
                proxyAuthMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            }
            
            String runID = job.getID();
            if (job.getRunID() != null)
                runID = job.getRunID();
            
            List<String> idList = ParameterUtil.findParameterValues("ID", job.getParameterList());

            CaomTapQuery query = new CaomTapQuery(tapID, runID);
            
            CaomArtifactResolver artifactResolver = new CaomArtifactResolver();
            artifactResolver.setAuthMethod(proxyAuthMethod); // override auth method for proxied calls
            
            for (String suri : idList)
            {
                URI uri = new URI(suri);
                PlaneURI puri;
                ArtifactQueryResult result;
                if ( PublisherID.SCHEME.equals(uri.getScheme()))
                {
                    PublisherID p = new PublisherID(uri);
                    result = query.performQuery(p, true);
                    puri = toPlaneURI(p);
                }
                else
                {
                    puri = new PlaneURI(uri);
                    result = query.performQuery(puri, true);
                }
                List<Artifact> artifacts = result.getArtifacts();
                
                stripPreviews(artifacts);
                if (idList.size() == 1 && artifacts.size() == 1)
                {
                    // single file result: redirect
                    artifactResolver.setAuthMethod(authMethod); // original auth method for redirect
                    Artifact a = artifacts.get(0);
                    URL url = artifactResolver.getURL(a.getURI());
                    log.debug("redirect: " + a.getURI() + " from " + url);
                    syncOutput.setResponseCode(303);
                    syncOutput.setHeader("Location", url.toExternalForm());
                    break; // not really needed since nothing below the else
                }
                else
                {
                    if (w == null)
                    {
                        // generate package name
                        String tname = generatePackageName();
                        if (idList.size() == 1)
                            tname = generatePackageName(puri);
                        w = initTarResponse(tname);
                    }
                    // always generate subdir name for tar file from the current plane
                    String pname = generatePackageName(puri);
                    if ( artifacts.isEmpty())
                    {
                        // either the input ID was: not found, access-controlled, or has no artifacts
                        w.addMessage(pname, "no files available for ID=" + suri);
                    }
                    else
                    {
                        for (Artifact a : artifacts)
                        {
                            URL url = artifactResolver.getURL(a.getURI());
                            log.debug("write: " + a.getURI() + " from " + url);
                            try
                            {
                                w.write(pname, url);
                            }
                            catch(TarProxyException ex)
                            {
                                // TarWriter tracks these and appends a README, so continue
                                log.debug("proxy failure", ex);
                            }
                            finally { }
                        }
                    }
                }
            }
            
            // set final phase, only sync so no results
            log.debug(job.getID() + ": EXECUTING -> COMPLETED...");
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, new Date());
            if ( !ExecutionPhase.COMPLETED.equals(ep) )
            {
                ep = jobUpdater.getPhase(job.getID());
                log.debug(job.getID() + ": EXECUTING -> COMPLETED [FAILED], phase was " + ep);
                logInfo.setSuccess(false);
                logInfo.setMessage("Could not set job phase to completed.");
                return;
            }
            log.debug(job.getID() + ": EXECUTING -> COMPLETED [OK]");

        }
        catch(IllegalArgumentException ex)
        {
            sendError(ex, ex.getMessage(), 400);
        }
        catch(UnsupportedOperationException ex)
        {
            sendError(ex, "unsupported operation: " + ex.getMessage(), 400);
        }
        catch(TransientFault ex)
        {
            sendError(ex, ex.getMessage(), ex.getResponseCode());
        }
        catch(Throwable t)
        {
            if ( ThrowableUtil.isACause(t, InterruptedException.class) )
            {
                try
                {
                    ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
                    if ( !ExecutionPhase.ABORTED.equals(ep) )
                        return; // clean exit of aborted job
                }
                catch(Exception ex2)
                {
                    log.error("failed to check job phase after InterruptedException", ex2);
                }
            }
            sendError(t, 500);
        }
        finally
        {
            if (w != null)
                try { w.close(); }
                catch(Exception ignore) { }
        }
    }

    // temporary hack to support both caom and ivo uris in generatePackageName
    private PlaneURI toPlaneURI(PublisherID pid)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("caom:");
        String collection = pid.getResourceID().getPath();
        while (collection.charAt(0) == '/')
            collection = collection.substring(1);
        sb.append(collection).append("/");
        sb.append(pid.getURI().getQuery());
        return new PlaneURI(URI.create(sb.toString()));
    }
    
    // used in an int-test
    public static String generatePackageName(PlaneURI uri)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(uri.getParent().getCollection()).append("-");
        sb.append(uri.getParent().getObservationID()).append("-");
        sb.append(uri.getProductID());
        return sb.toString();
    }
    
    public void stripPreviews(List<Artifact> artifacts)
    {
        ListIterator<Artifact> iter = artifacts.listIterator();
        while (iter.hasNext())
        {
            Artifact a = iter.next();
            if ( ProductType.PREVIEW.equals(a.getProductType())
                || ProductType.THUMBNAIL.equals(a.getProductType()) )
            {
                iter.remove();
                log.debug("stripPreviews: removed " + a.getProductType() + " " + a.getURI());
            }
        }
    }
    
    String generatePackageName()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("cadc-download-").append(job.getID());
        return sb.toString();
    }
    
    private TarWriter initTarResponse(String name)
        throws IOException
    {
        StringBuilder cdisp = new StringBuilder();
        cdisp.append("inline;filename=");
        cdisp.append(name);
        cdisp.append(".tar");

        syncOutput.setResponseCode(200);
        syncOutput.setHeader("Content-Type", TarWriter.CONTENT_TYPE);
        syncOutput.setHeader("Content-Disposition", cdisp.toString());
        this.counter = new ByteCountOutputStream(syncOutput.getOutputStream());
        return new TarWriter(counter);
    }

    private class ObservationNotFoundException extends Exception
    {

        public ObservationNotFoundException(ObservationURI uri)
        {
            super("not found: " + uri.getURI().toASCIIString());
        }
        
    }
    
    private void sendError(Throwable t, int code)
    {
        if (code >= 500)
            log.error("EPIC FAIL", t);
        sendError(t, "unexpected failure: " + t.toString(), code);
    }

    private void sendError(Throwable t, String s, int code)
    {
    	logInfo.setSuccess(false);
        logInfo.setMessage(s);
        log.debug("sendError", t);
        try
        {
            ErrorSummary err = new ErrorSummary(s, ErrorType.FATAL);
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR, err, new Date());
            if ( !ExecutionPhase.ERROR.equals(ep) )
            {
                ep = jobUpdater.getPhase(job.getID());
                log.debug(job.getID() + ": EXECUTING -> ERROR [FAILED] -- DONE");
            }
            else
                log.debug(job.getID() + ": EXECUTING -> ERROR [OK]");
        }
        catch(Throwable t2)
        {
            log.error("failed to persist Job ERROR for " + job.getID(), t2);
        }

        // attempt to write VOTable eror output
        try
        {
            syncOutput.setHeader("Content-Type", VOTableWriter.CONTENT_TYPE);
            syncOutput.setResponseCode(code);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(syncOutput.getOutputStream()));
            writer.println(s);
            writer.flush();
            writer.close();
        }
        catch(IOException ex)
        {
            log.debug("write error failed", ex);
        }
    }
}
