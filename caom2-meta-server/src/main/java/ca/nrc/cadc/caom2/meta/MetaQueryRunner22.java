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

package ca.nrc.cadc.caom2.meta;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.xml.JsonWriter;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.caom2.xml.XmlConstants;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.caom2ops.TransientFault;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 * MetaQueryRunner hard coded to output CAOM-2.2 documents.
 * 
 * @author pdowler
 */
public class MetaQueryRunner22 implements JobRunner
{
    private static final Logger log = Logger.getLogger(MetaQueryRunner22.class);

    private static final String DEFAULT_FORMAT = "text/xml";
    private static final String JSON_FORMAT = "application/json";
    
    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private WebServiceLogInfo logInfo;
    
    private final URI metaID;
    private final URI tapID;

    public MetaQueryRunner22() 
    { 
        ServiceConfig sc = new ServiceConfig();
        this.metaID = sc.getMetaID();
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

        logInfo.setElapsedTime(System.currentTimeMillis() - start);
        log.info(logInfo.end());
    }

    private void doIt()
    {

        ExecutionPhase ep;
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
            
            // input parameter
            String suri = ParameterUtil.findParameterValue("ID", job.getParameterList());
            if (suri == null)
                throw new IllegalArgumentException("missing required parameter: ID");

            String format = ParameterUtil.findParameterValue("RESPONSEFORMAT", job.getParameterList());
            if (format == null)
                format = DEFAULT_FORMAT;
            
            
            ObservationURI uri = null;
            try 
            {
                uri = new ObservationURI(new URI(suri));
            }
            catch(URISyntaxException ex)
            {
                StringBuilder msg = new StringBuilder();
                msg.append("invalid URI: '").append(suri).append("'");
                if (suri.indexOf(' ') > 0)
                    msg.append(" contains space(s) -- client failed to URL-encode?");
                throw new IllegalArgumentException(msg.toString(), ex);
            }
            
            String runID = job.getID();
            if (job.getRunID() != null)
                runID = job.getRunID();
            CaomTapQuery query = new CaomTapQuery(tapID, runID);
            Observation obs = query.performQuery(uri);
            log.warn("found: " + obs);
            
            if (obs == null)
            {
                throw new ObservationNotFoundException(uri);
            }
                
            syncOutput.setResponseCode(HttpURLConnection.HTTP_OK);
            syncOutput.setHeader("Content-Type", format);
            
            if (JSON_FORMAT.equals(format))
            {
                JsonWriter writer = new JsonWriter(true, XmlConstants.CAOM2_2_NAMESPACE);
                writer.write(obs, syncOutput.getOutputStream());
            }
            else
            {
                //RegistryClient reg = new RegistryClient();
                //URL metaURL = reg.getServiceURL(metaID, Standards.CAOM2_OBS_20, AuthMethod.ANON);
                //String styleSheetURL = metaURL.toExternalForm().replace("/meta", "/caom2_summary.xslt");
                ObservationWriter writer = new ObservationWriter("caom2", XmlConstants.CAOM2_2_NAMESPACE, false);
                //writer.setStylesheetURL(styleSheetURL);
                writer.write(obs, syncOutput.getOutputStream());
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
        catch(CertificateException ex)
        {
            sendError(ex, "permission denied -- reason: invalid proxy certificate", 403);
        }
        catch(IllegalArgumentException ex)
        {
            sendError(ex, ex.getMessage(), 400);
        }
        catch(ObservationNotFoundException ex)
        {
            sendError(ex, ex.getMessage(), 404);
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
