/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.sia;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.HttpConstants;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.tap.TapClient;

/**
 *
 * @author jburke
 */
public class SiaRunner implements JobRunner
{
    private static Logger log = Logger.getLogger(SiaRunner.class);

    private static final String INPUT_PARAMS_RESOURCE = "inputparams.xml";
    private static final double MAX_SEARCH_SIZE = 10.0;

    public static final String TAP_URI = "ivo://cadc.nrc.ca/argus";

    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private JobLogInfo logInfo;

    public void setJob(Job job)
    {
        this.job = job;
    }

    public void setJobUpdater(JobUpdater ju)
    {
        jobUpdater = ju;
    }

    public void setSyncOutput(SyncOutput so)
    {
        syncOutput = so;
    }

    public void run()
    {
        log.debug("RUN SiaRunner: " + job.owner);
        
        logInfo = new JobLogInfo(job);

        String startMessage = logInfo.start();
        log.info(startMessage);

        long t1 = System.currentTimeMillis();
        doit();
        long t2 = System.currentTimeMillis();

        logInfo.setElapsedTime(t2 - t1);

        String endMessage = logInfo.end();
        log.info(endMessage);
    }

    private void doit()
    {
        RegistryClient regClient = new RegistryClient();
        URL url = null;
        try
        {
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if ( !ExecutionPhase.EXECUTING.equals(ep) )
            {
                String message = job.getID() + ": QUEUED -> EXECUTING [FAILED] -- DONE";
                logInfo.setSuccess(false);
                logInfo.setMessage(message);
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");

            // Get the SIA request parameters.
            SiaRequest siaRequest = new SiaRequest(job, MAX_SEARCH_SIZE);
            log.debug(siaRequest);

            // URL to the TAP sync service.
            AuthMethod authMethod = AuthMethod.ANON;
            if (CredUtil.checkCredentials())
            {
                Subject currentSubject = AuthenticationUtil.getCurrentSubject();
                authMethod = AuthenticationUtil.getAuthMethodFromCredentials(currentSubject);
            }
            TapClient tc = new TapClient(URI.create(TAP_URI));
            URL tapSyncURL = tc.getSyncURL(Standards.getSecurityMethod(authMethod));
            
            // Get the ADQL request parameters.
            AdqlQueryGenerator queryGenerator = new AdqlQueryGenerator(siaRequest);
            Map<String, Object> parameters = queryGenerator.getParameterMap();
            for (Map.Entry<String,Object> me : parameters.entrySet())
            {
                log.debug("TAP request: " + me.getKey() + "=" + me.getValue());
            }
            // We don't want to follow the redirect following the POST.
            boolean followRedirects = false;

            // POST the query to the sync TAP service.
            HttpPost post = new HttpPost(tapSyncURL, parameters, followRedirects);
            post.run();

            // Create an ErrorSummary and throw RuntimeException if the POST failed.
            if (post.getThrowable() != null)
            {
                throw new RuntimeException("sync TAP query (" + tapSyncURL.toExternalForm() +
                                           ") failed because " +
                                           post.getThrowable().getMessage());
            }

            // Redirect URL: this is technically using the AuthMethodFromCredentials (cookie or cert)
            // which works but not for the right reason - correct impl would be to extract the jobID
            // and generate a new URL using the AuthMethod of the caller
            url = post.getRedirectURL();
            log.debug("redirectURL " + url);

            // For a Metadata query.
            if (siaRequest.isMetadataFormat)
            {
                log.debug("metadata query: proxying/modifying query response from " + tapSyncURL);
                // Get the query results VOTable.
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpDownload get = new HttpDownload(url, out);
                get.run();

                // Create an ErrorSummary and throw RuntimeException if the GET failed.
                if (get.getThrowable() != null)
                {
                    throw new RuntimeException("sync TAP query (" + tapSyncURL.toExternalForm() +
                                               ") failed, unable to process the query results because " +
                                               post.getThrowable().getMessage());
                }
                
                final String contentType = get.getContentType();
                
                VOTableReader reader = new VOTableReader();
                VOTableDocument doc = reader.read(new String(out.toByteArray(), "UTF-8"));
                VOTableResource res = doc.getResourceByType("results");
                VOTableTable vtab = res.getTable();
                
                // Get the extra input PARAMs VOTable.
                InputStream is = getResourceAsStream(INPUT_PARAMS_RESOURCE, SiaRunner.class);
                VOTableDocument idoc = reader.read(is);
                VOTableResource ires = idoc.getResourceByType("meta");
                VOTableTable itab = ires.getTable();

                // Add input PARAMs to the results VOTable.
                vtab.getParams().addAll(itab.getParams());

                // Write out the VOTable.
                VOTableWriter writer = new VOTableWriter();
                syncOutput.setHeader(HttpConstants.HDR_CONTENT_TYPE, contentType);
                syncOutput.setCode(200);
                writer.write(doc, syncOutput.getOutputStream());
            }
            else if ( "https".equals(tapSyncURL.getProtocol()))
            {
                log.debug("authenticated: proxying query response from " + tapSyncURL);
                // have to proxy the response stream
                syncOutput.setHeader("Content-Type", AdqlQueryGenerator.SIA_CONTENT_TYPE);
                HttpDownload get = new HttpDownload(url, syncOutput.getOutputStream());
                get.run();
                
                // Create an ErrorSummary and throw RuntimeException if the GET failed.
                if (get.getThrowable() != null)
                {
                    throw new RuntimeException("sync TAP query (" + tapSyncURL.toExternalForm() +
                                               ") failed, unable to process the query results because " +
                                               post.getThrowable().getMessage());
                }
            }
            else // anon http so we just redirect
            {
                // URL to the query results.
                log.debug("redirect: " + url);
                syncOutput.setResponseCode(303);
                syncOutput.setHeader("Location", url.toExternalForm());
            }
            
            // Mark the Job as completed adding the URL to the query results.
            List<Result> results = new ArrayList<Result>();
            results.add(new Result("result", new URI(url.toExternalForm())));
            jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.COMPLETED, results, new Date());
        }
        catch (Throwable t)
        {
            logInfo.setSuccess(false);
            logInfo.setMessage(t.getMessage());
            log.debug(t);
            try
            {
                VOTableWriter writer = new VOTableWriter();
                syncOutput.setHeader("Content-Type", AdqlQueryGenerator.SIA_CONTENT_TYPE);
                syncOutput.setResponseCode(200);
                writer.write(t, syncOutput.getOutputStream());
            }
            catch (IOException ioe)
            {
                log.debug("Error writing error document " + ioe.getMessage());
            }
            ErrorSummary errorSummary = new ErrorSummary(t.getMessage(), ErrorType.FATAL, url);
            try
            {
                jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, ExecutionPhase.ERROR,
                    errorSummary, new Date());
            }
            catch(Throwable oops)
            {
                log.debug("failed to set final error status after " + t, oops);
            }
        }
    }

    public static InputStream getResourceAsStream(String resourceFileName, Class runningClass)
    {
        InputStream is = runningClass.getClassLoader().getResourceAsStream(resourceFileName);
        if (is == null)
        {
            throw new MissingResourceException("Resource not found: " + resourceFileName, runningClass.getName(), resourceFileName);
        }
        return is;
    }
    
}
