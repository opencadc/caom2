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

package ca.nrc.cadc.caom2.datalink;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.MissingResourceException;


import ca.nrc.cadc.reg.Standards;
import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.TransientFault;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.dali.MaxRecValidator;
import ca.nrc.cadc.dali.tables.ListTableData;
import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableGroup;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.reg.client.RegistryClient;
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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author pdowler
 */
public class LinkQueryRunner implements JobRunner
{
    private static final Logger log = Logger.getLogger(LinkQueryRunner.class);

    private static final String SERVICES_RESOURCE = "cutoutMetaResource.xml";
    private static final String TAP_URI = "ivo://cadc.nrc.ca/tap";

    private static final int MAXREC = 100;
    private static final String GETDOWNLOAD = "getDownloadLinks";
    
    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private WebServiceLogInfo logInfo;

    public LinkQueryRunner() { }

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
            
            String request = ParameterUtil.findParameterValue("REQUEST", job.getParameterList());
            String fmt = ParameterUtil.findParameterValue("RESPONSEFORMAT", job.getParameterList());
            
            boolean downloadFilesOnly = false;
            if (ManifestWriter.CONTENT_TYPE.equals(fmt) || GETDOWNLOAD.equalsIgnoreCase(request))
                downloadFilesOnly = true;
            
            MaxRecValidator mrv = new MaxRecValidator();
            mrv.setJob(job);
            if (downloadFilesOnly)
            {
                // no limit
                mrv.setDefaultValue(null);
                mrv.setMaxValue(null);
            }
            else
            {
                mrv.setDefaultValue(MAXREC);
                mrv.setMaxValue(MAXREC);
            }
            Integer maxrec = mrv.validate();

            // obtain credentials fropm CDP if the user is authorized
            AuthMethod queryAuthMethod = AuthMethod.ANON;
            if ( CredUtil.checkCredentials() )
            {
                queryAuthMethod = AuthMethod.CERT;
            }

            RegistryClient reg = new RegistryClient();
            URL tapURL = reg.getServiceURL(URI.create(TAP_URI), Standards.TAP_SYNC_11, queryAuthMethod);

            VOTableDocument vot = new VOTableDocument();
            VOTableResource vr = new VOTableResource("results");
            vot.getResources().add(vr);
            VOTableTable tab = new VOTableTable();
            vr.setTable(tab);
            tab.getFields().addAll(DataLink.getFields());

            String runID = job.getID();
            if (job.getRunID() != null)
                runID = job.getRunID();
            CaomTapQuery query = new CaomTapQuery(tapURL, runID);
            ArtifactProcessor ap = new ArtifactProcessor(runID, reg);
            ap.setDownloadOnly(downloadFilesOnly); 
            DynamicTableData dtd = null;
            if (downloadFilesOnly)
            {
                int limit = Integer.MAX_VALUE;
                if (maxrec != null)
                    limit = maxrec + 1;
                dtd = new DynamicTableData(limit, job, query, downloadFilesOnly, ap);
                tab.setTableData(dtd);
            }
            else
            {
                // generate links with maxrec limit but have to truncate between ID values
                dtd = new DynamicTableData(maxrec + 1, job, query, downloadFilesOnly, ap);
                Iterator<List<Object>> links = dtd.iterator();
                ListTableData tdata = new ListTableData();
                while (links.hasNext())
                {
                    tdata.getArrayList().add(links.next());
                }
                tab.setTableData(tdata);
            }

            // Add the generic service descriptor(s)
            InputStream is = LinkQueryRunner.class.getClassLoader().getResourceAsStream(SERVICES_RESOURCE);
            if (is == null)
            {
                throw new MissingResourceException(
                    "Resource not found: " + SERVICES_RESOURCE, LinkQueryRunner.class.getName(), SERVICES_RESOURCE);
            }
            VOTableReader reader = new VOTableReader();
            VOTableDocument serviceDocument = reader.read(is);
            // generic descriptors: this, cutout, maybe preview someday
            for (VOTableResource metaResource : serviceDocument.getResources())
            {
                setServiceURL(metaResource);
                vot.getResources().add(metaResource);
            }
            // dynamic link-specific descriptors
            Iterator<ServiceDescriptor> sdi = dtd.descriptors();
            while (sdi.hasNext())
            {
                ServiceDescriptor sd = sdi.next();
                VOTableResource metaResource = new VOTableResource("meta");
                metaResource.id = sd.getID();
                metaResource.utype = "adhoc:service";
                String val = sd.getResourceIdentifier().toASCIIString();
                metaResource.getParams().add(new VOTableParam("resourceIdentifier", "char", val.length(), false, val));
                metaResource.getParams().add(new VOTableParam("accessURL", "char", null, true, "place holder"));
                if (sd.standardID != null)
                {
                    val = sd.standardID.toASCIIString();
                    metaResource.getParams().add(new VOTableParam("standardID", "char", val.length(), false, val));
                }
                VOTableGroup inputParams = new VOTableGroup("inputParams");
                for (ServiceParameter p : sd.getInputParams())
                {
                    VOTableParam vp = new VOTableParam(p.getName(), p.getDatatype(), p.getArraysize(), p.isVarsize(), p.getValue());
                    vp.ref = p.getRef();
                    vp.ucd = p.getUcd();
                    vp.unit = p.unit;
                    vp.utype = p.utype;
                    vp.xtype = p.xtype;
                    vp.description = p.description;
                    if (p.getMin() != null || p.getMax() != null || !p.getOptions().isEmpty())
                    {
                        vp.setMin(p.getMin());
                        vp.setMax(p.getMax());
                        vp.getOptions().addAll(p.getOptions());
                    }
                    inputParams.getParams().add(vp);
                }
                metaResource.getGroups().add(inputParams);
                setServiceURL(metaResource);
                vot.getResources().add(metaResource);
            }

            TableWriter<VOTableDocument> writer;
            
            if (fmt == null || VOTableWriter.CONTENT_TYPE.equals(fmt) )
            {
                writer = new VOTableWriter();
                String contentType = VOTableWriter.CONTENT_TYPE + ";content=datalink";
                syncOutput.setHeader("Content-Type", contentType);
            }
            else if ( ManifestWriter.CONTENT_TYPE.equals(fmt) )
            {
                ap.setDownloadOnly(true);
                writer = new ManifestWriter(0, 1, 7); // these values rely on column order in DataLink.iterator
                syncOutput.setHeader("Content-Type", ManifestWriter.CONTENT_TYPE);
            }
            else
            {
                throw new UnsupportedOperationException("unknown format: " + fmt);
            }

            syncOutput.setResponseCode(HttpURLConnection.HTTP_OK);
            // TODO: enable VOTableWriter to detect truncate and put overflow indicator
            writer.write(vot, syncOutput.getOutputStream());

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
        catch(AccessControlException ex)
        {
            sendError(ex, "permission denied -- reason: " + ex.getMessage(), 403);
        }
        catch(CertificateException ex)
        {
            sendError(ex, "permission denied -- reason: invalid proxy certificate", 403);
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
            else if ( ThrowableUtil.isACause(t, AccessControlException.class) ) // CredUtil + CredClient
            {
                sendError(t, "permission denied -- reason: " + t.getCause().getMessage(), 403);
                return;
            }
            sendError(t, 500);
        }
    }

    private void setServiceURL(VOTableResource metaResource)
        throws URISyntaxException, MalformedURLException
    {
        // set the access URL
        VOTableParam accessURLParam = null;
        VOTableParam resourceIDParam = null;
        VOTableParam standardIDParam = null;
        for (VOTableParam param :  metaResource.getParams())
        {
            if (param.getName().equals("accessURL"))
                accessURLParam = param;
            else if (param.getName().equals("resourceIdentifier"))
                resourceIDParam = param;
            else if (param.getName().equals("standardID"))
                standardIDParam = param;
        }
        if (accessURLParam == null)
            throw new MissingResourceException(
                "accessURL parameter missing from " + SERVICES_RESOURCE, LinkQueryRunner.class.getName(), "accessURL");
        if (resourceIDParam == null)
            throw new MissingResourceException(
                "resourceIdentifier parameter missing from " + SERVICES_RESOURCE, LinkQueryRunner.class.getName(), "resourceIdentifier");
        if (standardIDParam == null)
            throw new MissingResourceException(
                "standardID parameter missing from " + SERVICES_RESOURCE, LinkQueryRunner.class.getName(), "resourceIdentifier");

        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (am == null)
        {
            am = AuthMethod.ANON;
        }
        RegistryClient regClient = new RegistryClient();
        URI serviceID = URI.create(resourceIDParam.getValue());
        URI standardID = URI.create(standardIDParam.getValue());
        URL serviceURL = regClient.getServiceURL(serviceID, standardID, am);

        VOTableParam newAccessURL = new VOTableParam(
            accessURLParam.getName(), accessURLParam.getDatatype(), accessURLParam.getArraysize(),
            accessURLParam.isVariableSize(), serviceURL.toString());
        metaResource.getParams().remove(accessURLParam);
        metaResource.getParams().add(newAccessURL);
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
            VOTableWriter writer = new VOTableWriter();
            syncOutput.setHeader("Content-Type", VOTableWriter.CONTENT_TYPE);
            syncOutput.setResponseCode(code);
            writer.write(t, syncOutput.getOutputStream());
        }
        catch(IOException ex)
        {
            log.debug("write error failed", ex);
        }
    }
}
