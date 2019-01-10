/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.access.AccessUtil;
import ca.nrc.cadc.caom2.access.ArtifactAccess;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.caom2.xml.ArtifactAccessWriter;
import ca.nrc.cadc.dali.ParamExtractor;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class AccessQueryRunner implements JobRunner {
    private static final Logger log = Logger.getLogger(AccessQueryRunner.class);

    private JobLogInfo logInfo;
    private JobUpdater jobUpdater;
    private Job job;
    private SyncOutput syncOutput;
    
    private Map<String,List<String>> params;
    
    public AccessQueryRunner() { 
    }

    @Override
    public void setJobUpdater(JobUpdater jobUpdater) {
        this.jobUpdater = jobUpdater;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
        List<String> pnames = new ArrayList<String>();
        pnames.add("ID");
        ParamExtractor pe = new ParamExtractor(pnames);
        this.params = pe.getParameters(job.getParameterList());
    }

    @Override
    public void setSyncOutput(SyncOutput syncOutput) {
        this.syncOutput = syncOutput;
    }

    @Override
    public void run() {
        long t1 = System.currentTimeMillis();
        try {
            this.logInfo = new JobLogInfo(job);
            logInfo.setSubject(AuthenticationUtil.getCurrentSubject());
            log.info(logInfo.start());
            doit();
        } finally {
            long dt = System.currentTimeMillis() - t1;
            logInfo.setElapsedTime(dt);
            log.info(logInfo.end());
        }
    }

    private String getParamValue(String s) {
        List<String> vals = params.get(s);
        if (vals != null) {
            Iterator<String> iter = vals.iterator();
            if (iter.hasNext()) {
                String ret = iter.next();
                if (iter.hasNext()) {
                    throw new IllegalArgumentException("badArgument");
                }
                return ret;
            }
        }
        return null;
    }
    
    private void doit() {
        logInfo.setSuccess(false);
        
        try {
            try {
                checkPermission();
                String id = getParamValue("ID");
                URI uri = null;
                try {
                    uri = new URI(id);
                } catch (Exception ex) {
                    throw new IllegalArgumentException("missing/invalid ID: " + id, ex);
                }
                ReadAccessDAO dao = getDAO(uri);
                ReadAccessDAO.RawArtifactAccess raa = dao.getArtifactAccess(uri);
                if (raa == null) {
                    throw new ResourceNotFoundException("not found: " + id);
                }
                List<URI> metaReadAccessGroups = new ArrayList<URI>();
                List<URI> dataReadAccessGroups = new ArrayList<URI>();
                for (URI mra : raa.metaReadAccessGroups) {
                    metaReadAccessGroups.add(mra);
                }
                for (URI dra : raa.dataReadAccessGroups) {
                    dataReadAccessGroups.add(dra);
                }
                ArtifactAccess aa = AccessUtil.getArtifactAccess(raa.artifact, 
                        raa.metaRelease, metaReadAccessGroups, 
                        raa.dataRelease, dataReadAccessGroups);
                ArtifactAccessWriter w = new ArtifactAccessWriter();
                syncOutput.setHeader("Content-Type", "text/xml");
                syncOutput.setCode(200);
                w.write(aa, syncOutput.getOutputStream());
                logInfo.setSuccess(true);
                
            } catch (IllegalArgumentException ex) {
                logInfo.setSuccess(true);
                logInfo.setMessage(ex.getMessage());
                sendError(400, ex.getMessage());
            } catch (ResourceNotFoundException ex) {
                logInfo.setSuccess(true);
                logInfo.setMessage(ex.getMessage());
                sendError(404, ex.getMessage());
            } catch (AccessControlException ex) {
                logInfo.setSuccess(true);
                logInfo.setMessage(ex.getMessage());
                sendError(401, ex.getMessage());
            } catch (UnsupportedOperationException ex) {
                log.error("oops", ex);
                logInfo.setSuccess(true);
                String msg = ex.getMessage();
                if (msg == null) {
                    msg = "not implemented";
                }
                logInfo.setMessage(msg);
                sendError(400, msg);
            } catch (IOException ex) {
                log.error("FAIL", ex);
                sendError(500, "failed to access content");
            }
        } catch (IOException ex) {
            // failed to send error to caller
            log.debug("failed to send error to caller", ex);
        } catch (Throwable t) {
            try {
                logInfo.setMessage(t.toString());
                log.error("FAIL", t);
                sendError(500, t.toString());
            } catch (Throwable t2) {
                log.debug("failed to send error to caller", t2);
            }
        }
    }
    
    private void sendError(int code, String msg) throws IOException {
        syncOutput.setResponseCode(code);
        syncOutput.setHeader("Content-Type", "text/plain");
        
        PrintWriter w = new PrintWriter(syncOutput.getOutputStream());
        w.println(msg);
        w.close();
    }
    
    private ReadAccessDAO getDAO(URI uri) throws ResourceNotFoundException {
        String[] ss = job.getRequestPath().split("/");
        String srv = ss[1];
        log.debug("job.requestpath: " + job.getRequestPath() + " srv: " + srv);
        File config = new File(System.getProperty("user.home") + "/config/" + srv + ".properties");
        try {
            CaomRepoConfig cf = new CaomRepoConfig(config);
            Iterator<CaomRepoConfig.Item> i = cf.iterator();
            while (i.hasNext()) {
                CaomRepoConfig.Item item = i.next();
                if (archiveMatch(uri, item.getArtifactPattern())) {
                    ReadAccessDAO ret = new ReadAccessDAO();
                    ret.setConfig(cf.getDAOConfig(item.getCollection()));
                    return ret;
                }
            }
            throw new ResourceNotFoundException("not found: " + uri);
        } catch (IOException ex) {
            throw new RuntimeException("CONFIG: failed to read config from " + config.getAbsolutePath());
        }
    }
    
    private boolean archiveMatch(URI uri, String pattern) {
        if (pattern == null) {
            return false;
        }
        if (uri.toASCIIString().contains(pattern)) {
            return true;
        }
        return false;
    }
    
    private void checkPermission() {
        String fname = AccessQueryRunner.class.getSimpleName() + ".properties";
        URL url = null;
        try {
            url = AccessQueryRunner.class.getClassLoader().getResource(fname);
            if (url != null) {
                Properties config = new Properties();
                config.load(url.openStream());
                String dn = config.getProperty(AccessQueryRunner.class.getName() + ".allowed");
                X500Principal allowed = new X500Principal(dn);
                log.debug("allowed: " + allowed);
                Subject cur = AuthenticationUtil.getCurrentSubject();
                for (X500Principal p : cur.getPrincipals(X500Principal.class)) {
                    if (AuthenticationUtil.equals(allowed, p)) {
                        return;
                    }
                }
            } else {
                throw new RuntimeException("not found: " + fname);
            }
        } catch (Exception ex) {
            throw new RuntimeException("failed to read " + fname + " from " + url, ex);
        }
        throw new AccessControlException("permission denied");
    }
}
