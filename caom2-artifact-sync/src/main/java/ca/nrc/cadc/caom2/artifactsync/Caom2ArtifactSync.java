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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Base class to parse the shared command line inputs for all modes. 
 * It handles the case when no mode is specified.
 *
 * @author majorb
 */
public abstract class Caom2ArtifactSync {

    private static Logger log = Logger.getLogger(Caom2ArtifactSync.class);
    public static String DEFAULT_APPLICATION_NAME = "caom2-artifact-sync";

    private String asClassName;
    private Exception asException;
    private String exceptionMsg;
    private int exitValue = 0;

    protected ArtifactStore artifactStore;
    protected String applicationName = DEFAULT_APPLICATION_NAME;
    protected String mode;
    protected String errorMsg;
    protected Subject subject;
    protected String[] dbInfo;
    protected Map<String, Object> daoConfig;

    // when isDone is true, do not execute the command any further
    protected boolean isDone = false;
    
    public abstract void execute() throws Exception;
    
    protected abstract void printUsage();
    
    public static String getApplicationName() {
        String appName = Caom2ArtifactSync.DEFAULT_APPLICATION_NAME;
        // A custom application name can be passed in via this environment variable
        String importedName = System.getProperty("ca.nrc.cadc.caom2.artifactsync.Main.name");
        if (StringUtil.hasText(importedName)) {
            appName = importedName;
        }
        
        return appName;
    }
    
    public Caom2ArtifactSync(ArgumentMap am) {
        init(am);

        if (am.isSet("h") || am.isSet("help")) {
            this.printUsage();;
            this.setExitValue(0);
        } else {
            log.debug("Artifact store class: " + asClassName);

            if (StringUtil.hasText(errorMsg)) {
                printErrorUsage(errorMsg);
            } else if (StringUtil.hasText(exceptionMsg)) {
                this.logException(exceptionMsg, asException);
            }
        }
    }
    
    public void printErrorUsage(String msg) {
        log.error(msg);
        this.printUsage();
        this.setExitValue(-1);
    }
    
    public int getExitValue() {
        return exitValue;
    }

    protected class ShutdownHook implements Runnable {
        List<ShutdownListener> listeners;

        ShutdownHook(List<ShutdownListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void run() {
            for (ShutdownListener listener : listeners) {
                listener.shutdown();
            }

            if (exitValue != 0) {
                log.error("terminating with exit status " + exitValue);
            }
        }
    }
    
    protected void setIsDone(boolean value) {
        this.isDone = value;
    }
 
    protected void setExitValue(int value) {
        this.exitValue = value;
    }
    
    protected void logException(String msg, Exception ex) {
        log.error(msg, ex);
        this.setExitValue(-1);
        this.isDone = true;
    }

    protected void parseDbParam(ArgumentMap am, String source) {
        String database = am.getValue(source);
        if (!StringUtil.hasText(database)) {
            String msg = "Must specify database." ;
            this.printErrorUsage(msg);
        } else if (database.equalsIgnoreCase("true")) {
            String msg = "Must specify source with database=";
            this.printErrorUsage(msg);
        } else {
            // database=<server.database.schema>
            String [] tempDbInfo = database.split("[.]");
            if (tempDbInfo.length == 3) {
                this.dbInfo = tempDbInfo;
                this.daoConfig = new HashMap<>(2);
                this.daoConfig.put("server", dbInfo[0]);
                this.daoConfig.put("database", dbInfo[1]);
                this.daoConfig.put("schema", dbInfo[2]);
                this.daoConfig.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            } else {
                String msg = "database must be <server.database.schema>.";
                this.printErrorUsage(msg);
            }
        }
    }
    
    protected String parseCollection(ArgumentMap am) {
        String collection = am.getValue("collection");
        if (!StringUtil.hasText(collection)) {
            String msg = "Must specify collection.";
            this.printErrorUsage(msg);
        } else if (collection.equalsIgnoreCase("true")) {
            String msg = "Must specify collection with collection=";
            this.printErrorUsage(msg);
        }
        
        return collection;
    }
    
    protected void createSubject(ArgumentMap am) {
        if (am.isSet("netrc")) {
            this.subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
        } else if (am.isSet("cert")) {
            this.subject = CertCmdArgUtil.initSubject(am);
        }
        
        if (this.subject != null) {
            AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            log.debug("authentication using: " + meth);
        }
    }
    
    private void init(ArgumentMap am) {
        this.applicationName = getApplicationName();
        this.mode = am.getPositionalArgs().get(0);
        String asPackage = loadArtifactStore(am);
        setLogLevel(am, asPackage);
        this.createSubject(am);
    }
    
    private static void setLogLevel(ArgumentMap am, String asPackage) {
        if (am.isSet("d") || am.isSet("debug")) {
            Log4jInit.setLevel("ca.nrc.cadc.caom2.artifactsync", Level.DEBUG);
            Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.DEBUG);
            Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
            Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
            if (asPackage != null) {
                Log4jInit.setLevel(asPackage, Level.DEBUG);
            }
        } else if (am.isSet("v") || am.isSet("verbose")) {
            Log4jInit.setLevel("ca.nrc.cadc.caom2.artifactsync", Level.INFO);
            Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.INFO);
            if (asPackage != null) {
                Log4jInit.setLevel(asPackage, Level.INFO);
            }
        } else {
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.WARN);
            if (asPackage != null) {
                Log4jInit.setLevel(asPackage, Level.WARN);
            }
        }
        
        if (am.isSet("profile")) {
            Log4jInit.setLevel("ca.nrc.cadc.profiler", Level.INFO);
        }
    }
    
    private String loadArtifactStore(ArgumentMap am) {
        this.asClassName = am.getValue("artifactStore");
        String asPackage = null;
        
        if (asClassName != null) {
            try {
                Class<?> asClass = Class.forName(asClassName);
                asPackage = asClass.getPackage().getName();
                this.artifactStore = (ArtifactStore) asClass.newInstance();
            } catch (ClassNotFoundException cnfe) {
                this.exceptionMsg = "Failed to load store class:." + asClassName;
                this.asException = cnfe;
            } catch (Exception e) {
                this.exceptionMsg = "Failed to access store class " + asClassName;
                this.asException = e;
            }
        } else {
            this.errorMsg = "Must specify artifactStore";
        }
        
        return asPackage;
    }
}
