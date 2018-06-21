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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Command line entry point for running the caom2-artifact-sync tool.
 *
 * @author majorb
 */
public class Main {

    private static Logger log = Logger.getLogger(Main.class);
    private static int exitValue = 0;

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);

            String asClassName = am.getValue("artifactStore");
            ArtifactStore artifactStore = null;
            String asPackage = null;
            Exception asException = null;

            if (asClassName != null) {
                try {
                    Class<?> asClass = Class.forName(asClassName);
                    artifactStore = (ArtifactStore) asClass.newInstance();
                    asPackage = asClass.getPackage().getName();
                } catch (Exception e) {
                    asException = e;
                }
            }

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

            log.debug("Artifact store class: " + asClassName);

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            if (asClassName == null) {
                log.error("Must specify artifactStore.");
                usage();
                System.exit(-1);
            }
            if (asException != null) {
                log.error("Failed to load " + asClassName, asException);
                System.exit(-1);
            }

            // setup optional authentication for harvesting from a web service
            Subject subject = createSubject(am);

            if (subject != null) {
                AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
                log.info("authentication using: " + meth);
            }
            
            exitValue = 2;
            
            List<ShutdownListener> listeners = new ArrayList<ShutdownListener>(2);
            ArtifactHarvester harvester = null;
            ArtifactValidator validator = null;
            DownloadArtifactFiles downloader = null;
            if (am.isSet("harvest") || am.isSet("download")) {
                int batchSize = ArtifactHarvester.DEFAULT_BATCH_SIZE;
                if (am.isSet("batchsize")) {
                    try {
                        batchSize = Integer.parseInt(am.getValue("batchsize"));
                        if (batchSize < 1 || batchSize > 100000) {
                            log.error("value for --batchsize must be between 1 and 100000");
                            usage();
                            System.exit(-1);
                        }
                    } catch (NumberFormatException nfe) {
                        log.error("Illegal value for --batchsize: " + am.getValue("batchsize"));
                        usage();
                        System.exit(-1);
                    }
                }

                String dbParam = am.getValue("database");
                if (dbParam == null) {
                    log.error("Must specify database information.");
                    usage();
                    System.exit(-1);
                }
                
                String[] dbInfo = dbParam.split("[.]");
                Map<String, Object> daoConfig = new HashMap<>(2);
                daoConfig.put("server", dbInfo[0]);
                daoConfig.put("database", dbInfo[1]);
                daoConfig.put("schema", dbInfo[2]);
                daoConfig.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);

                if (am.isSet("harvest")) {
	                ObservationDAO observationDAO = new ObservationDAO();
	                observationDAO.setConfig(daoConfig);

	                if (!am.isSet("collection")) {
	                    log.error("Missing required parameter 'collection'");
	                    usage();
	                    System.exit(-1);
	                }
	                
	                String collection = am.getValue("collection");
	                
	                harvester = new ArtifactHarvester(
	                    observationDAO, dbInfo, artifactStore, collection, batchSize);
	                listeners.add(harvester);
	            }
	            
                if (am.isSet("download")) {
	                ArtifactDAO artifactDAO = new ArtifactDAO();
	                artifactDAO.setConfig(daoConfig);
	                
	                Integer retryAfterHours = null;
	                if (am.isSet("retryAfter")) {
	                    try {
	                        retryAfterHours = Integer.parseInt(am.getValue("retryAfter"));
	                    } catch (NumberFormatException e) {
	                        log.error("Illegal value for --retryAfter: " + am.getValue("retryAfter"));
	                        usage();
	                        System.exit(-1);
	                    }
	                }
	                
	                boolean verify = !am.isSet("noverify");

	                int nthreads = 1;
	                if (am.isSet("threads")) {
	                    try {
	                        nthreads = Integer.parseInt(am.getValue("threads"));
	                        if (nthreads < 1 || nthreads > 250) {
	                            log.error("value for --threads must be between 1 and 250");
	                            usage();
	                            System.exit(-1);
	                        }
	                    } catch (NumberFormatException nfe) {
	                        log.error("Illegal value for --threads: " + am.getValue("threads"));
	                        usage();
	                        System.exit(-1);
	                    }
	                }

	                downloader = new DownloadArtifactFiles(
	                    artifactDAO, dbInfo, artifactStore, nthreads, batchSize,
	                    retryAfterHours, verify);
	                listeners.add(downloader);
	            }
            }
            
            if (am.isSet("validate")) {
                if (subject == null) {
                    log.error("Anonymous execution not supported.  Please use --netrc or --cert");
                    usage();
                    System.exit(-1);
                } else {
                    AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
                    log.debug("authentication using: " + meth);
                }

                if (!am.isSet("collection")) {
                    log.error("Missing required parameter 'collection'");
                    usage();
                    System.exit(-1);
                }
                
                String collection = am.getValue("collection");
                
                boolean summary = am.isSet("summary");
                boolean reportOnly = am.isSet("reportOnly");

                String dbParam = am.getValue("database");
                if (dbParam == null) {
                    log.error("Must specify database information.");
                    usage();
                    System.exit(-1);
                }
                
                if (!am.isSet("source")) {
                    log.error("Missing required parameter 'source'");
                    usage();
                    System.exit(-1);
                }
                String source = am.getValue("source");
                if (source == null) {
                    log.error("Must specify source information.");
                    usage();
                    System.exit(-1);
                } else if (source.contains("ivo:")) {
                	// TAP Resource ID
                    URI tapResourceID = URI.create(source);
                    validator = new TapBasedValidator(tapResourceID, collection, summary, reportOnly, artifactStore);
                } else {
                	// database <server.database.schema>
                    String[] dbInfo = dbParam.split("[.]");
                    Map<String, Object> daoConfig = new HashMap<>(2);
                    daoConfig.put("server", dbInfo[0]);
                    daoConfig.put("database", dbInfo[1]);
                    daoConfig.put("schema", dbInfo[2]);
                    daoConfig.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
	                ObservationDAO observationDAO = new ObservationDAO();
	                observationDAO.setConfig(daoConfig);
                    
                    validator = new DbBasedValidator(observationDAO.getDataSource(), dbInfo, observationDAO, 
                    		collection, summary, reportOnly, artifactStore);
                }
                
                listeners.add(validator);
	            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(listeners)));
                Subject.doAs(subject, validator);
                exitValue = 0; // finished cleanly
            }  else {
	            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(listeners)));
	
	            int loopNum = 1;
	            boolean loop = am.isSet("continue");
	            boolean stopHarvest = false;
	            boolean stopDownload = false;
	            do {
	                if (loop) {
	                    log.info("-- STARTING LOOP #" + loopNum + " --");
	                }
	
	                if (!stopHarvest && am.isSet("harvest")) {
	                    if (subject != null) {
	                        stopHarvest = Subject.doAs(subject, harvester) == 0;
	                    } else {
	                        stopHarvest = harvester.run() == 0;
	                    }
	                }
	
	                if (!stopDownload && am.isSet("download")) {
	                    if (subject != null) {
	                        stopDownload = Subject.doAs(subject, downloader) == 0;
	                    } else {
	                        stopDownload = downloader.run() == 0;
	                    }
	                }
	
	                if (loop) {
	                    log.info("-- ENDING LOOP #" + loopNum + " --");
	                }
	
	                loopNum++;
	                
	                // re-initialize the subject
	                subject = createSubject(am);
	                
	            } while (loop && !stopHarvest && !stopDownload); // continue if work was done

	            exitValue = 0; // finished cleanly
            }
        } catch (Throwable t) {
            log.error("uncaught exception", t);
            exitValue = -1;
            System.exit(exitValue);
        } finally {
            System.exit(exitValue);
        }        

    }
    
    private static Subject createSubject(ArgumentMap am) {
        Subject subject = null;
        if (am.isSet("netrc")) {
            subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
        } else if (am.isSet("cert")) {
            subject = CertCmdArgUtil.initSubject(am);
        }
        return subject;
    }

    private static class ShutdownHook implements Runnable {
        
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

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caom2-artifact-sync <mode> [mode-args] --artifactStore=<fully qualified class name>");
        sb.append("\n\n    where <mode> can be one of:");
        sb.append("\n\n        --harvest: Incrementally harvest artifacts");
        sb.append("\n            [mode-args]:");
        sb.append("\n                --database=<server.database.schema>");
        sb.append("\n                --batchsize=<integer> Max observations to check (default: 1000)");
        sb.append("\n                --continue : Repeat batches until no work left");
        sb.append("\n                --collection=<collection> : The collection to harvest");
        sb.append("\n\n        --download: Download artifacts");
        sb.append("\n            [mode-args]:");
        sb.append("\n                --database=<server.database.schema>");
        sb.append("\n                --threads=<integer> : Number of download threads (default: 1)>");
        sb.append("\n                --batchsize=<integer> Max skip URIs to download (default: 1000)");
        sb.append("\n                --continue : Repeat batches until no work left");
        sb.append("\n                --retryAfter=<integer> Hours after failed downloads should be retried (default: 24)");
        sb.append("\n                --noverify : Do not confirm by MD5 sum after download");
        sb.append("\n\n        --validate: Discover missing artifacts");
        sb.append("\n            [mode-args]:");
        sb.append("\n                --source=<server.database.schema | TAP resource ID>");
        sb.append("\n                    (If source is TAP resourceID, reportOnly is always true)");
        sb.append("\n                --reportOnly : Do not create harvest skip records");
        sb.append("\n                --summary : Show only summary validation information");
        sb.append("\n                --collection=<collection> : The collection to validate");
        sb.append("\n\n    optional general args:");
        sb.append("\n        -v | --verbose");
        sb.append("\n        -d | --debug");
        sb.append("\n        -h | --help");
        sb.append("\n        --profile : Profile task execution");
        sb.append("\n\n    authentication:");
        sb.append("\n        [--netrc|--cert=<pem file>]");
        sb.append("\n        --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n        --cert=<pem file> : read client certificate from PEM file");

        log.warn(sb.toString());
    }
}
