/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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

package ca.nrc.cadc.caom2.validator.collection;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author hjeeves
 */
public class Main {
    private static Logger log = Logger.getLogger(Main.class);
    
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static int exitValue = 0;

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2.validator.collection", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.version", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
            } else if (am.isSet("v") || am.isSet("verbose")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2.validator.collection", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.version", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.INFO);
            } else {
                Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            }

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // setup optional authentication for harvesting from a web service
            Subject subject = AuthenticationUtil.getAnonSubject();
            if (am.isSet("netrc")) {
                subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            } else if (am.isSet("cert")) {
                subject = CertCmdArgUtil.initSubject(am);
            }
            AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            log.info("authentication using: " + meth);

            // required args
            String collection = am.getValue("collection");
            boolean nocol = (collection == null || collection.trim().length() == 0);
            if (nocol) {
                log.warn("missing required argument: --collection=<name>");
                usage();
                System.exit(1);
            }

            int nthreads = 1;
            HarvestResource src;
            src = getSource(am, collection);
            if (src.getResourceType() != HarvestResource.SOURCE_DB) {
                try {
                    if (am.isSet("threads")) {
                        nthreads = Integer.parseInt(am.getValue("threads"));
                    }
                } catch (NumberFormatException ex) {
                    log.warn("invalid value for --threads parameter: " + am.getValue("threads") + " -- must be an integer");
                    usage();
                    System.exit(1);
                }
            }

            String progressFileName = am.getValue("progressFile");
            if (progressFileName == null || progressFileName.trim().length() == 0) {
                log.error("progress file name required (--progressFile)");
                usage();
                System.exit(1);
            }

            // Optional args
            Integer batchSize = null;
            String sbatch = am.getValue("batchSize");

            if (sbatch != null && sbatch.trim().length() > 0) {
                try {
                    batchSize = new Integer(sbatch);
                } catch (NumberFormatException nex) {
                    usage();
                    log.error("value for --batchSize must be an integer, found: " + sbatch);
                    System.exit(1);
                }
            }

            if (batchSize == null) {
                log.debug("no --batchSize specified: defaulting to " + DEFAULT_BATCH_SIZE);
                batchSize = DEFAULT_BATCH_SIZE;
            }


            Date minDate = null;
            String minDateStr = am.getValue("minDate");
            if (minDateStr != null && minDateStr.trim().length() > 0) {
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                try {
                    minDate = df.parse(minDateStr);
                } catch (ParseException ex) {
                    log.error("invalid minDate: " + minDateStr + " reason: " + ex);
                    usage();
                    System.exit(1);
                }
            }

            // maxDate is set to 'Now'
            Date maxDate = new Date();
            String maxDateStr = am.getValue("maxDate");
            if (maxDateStr != null && maxDateStr.trim().length() > 0) {
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                try {
                    maxDate = df.parse(maxDateStr);
                } catch (ParseException ex) {
                    log.error("invalid maxDate: " + maxDateStr + " reason: " + ex);
                    usage();
                    System.exit(1);
                }
            }

            boolean computePlaneMetadata = am.isSet("compute");

            File progressFile = new File(progressFileName);
            ObservationValidator obsValidator = new ObservationValidator(src, progressFile, batchSize, nthreads);
            obsValidator.setMinDate(minDate);
            obsValidator.setMaxDate(maxDate);
            obsValidator.setCompute(computePlaneMetadata);
            Runnable action = obsValidator;

            exitValue = 2; // in case we get killed
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(obsValidator)));

            if (subject != null) {
                Subject.doAs(subject, new RunnableAction(action));
            }

            exitValue = 0; // finished cleanly
        } catch (Exception e) {
            log.error("uncaught exception", e);
            exitValue = -1;
            System.exit(exitValue);
        } finally {
            System.exit(exitValue);
        }
    }

    private static class ShutdownHook implements Runnable {

        private ObservationValidator obsValidator;

        ShutdownHook(ObservationValidator obsValidator) {
            this.obsValidator = obsValidator;
        }

        @Override
        public void run() {
            this.obsValidator.printAggregateReport();

            if (exitValue != 0) {
                System.out.println("\nTerminated with exit status " + exitValue + ". progress file shows last observation being processed.");
                log.error("terminating with exit status " + exitValue);
            }
        }

    }
    
    private static HarvestResource getSource(ArgumentMap am, String collection) {
        // source can be a database or service
        String source = am.getValue("source");

        boolean nosrc = (source == null || source.trim().length() == 0);
        if (nosrc) {
            log.warn("missing required argument: --source=<server.database.schema> | <resourceID> | <capabilities URL>");
            usage();
            System.exit(1);
        }

        int sourceType = getSourceType(source);

        HarvestResource src = null;
        int nthreads = 1;
        switch (sourceType) {
            case HarvestResource.SOURCE_URI:
                try {
                    src = new HarvestResource(new URI(source), collection);
                } catch (URISyntaxException ex) {
                    log.warn("invalid value for --source parameter: " + source + " reason: " + ex.toString());
                    usage();
                    System.exit(1);
                }   
                break;
            case HarvestResource.SOURCE_DB:
                String[] srcDS = source.split("[.]");
                if (srcDS.length != 3) {
                    log.warn("malformed --source value, found " + source + " expected: server.database.schema");
                    usage();
                    System.exit(1);
                }   
                src = new HarvestResource(srcDS[0], srcDS[1], srcDS[2], collection);
                break;
            case HarvestResource.SOURCE_CAP_URL:
                try {
                    src = new HarvestResource(new URL(source), collection);
                } catch (MalformedURLException ex) {
                    log.warn("invalid value for --source parameter: " + source + " reason: " + ex.toString());
                    usage();
                    System.exit(1);
                }  
                break;
            default:
                log.warn("invalid value for --source parameter: " + source + " reason: Impossible to identify source type.");
                usage();
                System.exit(1);
        }
        return src;
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caom2-collection-validator [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n         --collection=<name> : name of collection to retrieve> (e.g. IRIS)");
        sb.append("\n         --source=<server.database.schema> | <resourceID> | <capabilities URL>");
        sb.append("\n         --progressFile=<filename> : file containing information on last successfully processed observation. ");

        sb.append("\n\nSource selection:");
        sb.append("\n          <server.database.schema> : the server and database connection info will be found in $HOME/.dbrc");
        sb.append("\n          <resourceID> : resource identifier for a registered caom2 repository service (e.g. ivo://cadc.nrc.ca/ams)");
        sb.append("\n          <capabilities URL> : direct URL to a VOSI capabilities document with caom2 repository endpoints (use: unregistered service)");
        sb.append("\n         [--threads=<num threads>] : number  of threads used to read observation documents (service only, default: 1)");

        sb.append("\n\nOptional authentication: [--netrc|--cert=<pem file>] (default: anonymous)");
        sb.append("\n         --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n         --cert=<pem file> : read client certificate from PEM file");

        sb.append("\n\nOptional modifiers:");
        sb.append("\n         --batchSize=<number of observations per batch> (default: ").append(DEFAULT_BATCH_SIZE).append(")");
        sb.append("\n         --compute : compute plane metadata to validate it can be done. (default: false) ");
        sb.append("\n         --minDate=<minimum> : Observation.maxLastModfied to consider (UTC timestamp)");
        sb.append("\n         --maxDate=<maximum> : Observation.maxLastModfied to consider (UTC timestamp)");

        sb.append("\n");
        log.warn(sb.toString());
    }

    private static int getSourceType(String source) {
        // Try source as URL
        try {
            new URL(source);
            return HarvestResource.SOURCE_CAP_URL;
        } catch (MalformedURLException e) {
            // Not a valid URL
        }

        // Try source as resourceUri
        if (source.startsWith("ivo://")) {
            try {
                new URI(source);
                return HarvestResource.SOURCE_URI;
            } catch (URISyntaxException e) {
                // Not a valid resourceID
            }
        }

        // Try source as DB
        String[] srcDS = source.split("[.]");
        if (srcDS.length == 3) {
            return HarvestResource.SOURCE_DB;
        }

        return HarvestResource.SOURCE_UNKNOWN;
    }
}
