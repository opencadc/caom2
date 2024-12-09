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

package ca.nrc.cadc.caom2.remove;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.harvester.HarvestResource;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.Console;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Command line entry point for running the caom2-remove tool.
 *
 * @author jeevesh
 */
public class Main {

    private static Logger log = Logger.getLogger(Main.class);
    private static int exitValue = 0;
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static int batchSize;

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2.remove", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
            } else {
                Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
            }

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // required args
            String collection = am.getValue("collection");
            boolean nocol = (collection == null || collection.trim().length() == 0);
            if (nocol) {
                log.warn("missing required argument: --collection=<name>");
                usage();
                System.exit(1);
            }

            String database = am.getValue("database");
            boolean nodest = (database == null || database.trim().length() == 0);
            if (nodest) {
                log.warn("missing required argument: --database=<server.database.schema>");
                usage();
                System.exit(1);
            }
            String[] destDS = database.split("[.]");
            if (destDS.length != 3) {
                log.warn("malformed --database value, found " + database + " expected: <server.database.schema>");
                usage();
                System.exit(1);
            }
            HarvestResource target = new HarvestResource(destDS[0], destDS[1], destDS[2], collection);

            String source = am.getValue("source");
            boolean nosource = am.isSet("nosource");
            if (!nosource && source == null) {
                log.warn("missing required argument: one of --source | --nosource must be specified");
                usage();
                System.exit(1);
            } else if (nosource && source != null) {
                log.warn("only one of --source and --nosource can be specified");
                usage();
                System.exit(1);
            }
            
            HarvestResource src = null;
            if (!nosource) {
                int sourceType = getSourceType(source);
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
            }

            // Optional arguments
            String batchSizeParam = am.getValue("batchSize");
            boolean nobatchsize = (batchSizeParam == null || batchSizeParam.trim().length() == 0);
            if (nobatchsize) {
                batchSize = DEFAULT_BATCH_SIZE;
            } else {
                try {
                    batchSize = Integer.parseInt(batchSizeParam);
                } catch (NumberFormatException nfe) {
                    log.error("invalid batch size: " + batchSizeParam);
                    usage();
                    System.exit(1);
                }
            }

            

            // Assert: at this point the source has been validated to be either a resource ID starting with ivo:
            // or a server + database + scheme combination where the collection can be found.

            Console console = System.console();
            if (console == null) {
                log.error("No console: can not continue non-interactive mode! Quitting.\n");
                System.exit(1);
            }

            System.out.print("\nAre you sure you want to remove this collection? Action can't be undone.\nRe-enter collection name to continue: ");
            String userAnswer = console.readLine();


            if (!userAnswer.equals(collection)) {
                log.error("Collection name does not match. Quitting.\n");

            } else {
                log.info("Continuing to remove " + collection + "...\n");

                Runnable action = null;

                try {
                    ObservationRemover cv = new ObservationRemover(target, src, batchSize);
                    action = cv;
                } catch (IOException ioex) {

                    log.error("failed to init: " + ioex.getMessage());
                    exitValue = -1;
                    System.exit(exitValue);
                }

                exitValue = 2; // in case we get killed
                Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
                // setup optional authentication for harvesting from a web service
                Subject subject = AuthenticationUtil.getAnonSubject();

                if (subject != null) {
                    Subject.doAs(subject, new RunnableAction(action));
                }

                exitValue = 0; // finished cleanly
            }
        } catch (Throwable t) {
            log.error("uncaught exception", t);
            log.error("Done, with errors.");
            exitValue = -1;
            System.exit(exitValue);
        } finally {
            log.info("Done.");
            System.exit(exitValue);
        }
    }

    private static class ShutdownHook implements Runnable {

        ShutdownHook() {
        }

        @Override
        public void run() {
            if (exitValue != 0) {
                log.error("terminating with exit status " + exitValue);
            }
        }

    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caom2-remove [-v|--verbose|-d|--debug] [-h|--help] --collection=<collection> --database=<server.database.schema>");
        sb.append("\n         --collection=<name> : name of collection to remove (e.g. IRIS)");
        sb.append("\n         --database=<server.database.schema> : collection location");
        sb.append("\none of: ");
        sb.append("\n         --source=<server.database.schema> | <resource ID> : to remove a harvested collection (e.g. ivo://cadc.nrc.ca/source-repo)");
        sb.append("\n         --nosource : to remove a non-harvested collection (e.g. a repository collection)");
        sb.append("\n\nOptional parameters:");
        sb.append("\n         --batchSize=<integer> :  default = 10000");
        log.warn(sb.toString());
    }
    
    // copied from caom2harvester Main
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