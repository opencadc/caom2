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
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.Console;
import java.io.IOException;
import java.net.URI;
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

            String source = am.getValue("source");
            boolean nosource = (source == null || source.trim().length() == 0);
            if (nosource) {
                log.warn("missing required argument: --source=<server.database.schema> | <resource ID>");
                usage();
                System.exit(1);
            }

            String[] sourceDS = null;

            HarvestResource target = new HarvestResource(destDS[0], destDS[1], destDS[2], collection);
            HarvestResource src = null;

            URI resourceURI = null;
            try {
                // Try make it a uri first
                resourceURI = new URI(source);
                String scheme = resourceURI.getScheme();
                if (!scheme.equals("ivo")) {
                    // must have a scheme, and it must be 'ivo'
                    log.warn("malformed --source value. Found scheme '" + scheme + "', expected: 'ivo'");
                    usage();
                    System.exit(1);
                } else {
                    src = new HarvestResource(resourceURI, collection);
                }
            } catch (Exception e) {
                sourceDS = source.split("[.]");
                if (sourceDS.length != 3) {
                    log.warn("malformed --source value, found '" + source + "', expected: <server.database.schema>");
                    usage();
                    System.exit(1);
                } else {
                    src = new HarvestResource(sourceDS[0],sourceDS[1],sourceDS[2], collection, false);
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

                int batchSize = 100;
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
        sb.append("\n\nusage: caom2-remove [-d|--debug] [-h|--help] ...");
        sb.append("\n         --collection=<name> : name of collection to remove (e.g. IRIS)");
        sb.append("\n         --database=<server.database.schema> : collection location");
        sb.append("\n         --source=<server.database.schema> | <resource ID> :  (e.g. ivo://cadc.nrc.ca/caom2repo)");
        log.warn(sb.toString());
    }
}