/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Command line entry point for running the caom2-artifact-sync tool.
 *
 * @author majorb
 */
public class Main {

    private static Logger log = Logger.getLogger(Main.class);
    private static Caom2ArtifactSync command;

    public static void main(String[] args) {
        try {
            Log4jInit.setLevel("ca.nrc.cadc.caom2.artifactsync", Level.INFO);
            ArgumentMap am = new ArgumentMap(args);
            List<String> positionalArgs = am.getPositionalArgs();
            if (positionalArgs.size() == 0) {
                if (am.isSet("h") || am.isSet("help")) {
                    // help on caom2-artifact-sync
                    printUsage();
                } else {
                    String msg = "Missing a valid mode: discover, download, validate, diff.";
                    exitWithErrorUsage(msg);
                }
            } else if (positionalArgs.size() > 1) {
                String msg = "Only one valid mode is allowed: discover, download, validate, diff.";
                exitWithErrorUsage(msg);
            } else {
                // one mode is specified
                String mode = positionalArgs.get(0);
                if (mode.equals("discover") || mode.equals("download")) {
                    command = new Discover(am);
                } else if (mode.equals("validate") || mode.equals("diff")) {
                    command = new Validate(am);
                } else {
                    String msg = "Unsupported mode: " + mode;
                    exitWithErrorUsage(msg);
                }
            }

            command.execute();
        } catch (Throwable t) {
            log.error("uncaught exception", t);
            System.exit(-1);
        } finally {
            System.exit(command.getExitValue());
        }
    }

    private static void printUsage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append(Caom2ArtifactSync.getApplicationName()).append(" <mode> [mode-args] --artifactStore=<fully qualified class name>");
        sb.append("\n\n    use '").append(Caom2ArtifactSync.getApplicationName()).append(" <mode> <-h|--help>' to get help on a <mode>");
        sb.append("\n    where <mode> can be one of:");
        sb.append("\n        discover: Incrementally harvest artifacts");
        sb.append("\n        download: Download artifacts");
        sb.append("\n        validate: Discover missing artifacts and update the HarvestSkipURI table");
        sb.append("\n        diff: Discover and report missing artifacts");
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

    private static void exitWithErrorUsage(String msg) {
        log.error(msg);
        printUsage();
        System.exit(-1);
    }
}
