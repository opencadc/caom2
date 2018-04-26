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

package ca.nrc.cadc.caom2.artifactvalidate;

import java.net.URI;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

/**
 * Command line entry point for running the caom2-artifact-validate tool.
 *
 * @author majorb
 */
public class Main {

    private static Logger log = Logger.getLogger(Main.class);
    private static int exitValue = 0;

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);
            
            if (am.isSet("d") || am.isSet("debug") || am.isSet("help")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2.artifactvalidate", Level.DEBUG);
                //Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
                //Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
            } else {
                Log4jInit.setLevel("ca.nrc.cadc.caom2.artifactvalidate", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
            }

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // setup optional authentication for harvesting from a web service
            Subject subject = null;
            if (am.isSet("netrc")) {
                subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            } else if (am.isSet("cert")) {
                subject = CertCmdArgUtil.initSubject(am);
            }
            if (subject == null) {
                log.error("Anonymous execution not supported.  Please use --netrc or --cert");
                usage();
                System.exit(-1);
            } else {
                AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
                log.debug("authentication using: " + meth);
            }

            if (!am.isSet("archive")) {
                log.error("Missing required parameter 'archive'");
                usage();
                System.exit(-1);
            }
            String archive = am.getValue("archive");
            
            if (!am.isSet("tap")) {
                log.error("Missing required parameter 'tap'");
                usage();
                System.exit(-1);
            }
            String tap = am.getValue("tap");
            URI tapResourceID = URI.create(tap);
            
            boolean summary = am.isSet("summary");

            exitValue = 2;
            
            ArtifactValidator validator = new ArtifactValidator(tapResourceID, archive, summary);

            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(validator)));

            Subject.doAs(subject, validator);

            exitValue = 0; // finished cleanly
        } catch (Throwable t) {
            log.error("uncaught exception", t);
            exitValue = -1;
            System.exit(exitValue);
        } finally {
            System.exit(exitValue);
        }
    }

    private static class ShutdownHook implements Runnable {
        
        ArtifactValidator validator;

        ShutdownHook(ArtifactValidator validator) {
            this.validator = validator;
        }

        @Override
        public void run() {
            validator.shutdown();
            if (exitValue != 0) {
                log.error("terminating with exit status " + exitValue);
            }
        }

    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caom2-artifact-validate [-d|--debug] [-h|--help] ...");
        sb.append("\n     --archive=<archive>");
        sb.append("\n     --tap=<tap resource ID> eg. ivo://cadc.nrc.ca/sc2tap");
        sb.append("\n     [--summary] Only print the validation summary");
        sb.append("\n\nAuthentication:");
        sb.append("\n     [--netrc|--cert=<pem file>]");
        sb.append("\n     --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n     --cert=<pem file> : read client certificate from PEM file");

        log.warn(sb.toString());
    }
}
