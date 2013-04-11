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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.auth.CertCmdArgUtil;

/**
 * Fits2caom command line arguments.
 * 
 * @author jburke
 */
public abstract class Argument
{
    // Argument constants.
    public static final String COLLECTION = "collection";
    public static final String CONFIG = "config";
    public static final String DEBUG = "debug";
    public static final String DEFAULT = "default";
    public static final String DUMPCONFIG = "dumpconfig";
    //public static final String GEN = "gen";
    
    public static final String IN = "in";
    public static final String KEEP = "keep";
    public static final String LOCAL = "local";
    public static final String LOG = "log";
    public static final String NETRC = "netrc";
    public static final String NETRC_SHORT = "n";
    public static final String OBSERVATION_ID = "observationID";
    public static final String OUT = "out";
    public static final String OVERRIDE = "override";
    public static final String PRODUCT_ID = "productID";
    public static final String TEMP = "temp";
    public static final String TEST = "test";
    public static final String URI = "uri";
    public static final String VERSION = "version";

    public static final String H = "h";
    public static final String HELP = "help";

    public static final String LOG_DEBUG_SHORT = "d";
    public static final String LOG_QUIET = "quiet";
    public static final String LOG_QUIET_SHORT = "q";
    public static final String LOG_VERBOSE = "verbose";
    public static final String LOG_VERBOSE_SHORT = "v";   
    
    // Array of valid arguments.
    public static final String[] ARGUMENTS = 
    {
        COLLECTION, CONFIG, DEBUG, DEFAULT,
        DUMPCONFIG, H, HELP, IN, KEEP, LOCAL, NETRC_SHORT, NETRC, LOG,
        OBSERVATION_ID, OUT, OVERRIDE, PRODUCT_ID,
        TEST, TEMP, URI,
        LOG_DEBUG_SHORT, LOG_QUIET, LOG_QUIET_SHORT, 
        LOG_VERBOSE, LOG_VERBOSE_SHORT
    };

    public static String usage()
    {
        String NEWLINE = System.getProperty("line.separator");
        
        StringBuilder sb = new StringBuilder();
        sb.append("usage: ").append(NEWLINE);

        sb.append("\tRequired arguments:").append(NEWLINE);
        
        sb.append("\t--").append(Argument.COLLECTION).append("=\t\t < Observation.collection >").append(NEWLINE);
        sb.append("\t--").append(Argument.OBSERVATION_ID).append("=\t < Observation.observationID >").append(NEWLINE);
        sb.append("\t--").append(Argument.PRODUCT_ID).append("=\t\t < Plane.productID >").append(NEWLINE);
        sb.append("\t--").append(Argument.URI).append("=\t\t\t < comma separated list of URI > (files to be ingested)").append(NEWLINE);
        sb.append("\t--").append(Argument.OUT).append("=\t\t\t < XML output filename >").append(NEWLINE);

        sb.append(NEWLINE);
        sb.append("Optional arguments:").append(NEWLINE);
        sb.append("\t-v | --verbose\t\t enable verbose output").append(NEWLINE);
        sb.append("\t-d | --debug\t\t enable debug output").append(NEWLINE);
        sb.append("\t-h | --help\t\t show this usage message").append(NEWLINE);
        sb.append("\t-").append(NETRC_SHORT).append(" | --").append(NETRC).append("\t\t use $HOME/.netrc to get http authentication credentials").append(NEWLINE);
        sb.append("\t--").append(Argument.IN).append("\t\t\t < XML input filename >").append(NEWLINE);
        sb.append("\t--").append(Argument.CONFIG).append("=\t\t < optional CAOM2 utype to keyword config file to merge with the internal configuration >").append(NEWLINE);
        sb.append("\t--").append(Argument.DEFAULT).append("=\t\t < file with default values for keywords >").append(NEWLINE);
        sb.append("\t--").append(Argument.DUMPCONFIG).append("\t\t output the utype to keyword mapping to the console").append(NEWLINE);
        sb.append("\t--").append(Argument.OVERRIDE).append("=\t\t < file with override values for keywords >").append(NEWLINE);
        sb.append("\t--").append(Argument.LOCAL).append("=\t\t\t < list of files in local filesystem > (same order as ").append(Argument.URI).append(")").append(NEWLINE);
        sb.append("\t--").append(Argument.LOG).append("=\t\t\t < log file name > (instead of console)").append(NEWLINE);
        sb.append("\t--").append(Argument.TEST).append("\t\t\t test mode, do not persist to database").append(NEWLINE);
        sb.append("\t--").append(Argument.VERSION).append("\t\t output the version number of fits2caom2 and included jars").append(NEWLINE);
        sb.append("\t").append(CertCmdArgUtil.getCertArgUsage()).append(NEWLINE);
        return sb.toString();
    }
}
