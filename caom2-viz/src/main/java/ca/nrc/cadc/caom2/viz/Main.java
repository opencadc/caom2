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

package ca.nrc.cadc.caom2.viz;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author pdowler
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) {
        String cur = null;
        try {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            Level lvl = Level.WARN;
            if (am.isSet("d") || am.isSet("debug")) {
                lvl = Level.DEBUG;
            } else if (am.isSet("v") || am.isSet("verbose")) {
                lvl = Level.INFO;
            }

            Log4jInit.setLevel("ca.nrc.cadc.caom2.viz", lvl);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.types", lvl);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.wcs", lvl);
            Log4jInit.setLevel("ca.nrc.cadc.caom2.compute", lvl);

            String fname = am.getValue("in");
            String productID = am.getValue("productID");
            boolean recomp = am.isSet("r");
            boolean headless = am.isSet("headless");
            if (fname == null) {
                usage();
                System.exit(1);
            }
            File f = new File(fname);

            if (headless) {
                ComputeFromXML cu = new ComputeFromXML(f, productID);
                cu.doit();
            } else {
                VizUnion vu = new VizUnion(f, productID, recomp);
                vu.doit();
            }
        } catch (Exception ex) {
            log.error("failed to display: " + cur, ex);
        }
    }

    private static void usage() {
        System.out.println("usage: caom2-viz [-h|--help] (to see this message)");
        System.out.println("usage: caom2-viz [-v|--verbose|-d|--debug]");
        System.out.println("                 --in=<caom observation xml file>");
        System.out.println("                 [--productID=<caom plane productID>] (to view just that plane)");
    }
}
