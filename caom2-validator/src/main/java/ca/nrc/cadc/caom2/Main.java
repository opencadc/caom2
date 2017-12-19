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
************************************************************************
 */

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public final class Main {

    private static final Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);
            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // Set debug mode
            CaomEntity.MCS_DEBUG = true;
            if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
            } else if (am.isSet("v") || am.isSet("verbose")) {
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
            } else {
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.WARN);
            }

            if (am.getPositionalArgs().isEmpty()) {
                usage();
                System.exit(1);
            }
            
            String fname = am.getPositionalArgs().get(0);
            File f = new File(fname);
            ObservationReader r = new ObservationReader();
            Observation obs = r.read(new FileReader(f));
            
            if (am.getPositionalArgs().size() == 2) {
                File out = new File(am.getPositionalArgs().get(1));
                ObservationWriter w = new ObservationWriter();
                w.write(obs, new FileWriter(out));
                log.info("wrote copy: " + out.getAbsolutePath());
            }
            
            List<Runnable> validators = new ArrayList<>();
            if (am.isSet("checksum")) {
                int depth = 5;
                if (am.isSet("depth")) {
                    try { 
                        depth = Integer.parseInt(am.getValue("depth"));
                    } catch (NumberFormatException ex) {
                        log.error("invalid depth: " + am.getValue("depth") + ": must be integer in [1,5]");
                        usage();
                        System.exit(1);
                    }
                }
                validators.add(new ChecksumValidator(obs, depth, am.isSet("acc")));
            }
            
            if (am.isSet("wcs")) {
                validators.add(new WCSValidator(obs));
            }

            for (Runnable v : validators) {
                log.info("START " + v.getClass().getName());
                v.run();
                log.info("DONE " + v.getClass().getName() + System.getProperty("line.separator"));
            }

        } catch (Throwable t) {
            log.error("unexpected failure", t);
            System.exit(1);
        }
        System.exit(0);
    }

    

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        String lineSep = System.getProperty("line.separator");
        sb.append(lineSep).append("usage: caom2 [-h|--help] [-v|--verbose|-d|--debug] <validation options> <observation xml file> [<output file>}");
        sb.append(lineSep).append("       <validation options> is one or more of:");
        sb.append(lineSep).append("");
        sb.append(lineSep).append("       --checksum [--acc] [--depth=1..5] : recompute and compare metaChecksum values");
        sb.append(lineSep).append("       --wcs : enable WCS validation");
        sb.append(lineSep).append("");
        sb.append(lineSep).append("       <output file> : reserialise to another file (pretty-print)");
        System.out.println(sb.toString());
    }
}
