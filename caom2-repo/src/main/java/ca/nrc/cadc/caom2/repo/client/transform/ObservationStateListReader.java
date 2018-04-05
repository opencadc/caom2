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

package ca.nrc.cadc.caom2.repo.client.transform;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * It reads the output from the main harvesting end point in ascii and transform it into a list of objects
 *
 * @author jduran, pdowler
 *
 */
public class ObservationStateListReader extends AbstractListReader<ObservationState> {
    private static final Logger log = Logger.getLogger(ObservationStateListReader.class);
    
    public ObservationStateListReader() {
        super();
    }

    /**
     * Read and parse content into ObservationState(s).
     * 
     * @param in
     * @return list of ObservationState
     * @throws IOException failed to read
     * @throws IllegalArgumentException invalid mandatory content (collection observationID)
     * @throws ParseException invalid maxlastModified timestamp
     * @throws URISyntaxException invalid accMetaChecksum URI
     */
    @Override
    public List<ObservationState> read(Reader in) throws IOException, ParseException, URISyntaxException {
        final List<ObservationState> ret = new ArrayList<>(1000);
        LineNumberReader reader = new LineNumberReader(in, 16384);
        String line = reader.readLine();
        while (line != null) {
            try {
                line = line.trim();
                if (!line.isEmpty()) {
                    // <Observation.collection> <Observation.observationID> <Observation.maxLastModified> <Observation.accMetaChecksum>
                    String[] tokens = line.split("[ \t]"); // ICD says tabs but be generous and split of any whitespace
                    String collection = tokens[0];
                    String observationID = tokens[1];
                    ObservationURI uri = new ObservationURI(collection, observationID);
                    ObservationState os = new ObservationState(uri);
                    // 
                    if (tokens.length > 2 && tokens[2].length() > 0) {
                        os.maxLastModified = DateUtil.flexToDate(tokens[2], dateFormat);
                    }
                    if (tokens.length > 3 && tokens[3].length() > 0) {
                        os.accMetaChecksum = new URI(tokens[3]);
                    }
                    ret.add(os);
                }

                line = reader.readLine();
            } catch (Exception ex) {
                log.error("read failure on line " + reader.getLineNumber(), ex);
                throw ex;
            } finally {
                if (reader.getLineNumber() % 100 == 0) {
                    log.debug("read: line " + reader.getLineNumber());
                }
            }
        }
        return ret;
    }
}
