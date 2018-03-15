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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * It reads the output from the main harvesting end point in ascii and transform it into a list of objects
 *
 * @author jduran
 *
 */
public class ObservationStateListReader extends AbstractListReader {

    public ObservationStateListReader(DateFormat dateFormat, char separator, char endOfLine) {
        super(dateFormat, separator, endOfLine);
    }

    @Override
    public List read(ByteArrayOutputStream bos) throws ParseException, IOException, URISyntaxException {
        // <Observation.collection> <Observation.observationID> <Observation.maxLastModified> <Observation.accMetaChecksum>  (version 2.3+)
        List<ObservationState> list = new ArrayList<>();

        String id = null;
        String sdate;
        Date date = null;
        String collection = null;
        String md5;
        String aux = "";

        boolean readingDate = false;
        boolean readingCollection = true;
        boolean readingId = false;

        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);

            if (c != ' ' && c != getSeparator() && c != getEndOfLine()) {
                aux += c;
            } else if (c == getSeparator()) {
                if (readingCollection) {
                    collection = aux;
                    // log.debug("*************** collection: " + collection);
                    readingCollection = false;
                    readingId = true;
                    readingDate = false;
                    aux = "";
                } else if (readingId) {
                    id = aux;
                    // log.debug("*************** id: " + id);
                    readingCollection = false;
                    readingId = false;
                    readingDate = true;
                    aux = "";
                } else if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }

            } else if (c == ' ') {
                if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());

                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }
            } else if (c == getEndOfLine()) {
                if (id == null || collection == null) {
                    continue;
                }

                ObservationState os = new ObservationState(new ObservationURI(collection, id));

                if (date == null) {
                    sdate = aux;
                    date = DateUtil.flexToDate(sdate, getDateFormat());
                }

                os.maxLastModified = date;

                md5 = aux;
                aux = "";
                // log.debug("*************** md5: " + md5);
                if (!md5.equals("")) {
                    os.accMetaChecksum = new URI(md5);
                }

                list.add(os);
                readingCollection = true;
                readingId = false;
            }
        }
        Collections.sort(list, getComparator());
        return list;
    }

    @Override
    public List read(String in) {
        return null;
    }

    @Override
    public List read(Reader in) {
        return null;
    }
}
