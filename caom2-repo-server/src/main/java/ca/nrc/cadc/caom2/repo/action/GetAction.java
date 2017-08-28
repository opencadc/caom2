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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.repo.action;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import ca.nrc.cadc.caom2.repo.CaomRepoConfig;
import org.apache.log4j.Logger;

import com.csvreader.CsvWriter;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;

/**
 *
 * @author pdowler
 */
public class GetAction extends RepoAction
{

    private static final Logger log = Logger.getLogger(GetAction.class);

    protected DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    public static final String CAOM_MIMETYPE = "text/x-caom+xml";

    public static final int MAX_OBS_LIST_SIZE = 100000;

    public GetAction()
    {
    }

    @Override
    public void doAction() throws Exception
    {
        log.debug("GET ACTION");
        ObservationURI uri = getURI();
        if (uri != null)
        {
            doGetObservation(uri);
            return;
        }
        else if (getCollection() != null)
        {
            // maxRec == null means list all
            String maxRecString = syncInput.getParameter("maxrec");
            Integer maxRec = MAX_OBS_LIST_SIZE;
            if (maxRecString != null)
            {
                int m = Integer.valueOf(maxRecString);
                if (m < maxRec)
                    maxRec = m;
            }

            try
            {
                // start date is optional
                Date start = null;
                String startString = syncInput.getParameter("start");
                if (startString != null)
                    start = df.parse(startString);

                // end date is optional
                Date end = null;
                String endString = syncInput.getParameter("end");
                if (endString != null)
                    end = df.parse(endString);

                doList(maxRec, start, end);
            }
            catch (ParseException e)
            {
                throw new IllegalArgumentException("wrong date format", e);
            }
        }
        else
        {
            // Responds to requests where no collection is provided.
            // Returns list of all collections.
            doGetCollectionList();
        }
    }

    protected void doGetObservation(ObservationURI uri) throws Exception
    {
        log.debug("START: " + uri);

        checkReadPermission(uri.getCollection());

        ObservationDAO dao = getDAO();
        Observation obs = dao.get(uri);

        if (obs == null)
            throw new ResourceNotFoundException("not found: " + uri);

        // write with default schema
        ObservationWriter ow = getObservationWriter();

        syncOutput.setHeader("Content-Type", CAOM_MIMETYPE);
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        ow.write(obs, bc);
        logInfo.setBytes(bc.getByteCount());

        log.debug("DONE: " + uri);
    }

    protected void doList(int maxRec, Date start, Date end) throws Exception
    {
        log.debug("START: " + getCollection());

        checkReadPermission(getCollection());

        ObservationDAO dao = getDAO();

        List<ObservationState> states = dao.getObservationList(getCollection(), start, end, maxRec);

        if (states == null)
            throw new ResourceNotFoundException("Collection not found: " + getCollection());

        long byteCount = writeObservationList(states);
        logInfo.setBytes(byteCount);

        log.debug("DONE: " + getCollection());
    }

    protected ObservationWriter getObservationWriter()
    {
        return new ObservationWriter();
    }

    protected long writeObservationList(List<ObservationState> states) throws IOException
    {
        // write in tsv format
        syncOutput.setHeader("Content-Type", "text/tab-separated-values");
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        OutputStreamWriter out = new OutputStreamWriter(bc, "US-ASCII");
        CsvWriter writer = new CsvWriter(out, '\t');
        for (ObservationState state : states)
        {
            writer.write(state.getURI().getCollection());
            writer.write(state.getURI().getObservationID());
            if (state.maxLastModified != null)
            {
                writer.write(df.format(state.maxLastModified));
            }
            else
            {
                writer.write("");
            }
            if (state.accMetaChecksum != null)
            {
                writer.write(state.accMetaChecksum.toASCIIString());
            }
            else
            {
                writer.write("");
            }
            writer.endRecord();
        }
        writer.flush();
        return bc.getByteCount();
    }

    /**
     * Get list of collection names from repo configuration.
     * @throws Exception
     */
    protected void doGetCollectionList()
            throws Exception
    {
        CaomRepoConfig curConfig = getConfig();
        Iterator<String> collectionListIterator = curConfig.collectionIterator();

        syncOutput.setHeader("Content-Type", "text/tab-separated-values");
        OutputStream os = syncOutput.getOutputStream();
        ByteCountOutputStream bc = new ByteCountOutputStream(os);
        OutputStreamWriter out = new OutputStreamWriter(bc, "US-ASCII");
        CsvWriter writer = new CsvWriter(out, '\t');

        while (collectionListIterator.hasNext()) {
            String collectionName = collectionListIterator.next();

            // Write out a single column as one entry per row
            if (!collectionName.isEmpty())
            {
                writer.write(collectionName);
                writer.endRecord();
            }
        }

        writer.flush();
        logInfo.setBytes(bc.getByteCount());
        log.debug("DONE");
    }

}
