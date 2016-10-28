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

package ca.nrc.cadc.caom2.repo.action;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.csvreader.CsvWriter;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.xml.ObservationWriter;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ByteCountWriter;
import ca.nrc.cadc.net.ResourceNotFoundException;

/**
 *
 * @author pdowler
 */
public class GetAction extends RepoAction
{
    private static final Logger log = Logger.getLogger(GetAction.class);
    
    private DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    
    public static final String CAOM_MIMETYPE = "text/x-caom+xml";
    
    public static final int MAX_OBS_LIST_SIZE = 100000;

    
    public GetAction() { }

    @Override
    public void doAction() throws Exception
    {
        log.debug("GET ACTION");
        String[] cop = path.split("/");
        if (cop.length == 2)
            doGetObservation();
        else if (cop.length == 1)
        {
            // maxRec == null means list all
            String maxRecString = syncInput.getParameter("maxRec");
            Integer maxRec = null;
            if (maxRecString != null)
            {
                maxRec = Integer.valueOf(maxRecString);
            }

            // limit the size of the batch
            if ((maxRec == null) || (maxRec > MAX_OBS_LIST_SIZE))
            {
                maxRec = MAX_OBS_LIST_SIZE;
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

                doList(maxRec.intValue(), start, end);
            } catch (ParseException e)
            {
                throw new IllegalArgumentException("wrong date format", e);
            }
        }
        else
        {
        	throw new IllegalArgumentException("wrong path");
        }
    }
    
    
    protected void doGetObservation()
        throws Exception
    {
        ObservationURI uri = new ObservationURI(getURI());
        log.debug("START: " + uri);

        checkReadPermission(uri.getCollection());

        ObservationDAO dao = getDAO();
        Observation obs = dao.get(uri);

        if (obs == null)
            throw new ResourceNotFoundException("Observation not found: " + uri);

        // write with default schema
        ObservationWriter ow = new ObservationWriter();
        
        syncOutput.setHeader("Content-Type", CAOM_MIMETYPE);
        ByteCountWriter bc = new ByteCountWriter(syncOutput.getWriter());
        ow.write(obs, bc);
        logInfo.setBytes(bc.getByteCount());
        
        log.debug("DONE: " + uri);
    }
    
    
    
    protected void doList(int maxRec, Date start, Date end)
            throws Exception
        {
            log.debug("START: " + getCollection());

            checkReadPermission(getCollection());

            ObservationDAO dao = getDAO();

            List<ObservationState> states = dao.getObservationList(
                    getCollection(), start, end, maxRec);

            if (states == null)
                throw new ResourceNotFoundException(
                        "Collection not found: " + getCollection());

            // write in csv format for now        
            syncOutput.setHeader("Content-Type", "text/csv");
            ByteCountWriter bc = new ByteCountWriter(syncOutput.getWriter());
            CsvWriter writer = new CsvWriter(bc, ',');
            for (ObservationState state : states)
            {
                writer.write(state.getObservationID());
                writer.write(df.format(state.getMaxLastModified()));
                writer.endRecord();
            }
            
            logInfo.setBytes(bc.getByteCount());
            
            log.debug("DONE: " + getCollection());
        }
    
}
