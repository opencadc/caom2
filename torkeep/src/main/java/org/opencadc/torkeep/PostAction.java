/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.torkeep;

import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.db.ObservationDAO;
import org.opencadc.caom2.util.ObservationState;

/**
 *
 * @author pdowler
 */
public class PostAction extends RepoAction {
    private static final Logger log = Logger.getLogger(PostAction.class);

    public PostAction() {
    }

    @Override
    public void doAction() throws Exception {
        URI uri = getObservationURI();
        log.debug("START: " + uri);

        checkWritePermission();

        Observation obs = getInputObservation();
        if (obs == null) {
            throw new IllegalArgumentException("invalid input: " + uri + " but no observation document in body");
        }

        // allow rename: same ID but change Observation.uri?
        // rationale: you can modify Plane.uri ...
        if (!uri.equals(obs.getURI())) {
            throw new IllegalArgumentException("invalid input: " + uri + " (path) must match : " + obs.getURI() + "(document)");
        }
        
        validate(obs);
        
        final URI expectedMetaChecksum;
        String condition = syncInput.getHeader("If-Match");
        if (condition != null) {
            condition = condition.trim();
            try {
                expectedMetaChecksum = new URI(condition);
            } catch (URISyntaxException ex) {
                throw new IllegalArgumentException("invalid If-Match value: " + condition, ex);
            }
        } else {
            expectedMetaChecksum = null;
        }

        ObservationDAO dao = getDAO();
        
        ObservationState s = null;
        try {
            // TODO: start txn
            // TODO: obtain lock and check vs state here
            s = dao.getState(obs.getID());
            if (s == null) {
                throw new ResourceNotFoundException("not found: observation " + obs.getID() + " aka " + obs.getURI()); 
            }

            if (expectedMetaChecksum != null) {
                if (!s.getAccMetaChecksum().equals(expectedMetaChecksum)) {
                    throw new PreconditionFailedException(obs.getURI() + " checksum " + s.getAccMetaChecksum()
                            + " does not match condition " + expectedMetaChecksum);
                }
            }

            assignPublisherID(obs);

            dao.put(obs);
            // TODO: commit txn
        } catch (PreconditionFailedException ex) {
            // TODO: rollback
            throw ex;
        } catch (ResourceNotFoundException ex) {
            // TODO: rollback
            throw ex;
        } catch (Exception ex) {
            // TODO: rollback
            throw new RuntimeException("failed to store observation " + obs.getURI(), ex);
        } finally {
            // TODO: check for open txn and rollback
        }
        
        log.debug("DONE: " + uri);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new ObservationInlineContentHandler();
    }

}
