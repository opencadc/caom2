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

import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.net.URI;
import org.apache.log4j.Logger;
import org.opencadc.caom2.DeletedObservationEvent;
import org.opencadc.caom2.db.DeletedObservationEventDAO;
import org.opencadc.caom2.db.ObservationDAO;
import org.opencadc.caom2.util.ObservationState;

/**
 *
 * @author pdowler
 */
public class DeleteAction extends RepoAction {
    private static final Logger log = Logger.getLogger(DeleteAction.class);

    public DeleteAction() {
        super();
    }

    @Override
    public void doAction() throws Exception {
        URI uri = getObservationURI();
        log.debug("START: " + uri);

        checkWritePermission();

        ObservationDAO dao = getDAO();
        ObservationState existing = dao.getState(uri);
        if (existing == null) {
            throw new ResourceNotFoundException("not found: " + uri);
        }

        DeletedObservationEventDAO doeDAO = new DeletedObservationEventDAO(dao);
        TransactionManager txnMgr = dao.getTransactionManager();
        try {
            log.debug("starting transaction");
            txnMgr.startTransaction();
            log.debug("start txn: OK");
            
            boolean locked = false;
            while (existing != null && !locked) {
                existing = dao.lock(existing.getID());
                if (existing != null) {
                    locked = true;
                } else {
                    // try again by uri, not locked
                    existing = dao.getState(uri);
                }
            }
            if (existing == null) {
                // observation deleted while trying to get a lock
                throw new ResourceNotFoundException("not found: " + uri);
            }
 
            DeletedObservationEvent doe = new DeletedObservationEvent(existing.getID(), existing.getURI());
            log.debug("delete: " + existing);
            dao.delete(existing.getID());
            log.debug("put: " + doe);
            doeDAO.put(doe);

            log.debug("committing transaction");
            txnMgr.commitTransaction();
            log.debug("commit txn: OK");
        } catch (Exception ex) {
            log.error("failed to delete " + uri, ex);
            txnMgr.rollbackTransaction();
            log.debug("rollback txn: OK");
            throw ex;
        } finally {
            if (txnMgr.isOpen()) {
                log.error("BUG - open transaction in finally");
                txnMgr.rollbackTransaction();
                log.error("rollback txn: OK");
            }
        }
        
        log.debug("DONE: " + uri);
    }
}
