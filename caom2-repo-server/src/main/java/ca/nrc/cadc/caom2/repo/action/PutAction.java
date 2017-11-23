/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.caom2.repo.ReadAccessTuplesGenerator;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.rest.InlineContentHandler;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;

/**
 *
 * @author pdowler
 */
public class PutAction extends RepoAction {
    private static final Logger log = Logger.getLogger(PutAction.class);

    public PutAction() {
    }

    @Override
    public void doAction() throws Exception {
        ObservationURI uri = getURI();
        log.debug("START: " + uri);

        checkWritePermission(uri);

        Observation obs = getInputObservation();

        if (!uri.equals(obs.getURI())) {
            throw new IllegalArgumentException("invalid input: " + uri);
        }

        ObservationDAO dao = getDAO();

        if (dao.exists(uri)) {
            throw new ResourceAlreadyExistsException("already exists: " + uri);
        }

        validate(obs);
        long transactionTime = -1;
        long t = System.currentTimeMillis();
        try {
            log.debug("starting transaction");
            dao.getTransactionManager().startTransaction();
            dao.put(obs);
            ReadAccessTuplesGenerator ratGenerator = getReadAccessTuplesGenerator(getCollection(), getReadAccessDAO(), getReadAccessGroupConfig());
            if (ratGenerator != null) {
                ratGenerator.generateTuples(obs);
            }
            
            log.debug("committing transaction");
            dao.getTransactionManager().commitTransaction();
            log.debug("commit: OK");
        } catch (DataAccessException e) {
            log.debug("failed to insert " + obs + ": ", e);
            dao.getTransactionManager().rollbackTransaction();
            log.debug("rollback: OK");
            throw e;
        } finally {
            if (dao.getTransactionManager().isOpen()) {
                log.error("BUG - open transaction in finally");
                dao.getTransactionManager().rollbackTransaction();
                log.error("rollback: OK");
            }
            
            transactionTime = System.currentTimeMillis() - t;
            log.debug("time to run transaction: " + transactionTime + "ms");
        }

        log.debug("DONE: " + uri);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new ObservationInlineContentHandler();
    }
}
