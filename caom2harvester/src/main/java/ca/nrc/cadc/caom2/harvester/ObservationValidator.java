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

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.UncategorizedSQLException;

/**
 *
 * @author pdowler
 */
public class ObservationValidator extends Harvester {

    private static Logger log = Logger.getLogger(ObservationValidator.class);

    private RepoClient srcObservationService;
    private ObservationDAO srcObservationDAO;
    private ObservationDAO destObservationDAO;

    HarvestSkipURIDAO harvestSkip = null;
    private boolean nochecksum = false;

    public ObservationValidator(HarvestResource src, HarvestResource dest, Integer batchSize,
            boolean dryrun, boolean nochecksum)
            throws IOException, URISyntaxException {
        super(Observation.class, src, dest, batchSize, false, dryrun);
        this.nochecksum = nochecksum;
        init();
    }

    private void init() throws IOException, URISyntaxException {
        if (src.getResourceID() != null) {
            // 1 thread since we only use the ObservationState listing
            this.srcObservationService = new RepoClient(src.getResourceID(), 1);
        } else {
            Map<String, Object> config1 = getConfigDAO(src);
            this.srcObservationDAO = new ObservationDAO();
            srcObservationDAO.setConfig(config1);
        }

        Map<String, Object> config2 = getConfigDAO(dest);
        this.destObservationDAO = new ObservationDAO();
        destObservationDAO.setConfig(config2);
        destObservationDAO.setOrigin(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
        
    }

    @Override
    public void run() {
        log.info("START VALIDATION: " + Observation.class.getSimpleName());

        Progress num = doit();

        if (num.found > 0) {
            log.info("finished batch: " + num);
        }

        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    private static class Progress {

        int found = 0;
        int mismatch = 0;
        int known = 0;
        int added = 0;

        @Override
        public String toString() {
            return found + " mismatches: " + mismatch + " known: " + known + " new: " + added;
        }
    }

    private Progress doit() {
        Progress ret = new Progress();

        long t = System.currentTimeMillis();
        long timeState = -1;
        long timeQuery = -1;
        long timeTransaction = -1;

        try {
            System.gc(); // hint
            t = System.currentTimeMillis();

            timeState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            log.info("getObservationList: " + src.getIdentifier());
            List<ObservationState> tmpSrcState = null;
            if (srcObservationDAO != null) {
                tmpSrcState = srcObservationDAO.getObservationList(src.getCollection(), null, null, null);
            } else if (srcObservationService != null) {
                tmpSrcState = srcObservationService.getObservationList(src.getCollection(), null, null, null);
            } else {
                throw new RuntimeException("BUG: both srcObservationDAO and srcObservationService are null");
            }
            log.info("found: " + tmpSrcState.size());
            
            Set<ObservationState> srcState = new TreeSet<>(compStateUri);
            srcState.addAll(tmpSrcState);
            tmpSrcState = null; // GC
            log.info("source set: " + srcState.size());
            
            log.info("getObservationList: " + dest.getIdentifier());
            List<ObservationState> tmpDstState = destObservationDAO.getObservationList(dest.getCollection(), null, null, null);
            log.info("found: " + tmpDstState.size());
            
            Set<ObservationState> dstState = new TreeSet<>(compStateUri);
            dstState.addAll(tmpDstState);
            tmpDstState = null; // GC
            log.info("destination set: " + dstState.size());

            Set<ObservationStateError> errlist = calculateErroneousObservationStates(srcState, dstState);

            log.info("discrepancies found: " + errlist.size());

            timeQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            List<SkippedWrapperURI<ObservationStateError>> entityListSrc = wrap(errlist);

            ret.found = srcState.size();

            ListIterator<SkippedWrapperURI<ObservationStateError>> iter = entityListSrc.listIterator();
            while (iter.hasNext()) {
                SkippedWrapperURI<ObservationStateError> ow = iter.next();
                ObservationStateError o = ow.entity;
                iter.remove(); // allow garbage collection during loop

                String skipMsg = null;

                try {
                    if (!dryrun) {
                        if (o != null) {
                            skipMsg = o.toString();
                            try {
                                log.debug("starting HarvestSkipURI transaction");
                                boolean putSkip = true;
                                HarvestSkipURI skip = harvestSkip.get(source, cname, o.getObs().getURI().getURI());
                                Date tryAfter = o.getObs().maxLastModified;
                                if (skip == null) {
                                    skip = new HarvestSkipURI(source, cname, o.getObs().getURI().getURI(), tryAfter, skipMsg);
                                    ret.added++;
                                } else {
                                    putSkip = false; // avoid lastModified update for no change
                                    ret.known++;
                                }

                                if (destObservationDAO.getTransactionManager().isOpen()) {
                                    throw new RuntimeException("BUG: found open trasnaction at start of next observation");
                                }
                                log.debug("starting transaction");
                                destObservationDAO.getTransactionManager().startTransaction();

                                // track the fail
                                if (putSkip) {
                                    log.info("put: " + skip);
                                    harvestSkip.put(skip);
                                } else {
                                    log.info("known: " + skip);
                                }
                            } catch (Throwable oops) {
                                log.warn("failed to insert HarvestSkipURI", oops);
                                destObservationDAO.getTransactionManager().rollbackTransaction();
                                log.warn("rollback HarvestSkipURI: OK");
                            }
                        }

                        log.debug("committing transaction");
                        destObservationDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ret.mismatch++;
                } catch (Throwable oops) {
                    String str = oops.toString();
                    if (oops instanceof Error) {
                        log.error("FATAL - probably installation or environment", oops);
                    } else if (oops instanceof NullPointerException) {
                        log.error("BUG", oops);
                    } else if (oops instanceof BadSqlGrammarException) {
                        log.error("BUG", oops);
                        BadSqlGrammarException bad = (BadSqlGrammarException) oops;
                        SQLException sex1 = bad.getSQLException();

                        if (sex1 != null) {
                            log.error("CAUSE", sex1);
                            SQLException sex2 = sex1.getNextException();
                            log.error("NEXT CAUSE", sex2);
                        }
                    } else if (oops instanceof DataAccessResourceFailureException) {
                        log.error("SEVERE PROBLEM - probably out of space in database", oops);
                    } else if (oops instanceof DataIntegrityViolationException
                            && str.contains("duplicate key value violates unique constraint \"i_observationuri\"")) {
                        log.error("CONTENT PROBLEM - duplicate observation: " + " " + o.getObs().getURI().getURI().toASCIIString());
                    } else if (oops instanceof UncategorizedSQLException) {
                        if (str.contains("spherepoly_from_array")) {
                            log.error("UNDETECTED illegal polygon: " + o.getObs().getURI().getURI());
                        } else {
                            log.error("unexpected exception", oops);
                        }
                    } else if (oops instanceof IllegalArgumentException
                            && str.contains("CaomValidator")
                            && str.contains("keywords")) {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " " + o.getObs().getURI().getURI().toASCIIString());
                    } else {
                        log.error("unexpected exception", oops);
                    }
                }
            }
        } finally {
            timeTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + timeState + "ms");
            log.debug("time to run ObservationListQuery: " + timeQuery + "ms");
            log.debug("time to run transactions: " + timeTransaction + "ms");
        }
        return ret;
    }

    private Set<ObservationStateError> calculateErroneousObservationStates(Set<ObservationState> srcState, Set<ObservationState> dstState) {
        Set<ObservationStateError> listErroneous = new TreeSet<ObservationStateError>(compError);
        Set<ObservationState> listCorrect = new TreeSet<ObservationState>(compStateSum);
        Iterator<ObservationState> iterSrc = srcState.iterator();
        Iterator<ObservationState> iterDst = dstState.iterator();
        while (iterSrc.hasNext()) {
            ObservationState os = iterSrc.next();
            if (!dstState.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "missed harvest");
                log.debug("************************ adding missed harvest: " + ose.getObs().getURI());
                listErroneous.add(ose);
            } else {
                listCorrect.add(os);
            }
        }
        while (iterDst.hasNext()) {
            ObservationState os = iterDst.next();
            if (!srcState.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "missed deletion");
                log.debug("************************ adding missed deletion: " + os.getURI());
                if (!listErroneous.contains(ose)) {
                    listErroneous.add(ose);
                }
            } else if (!nochecksum && !listCorrect.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "mismatched accMetaChecksum");
                log.info("************************ adding mismatched accMetaChecksum: " + os.getURI());
                if (!listErroneous.contains(ose)) {
                    listErroneous.add(ose);
                }
            }
        }

        return listErroneous;
    }

    Comparator<ObservationState> compStateUri = new Comparator<ObservationState>() {
        @Override
        public int compare(ObservationState o1, ObservationState o2) {
            return o1.getURI().compareTo(o2.getURI());
        }

    };
    Comparator<ObservationState> compStateSum = new Comparator<ObservationState>() {
        @Override
        public int compare(ObservationState o1, ObservationState o2) {
            int c1 = o1.getURI().compareTo(o2.getURI());
            if (c1 != 0) {
                return c1; // different observations
            }
            if (o1.accMetaChecksum == null || o2.accMetaChecksum == null) {
                return 0; // cannot compare
            }
            return o1.accMetaChecksum.compareTo(o2.accMetaChecksum);
        }

    };

    Comparator<ObservationStateError> compError = new Comparator<ObservationStateError>() {
        @Override
        public int compare(ObservationStateError o1, ObservationStateError o2) {
            return o1.getObs().getURI().compareTo(o2.getObs().getURI());
        }

    };

    private List<SkippedWrapperURI<ObservationStateError>> wrap(Set<ObservationStateError> errlist) {
        List<SkippedWrapperURI<ObservationStateError>> ret = new ArrayList<SkippedWrapperURI<ObservationStateError>>(errlist.size());
        for (ObservationStateError o : errlist) {
            ret.add(new SkippedWrapperURI<ObservationStateError>(o, null));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, @SuppressWarnings("rawtypes") Class c) {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest.getDatabase(), dest.getSchema());
    }
}
