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

package org.opencadc.icewind;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.date.DateUtil;
import java.sql.SQLException;
import java.text.DateFormat;
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
    private ObservationDAO destObservationDAO;
    private boolean dryrun = false;
    
    // unused
    private boolean nochecksum = false;
    
    HarvestSkipURIDAO harvestSkip = null;
    private int numMismatches = 0;
    private Date lastHarvested = null;

    public ObservationValidator(HarvestSource src, String collection, HarvestDestination dest, 
            Integer batchSize, int nthreads, boolean dryrun) {
        super(Observation.class, src, collection, dest);
        setBatchSize(batchSize);
        this.dryrun = dryrun;
        init(nthreads);
    }

    private void init(int nthreads) {
        this.srcObservationService = new RepoClient(src.getResourceID(), nthreads);

        // dest is always a database
        Map<String, Object> destConfig = getConfigDAO(dest);
        this.destObservationDAO = new ObservationDAO();
        destObservationDAO.setConfig(destConfig);
        destObservationDAO.setOrigin(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    public int getNumMismatches() {
        return numMismatches;
    }

    @Override
    public void run() {
        log.info("START VALIDATION: " + Observation.class.getSimpleName());

        Progress num = doit();
        this.numMismatches = num.found;
        if (num.found > 0) {
            log.info("finished batch: " + num);
        }

        log.info("DONE: " + Observation.class.getSimpleName() + "\n");
    }

    private static class Progress {

        int found = 0;
        int mismatch = 0;
        int known = 0;
        int added = 0;
        int missed = 0;
        int future = 0;

        @Override
        public String toString() {
            return found + " mismatches: " + mismatch + " known: " + known + " new: " + added
                    + " missed: " + missed + " future: " + future;
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

            final Set<ObservationState> srcState = new TreeSet<>(compStateUri);
            final Set<ObservationState> dstState = new TreeSet<>(compStateUri);
            
            HarvestStateDAO stateDAO = new PostgresqlHarvestStateDAO(destObservationDAO.getDataSource(), 
                    null, dest.getSchema());
            log.info("checking current harvest state: " + src.getIdentifier(collection));
            HarvestState state = stateDAO.get(src.getIdentifier(collection).toASCIIString(), Observation.class.getSimpleName());
            if (state != null) {
                this.lastHarvested = state.curLastModified;
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                log.info("current harvest state: " + df.format(lastHarvested));
            } else {
                log.info("current harvest state: " + null);
            }
            log.info("getObservationList: " + src.getIdentifier(collection));
            List<ObservationState> tmpSrcState = srcObservationService.getObservationList(collection, null, null, null);
            log.info("found: " + tmpSrcState.size() + " sorting...");
            // manually copy to allow GC
            Iterator<ObservationState> siter = tmpSrcState.iterator();
            while (siter.hasNext()) {
                ObservationState s = siter.next();
                boolean added = srcState.add(s);
                if (!added) {
                    log.warn("duplicate src entry: " + s.getURI().getURI() + " " + df.format(s.maxLastModified));
                    srcState.remove(s); // remove older one and keep newer one
                    if (!srcState.add(s)) {
                        log.warn("failed to remove+add to keep latest duplicate: " + s.getURI().getURI());
                    }    
                }
                siter.remove();
            }
            tmpSrcState = null; // was GC, now empty
            
            log.info("source set: " + srcState.size());

            log.info("getObservationList: " + dest + " " + collection);
            List<ObservationState> tmpDstState = destObservationDAO.getObservationList(collection, null, null, null);
            log.info("found: " + tmpDstState.size() + " sorting...");
            // manually copy to allow GC
            Iterator<ObservationState> diter = tmpDstState.iterator();
            while (diter.hasNext()) {
                ObservationState s = diter.next();
                dstState.add(s);
                diter.remove();
            }
            tmpDstState = null; // was GC, now empty
            log.info("destination set: " + dstState.size());

            log.info("comparing sets for discrepancies...");
            Set<ObservationStateError> errlist = calculateErroneousObservationStates(srcState, dstState);
            log.info("discrepancies found: " + errlist.size());

            timeQuery = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();
            ret.found = srcState.size();

            Iterator<ObservationStateError> iter = errlist.iterator();
            while (iter.hasNext()) {
                ObservationStateError ose = iter.next();
                iter.remove(); // allow garbage collection during loop

                try {
                    String skipMsg = ose.getError();
                    try {
                        log.debug("starting HarvestSkipURI transaction");
                        boolean knownSkip = false;
                        boolean futureSkip = false;
                        HarvestSkipURI skip = harvestSkip.get(source, cname, ose.getState().getURI().getURI());
                        Date tryAfter = ose.getState().maxLastModified;
                        if (skip == null) {
                            skip = new HarvestSkipURI(source, cname, ose.getState().getURI().getURI(), tryAfter, skipMsg);
                            ret.added++;
                        } else {
                            // avoid lastModified update for no change
                            knownSkip = true;
                            ret.known++;
                        }
                        if (!ose.missingFromSource) {
                            if (lastHarvested != null && lastHarvested.after(tryAfter) && !knownSkip) {
                                ret.missed++;
                            } else {
                                // incremental harvest mode will fix this in the future
                                futureSkip = true;
                                ret.future++;
                            }
                        }

                        if (destObservationDAO.getTransactionManager().isOpen()) {
                            throw new RuntimeException("BUG: found open transaction at start of next observation");
                        }
                        
                        if (!dryrun) {
                            log.debug("starting transaction");
                            destObservationDAO.getTransactionManager().startTransaction();
                        }

                        // track the fail
                        String emsg = skip.errorMessage;
                        if (emsg != null && emsg.length() > 32) {
                            emsg = emsg.substring(0, 32);
                        }
                        if (knownSkip) {
                            log.info("known: " + skip.getClass().getSimpleName() + "[" + skip.getSkipID() + " "
                                    + df.format(skip.getTryAfter()) + " " + emsg + "]");
                        } else if (futureSkip) {
                            log.info("future: " + skip.getClass().getSimpleName() + "[" + skip.getSkipID() + " "
                                    + df.format(skip.getTryAfter()) + " " + emsg + "]");
                        } else {
                            log.info("put: " + skip.getClass().getSimpleName() + "[" + skip.getSkipID() + " "
                                    + df.format(skip.getTryAfter()) + " " + emsg + "]");
                            if (!dryrun) {
                                harvestSkip.put(skip);
                            }
                        }
                    } catch (Throwable oops) {
                        log.warn("failed to insert HarvestSkipURI", oops);
                        if (!dryrun) {
                            destObservationDAO.getTransactionManager().rollbackTransaction();
                            log.warn("rollback HarvestSkipURI: OK");
                        }
                    }
                    if (!dryrun) {
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
                        log.error("CONTENT PROBLEM - duplicate observation: " + " " + ose.getState().getURI().getURI().toASCIIString());
                    } else if (oops instanceof UncategorizedSQLException) {
                        if (str.contains("spherepoly_from_array")) {
                            log.error("UNDETECTED illegal polygon: " + ose.getState().getURI().getURI());
                        } else {
                            log.error("unexpected exception", oops);
                        }
                    } else if (oops instanceof IllegalArgumentException
                            && str.contains("CaomValidator")
                            && str.contains("keywords")) {
                        log.error("CONTENT PROBLEM - invalid keywords: " + " " + ose.getState().getURI().getURI().toASCIIString());
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
        Set<ObservationStateError> listErroneous = new TreeSet<>(compError);
        Set<ObservationState> listCorrect = new TreeSet<>(compStateSum);
        Iterator<ObservationState> iterSrc = srcState.iterator();
        Iterator<ObservationState> iterDst = dstState.iterator();
        
        // TODO: implement a merge join of the two iterators and only accumulate the list of errors to return

        while (iterSrc.hasNext()) {
            ObservationState os = iterSrc.next();
            if (!dstState.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "missed harvest");
                log.debug("************************ adding missed harvest: " + ose.getState().getURI());
                listErroneous.add(ose);
            } else {
                listCorrect.add(os);
            }
        }
        while (iterDst.hasNext()) {
            ObservationState os = iterDst.next();
            if (!srcState.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "missed deletion");
                ose.missingFromSource = true;
                log.debug("************************ adding missed deletion: " + os.getURI());
                if (!listErroneous.contains(ose)) {
                    listErroneous.add(ose);
                }
            } else if (!nochecksum && !listCorrect.contains(os)) {
                ObservationStateError ose = new ObservationStateError(os, "mismatched accMetaChecksum (validate)");
                log.debug("************************ adding mismatched accMetaChecksum: " + os.getURI());
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
            return o1.getState().getURI().compareTo(o2.getState().getURI());
        }

    };

    private List<SkippedWrapperURI<ObservationStateError>> wrap(Set<ObservationStateError> errlist) {
        List<SkippedWrapperURI<ObservationStateError>> ret = new ArrayList<>(errlist.size());
        Iterator<ObservationStateError> iter = errlist.iterator();
        while (iter.hasNext()) {
            ObservationStateError o = iter.next();
            iter.remove(); // allow GC so we don't have to hold complete errlist and ret in memory
            ret.add(new SkippedWrapperURI<ObservationStateError>(o, null));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, @SuppressWarnings("rawtypes") Class c) {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, null, dest.getSchema());
    }
}
