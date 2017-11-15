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

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkip;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.date.DateUtil;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * Harvest a single type of entity in order of lastModified date.
 *
 * @author pdowler
 */
public class ReadAccessHarvester extends Harvester {

    private static Logger log = Logger.getLogger(ReadAccessHarvester.class);

    private ReadAccessDAO srcAccessDAO;
    private ReadAccessDAO destAccessDAO;

    private boolean skipped;
    private Date maxDate;

    protected HarvestSkipDAO harvestSkip;

    /**
     * Harvest ReadAccess tuples.
     *
     * @param entityClass
     * the type of entity to harvest
     * @param src
     * source server.database.schema
     * @param dest
     * destination server.database.schema
     * @param batchSize
     * ignored, always full list
     * @param full
     * ignored, always in lastModfied order
     * @param dryrun
     * do not actually update database if true (normally false)
     * @throws IOException
     * IOException
     */
    public ReadAccessHarvester(Class entityClass, HarvestResource src, HarvestResource dest,
            Integer batchSize, boolean full, boolean dryrun) throws IOException {
        super(entityClass, src, dest, batchSize, full, dryrun);
        init();
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    /**
     * initialize the harvester
     *
     * @throws IOException
     */
    private void init() throws IOException {
        Map<String, Object> config1 = getConfigDAO(src);
        this.srcAccessDAO = new ReadAccessDAO();
        srcAccessDAO.setConfig(config1);

        this.destAccessDAO = new ReadAccessDAO();

        Map<String, Object> config2 = getConfigDAO(dest);
        // required to update asset tables when assets change but identical
        // tuples are generated
        config2.put("forceUpdate", Boolean.TRUE);
        destAccessDAO.setConfig(config2);
        destAccessDAO.setOrigin(false); // copy as-is

        initHarvestState(destAccessDAO.getDataSource(), entityClass);
        this.harvestSkip = new HarvestSkipDAO(destAccessDAO.getDataSource(), dest.getDatabase(), dest.getSchema(), batchSize);
    }

    /**
     * cleanup connections and state
     *
     * @throws IOException
     */
    private void close() throws IOException {
        // TODO
    }

    /**
     * run
     */
    @Override
    public void run() {
        log.info("START: " + entityClass.getSimpleName());
        boolean go = true;
        while (go) {
            Progress num = doit();
            if (num.found > 0) {
                log.info("finished batch: " + num);
            }
            if (!skipped && num.failed > num.found / 2) {
                log.warn("failure rate is quite high: " + num.failed + "/" + num.found);
                num.abort = true;
            }
            if (num.abort) {
                log.error("batched aborted");
            }
            go = (num.found > 0 && !num.abort && !num.done);
            full = false; // do not start at min(lastModified) again
            if (dryrun) {
                go = false;
            }
            // go = false;// single loop for testing
        }
        try {
            close();
        } catch (Throwable oops) {
            log.error("failed to cleanup connections and state", oops);
            return;
        }
        log.info("DONE: " + entityClass.getSimpleName() + "\n");
    }

    /**
     * class that does the harvester work
     *
     */
    private static class Progress {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int ingested = 0;
        int failed = 0;

        @Override
        public String toString() {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private Date startDate;

    /**
     * Does the harvester work
     *
     * @return
     */
    private Progress doit() {
        log.info("batch: " + entityClass.getSimpleName());
        Progress ret = new Progress();

        int expectedNum = Integer.MAX_VALUE;
        if (batchSize != null) {
            expectedNum = batchSize.intValue();
        }

        try {
            HarvestState state = null;
            if (!skipped) {
                state = harvestStateDAO.get(source, cname);
                log.info("last harvest: " + format(state.curLastModified));
            }

            if (full) {
                startDate = null;
            } else if (!skipped) {
                startDate = state.curLastModified;
            }
            // else: skipped: keep startDate across multiple batches since we
            // don't persist harvest
            // state

            Date end = maxDate;
            List<SkippedWrapper<ReadAccess>> entityList = null;
            if (skipped) {
                entityList = getSkipped(startDate);
            } else {
                Date fiveMinAgo = new Date(System.currentTimeMillis() - 5 * 60000L); // 5
                // minutes
                // ago;
                if (end == null) {
                    end = fiveMinAgo;
                } else {
                    log.info("harvest limit: min( " + format(fiveMinAgo) + " " + format(end) + " )");
                    if (end.getTime() > fiveMinAgo.getTime()) {
                        end = fiveMinAgo;
                    }
                }

                List<ReadAccess> tmp = srcAccessDAO.getList(entityClass, startDate, end, batchSize);
                entityList = wrap(tmp);
            }

            if (entityList.size() >= expectedNum) {
                detectLoop(entityList);
            }

            ret.found = entityList.size();
            log.info("found: " + entityList.size());

            ListIterator<SkippedWrapper<ReadAccess>> iter = entityList.listIterator();
            while (iter.hasNext()) {
                SkippedWrapper<ReadAccess> sra = iter.next();
                ReadAccess ra = sra.entity;
                HarvestSkip hs = sra.skip;

                iter.remove(); // allow garbage collection asap

                if (!dryrun) {
                    destAccessDAO.getTransactionManager().startTransaction();
                }
                boolean ok = false;
                try {
                    if (ra != null) {
                        log.info("put: " + ra.getClass().getSimpleName() + " " + ra.getAssetID() + "/" + ra.getGroupID() + " " + format(ra.getLastModified()));
                    }
                    if (!dryrun) {
                        if (ra != null) {
                            if (skipped) {
                                startDate = hs.lastModified;
                            }

                            if (state != null) {
                                state.curLastModified = ra.getLastModified();
                                state.curID = ra.getID();
                            }

                            destAccessDAO.put(ra);

                            if (hs != null) {
                                log.info("delete: " + hs + " " + format(hs.lastModified));
                                harvestSkip.delete(hs);
                            } else {
                                harvestStateDAO.put(state);
                            }
                        } else if (skipped) {
                            log.info("delete: " + hs + " " + format(hs.lastModified));
                            harvestSkip.delete(hs);
                        }
                        log.debug("committing transaction");
                        destAccessDAO.getTransactionManager().commitTransaction();
                        log.debug("commit: OK");
                    }
                    ok = true;
                    ret.ingested++;
                } catch (Throwable t) {
                    if (t instanceof DataIntegrityViolationException && t.getMessage().contains("failed to update")) {
                        log.error(t.getMessage());
                    } else {
                        log.error("BUG - failed to put ReadAccess", t);
                        ret.abort = true;
                    }
                } finally {
                    if (!ok && !dryrun) {
                        log.warn("failed to process " + ra + ": trying to rollback the transaction");
                        destAccessDAO.getTransactionManager().rollbackTransaction();
                        log.warn("rollback: OK");

                        // track failures where possible
                        if (!skipped) {
                            try {
                                log.debug("starting harvestSkip transaction");
                                HarvestSkip skip = harvestSkip.get(source, cname, ra.getID());
                                if (skip == null) {
                                    skip = new HarvestSkip(source, cname, ra.getID(), null);
                                }
                                destAccessDAO.getTransactionManager().startTransaction();
                                log.info("skip: " + skip);

                                // track the harvest state progress
                                harvestStateDAO.put(state);
                                // track the fail
                                harvestSkip.put(skip);
                                // delete previous version of entity
                                destAccessDAO.delete(ra.getClass(), ra.getID());

                                log.debug("committing harvestSkip transaction");
                                destAccessDAO.getTransactionManager().commitTransaction();
                                log.debug("commit harvestSkip: OK");
                            } catch (Throwable oops) {
                                log.warn("failed to insert via HarvestSkip", oops);
                                destAccessDAO.getTransactionManager().rollbackTransaction();
                                log.warn("rollback harvestSkip: OK");
                            }
                        }
                        ret.failed++;
                    }
                }
            }
            if (ret.found < expectedNum) {
                ret.done = true;
                if (state != null && state.curLastModified != null && ret.found > 0) {
                    // tweak HarvestState so we don't keep picking up the same
                    // batch
                    Date n = new Date(state.curLastModified.getTime() + 1L); // 1
                    // ms
                    // ahead
                    Date now = new Date();
                    if (now.getTime() - n.getTime() > 600 * 1000L) {
                        n = new Date(state.curLastModified.getTime() + 100L);
                    }
                    // ahead
                    state.curLastModified = n;
                    log.info("reached last " + entityClass.getSimpleName() + ": setting curLastModified to " + format(state.curLastModified));
                    harvestStateDAO.put(state);
                }
            }
        } finally {
            log.debug("DONE");
        }
        return ret;
    }

    private void detectLoop(List<SkippedWrapper<ReadAccess>> entityList) {
        if (entityList.size() < 2) {
            return;
        }
        SkippedWrapper<ReadAccess> start = entityList.get(0);
        SkippedWrapper<ReadAccess> end = entityList.get(entityList.size() - 1);
        if (start.entity.getLastModified().equals(end.entity.getLastModified())) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.ISO8601_DATE_FORMAT_MSZ, DateUtil.UTC);
            throw new RuntimeException("detected infinite harvesting loop: " + entityClass.getSimpleName()
                    + " at " + df.format(start.entity.getLastModified()));
        }
    }

    private List<SkippedWrapper<ReadAccess>> wrap(List<ReadAccess> obsList) {
        List<SkippedWrapper<ReadAccess>> ret = new ArrayList<SkippedWrapper<ReadAccess>>(obsList.size());
        for (ReadAccess o : obsList) {
            ret.add(new SkippedWrapper<ReadAccess>(o, null));
        }
        return ret;
    }

    private List<SkippedWrapper<ReadAccess>> getSkipped(Date start) {

        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]");
        int found = 0;
        int notFound = 0;
        List<HarvestSkip> skip = harvestSkip.get(source, cname, start, null);
        List<SkippedWrapper<ReadAccess>> ret = new ArrayList<SkippedWrapper<ReadAccess>>(skip.size());
        for (HarvestSkip hs : skip) {
            ReadAccess o = srcAccessDAO.get(entityClass, hs.getSkipID());
            if (o == null) {
                notFound++;
            } else {
                found++;
            }
            ret.add(new SkippedWrapper<ReadAccess>(o, hs));
        }
        log.info("getSkipped found: " + found + " not found: " + notFound);
        return ret;
    }
}
