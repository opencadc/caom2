/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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

package org.opencadc.caom2.metasync;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * A wrapper that calls the Harvester implementations in the right order.
 *
 * @author pdowler
 */
public class CaomHarvester implements Runnable {
    private static final Logger log = Logger.getLogger(CaomHarvester.class);
    private static final Long DEFAULT_IDLE_TIME = 6000L;

    private final InitDatabase initdb;
    private final HarvesterResource src;
    private final HarvesterResource dest;
    private final List<String> collections;
    private final URI basePublisherID;
    private final int batchSize;
    private final int nthreads;
    private final boolean full;
    private final boolean skip;
    private final boolean nochecksum;
    private final boolean exitWhenComplete;
    private final long maxIdle;
    private Date minDate;
    private Date maxDate;
    private boolean computePlaneMetadata;
    private File readAccessConfigFile;

    /**
     * Harvest everything.
     *
     * @param src source resource
     * @param dest destination resource (must be a server/database/schema)
     * @param collections list of collections to process
     * @param basePublisherID base to use in generating Plane publisherID values in destination database
     * @param batchSize number of observations per batch (~memory consumption)
     * @param nthreads max threads when harvesting from a service
     * @param full full harvest of all source entities
     * @param skip attempt retry of all skipped observations
     * @param nochecksum disable metadata checksum comparison
     * @param exitWhenComplete exit after processing each collection if true, else continuously loop
     * @param maxIdle max sleep time in seconds between runs when running continuously
     */
    public CaomHarvester(HarvesterResource src, HarvesterResource dest, List<String> collections,
                         URI basePublisherID, int batchSize, int nthreads, boolean full, boolean skip,
                         boolean nochecksum, boolean exitWhenComplete, long maxIdle) {
        this.src = src;
        this.dest = dest;
        this.collections = collections;
        this.basePublisherID = basePublisherID;
        this.batchSize = batchSize;
        this.nthreads = nthreads;
        this.full = full;
        this.skip = skip;
        this.nochecksum = nochecksum;
        this.exitWhenComplete = exitWhenComplete;
        this.maxIdle = maxIdle;
        this.minDate = null;
        this.maxDate = null;
        this.computePlaneMetadata = false;
        this.readAccessConfigFile = null;

        ConnectionConfig cc = new ConnectionConfig(null, null, dest.getUsername(), dest.getPassword(),
                HarvesterResource.POSTGRESQL_DRIVER, dest.getJdbcUrl());
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest.getDatabase(), dest.getSchema());
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }
    
    /**
     * Enable the plane metadata compute plugin.
     * 
     * @param compute enable Plane metadata computation if true
     */
    public void setCompute(boolean compute) {
        this.computePlaneMetadata = compute;
    }
    
    /**
     * Enable the generate read access grants plugin with the specified config.
     * 
     * @param config enable read access generation from the specified config file
     */
    public void setGenerateReadAccess(String config) {
        this.readAccessConfigFile = new File(config);
    }

    @Override
    public void run() {

        if (this.computePlaneMetadata) {
            // make sure wcslib can be loaded
            try {
                log.info("loading ca.nrc.cadc.wcs.WCSLib");
                Class.forName("ca.nrc.cadc.wcs.WCSLib");
            } catch (Throwable t) {
                throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
            }
        }

        boolean init = false;
        if (initdb != null) {
            boolean created = initdb.doInit();
            if (created) {
                init = true; // database is empty so can bypass processing old
            } // deletions
        }

        long sleep = 0;
        boolean done = false;
        while (!done) {
            int ingested = 0;
            for (String collection : collections) {
                log.info("processing collection: " + collection);

                URI publisherID = URI.create(basePublisherID + collection);
                ObservationHarvester obsHarvester = new ObservationHarvester(src, dest, collection, publisherID, batchSize,
                        nthreads, full, nochecksum);
                obsHarvester.setSkipped(skip);
                obsHarvester.setComputePlaneMetadata(computePlaneMetadata);
                if (minDate != null) {
                    obsHarvester.setMaxDate(minDate);
                }
                if (maxDate != null) {
                    obsHarvester.setMaxDate(maxDate);
                }
                if (readAccessConfigFile != null) {
                    obsHarvester.setGenerateReadAccessTuples(readAccessConfigFile);
                }

                // deletions in incremental mode only
                if (!full && !skip && !src.getIdentifier(collection).equals(dest.getIdentifier(collection))) {
                    DeletionHarvester obsDeleter = new DeletionHarvester(DeletedObservation.class, src, dest,
                            collection, batchSize * 100);
                    if (minDate != null) {
                        obsDeleter.setMaxDate(minDate);
                    }
                    if (maxDate != null) {
                        obsDeleter.setMaxDate(maxDate);
                    }
                    boolean initDel = init;
                    if (!init) {
                        // check if we have ever harvested before
                        HarvestState hs = obsHarvester.harvestStateDAO.get(obsHarvester.source, obsHarvester.cname);
                        initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
                    }

                    // delete observations before harvest to avoid observationURI conflicts from delete+create
                    log.info("init: " + obsDeleter.source + " " + obsDeleter.cname);
                    obsDeleter.setInitHarvestState(initDel);
                    log.debug("************** obsDeleter.run() ****************");
                    obsDeleter.run();
                }

                // harvest observations
                log.debug("************** obsHarvester.run() ***************");
                obsHarvester.run();
                ingested += obsHarvester.getIngested();
                log.info("     source: " + src.getIdentifier(collection));
                log.info("destination: " + dest.getIdentifier(collection));
            }
            if (this.exitWhenComplete) {
                done = true;
            } else {
                if (ingested > 0 || sleep == 0) {
                    sleep = DEFAULT_IDLE_TIME;
                } else {
                    sleep = Math.min(sleep * 2, maxIdle * 1000L);
                }
                try {
                    log.debug("sleeping for " + sleep);
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread sleep interrupted", e);
                }
            }
        }
    }

}
