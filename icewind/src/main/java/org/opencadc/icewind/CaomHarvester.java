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

package org.opencadc.icewind;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.TransientException;
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
    private static final Long DEFAULT_IDLE_TIME = 30000L;

    private final InitDatabase initdb;
    private final HarvestSource src;
    private final HarvestDestination dest;
    private final List<String> collections;
    private final URI basePublisherID;
    
    // optional
    int batchSize;
    int numThreads;
    boolean exitWhenComplete = false;
    long maxSleep;
    boolean validateMode = false;
    boolean skipMode = false;
    String retryErrorMessagePattern;
    
    // not used by main
    private boolean nochecksum;
    
    /**
     * Harvest everything.
     *
     * @param src source resource
     * @param dest destination resource (must be a server/database/schema)
     * @param collections list of collections to process
     * @param basePublisherID base to use in generating Plane publisherID values in destination database
     */
    public CaomHarvester(HarvestSource src, List<String> collections, HarvestDestination dest, URI basePublisherID) {
        this.src = src;
        this.collections = collections;
        this.dest = dest;
        this.basePublisherID = basePublisherID;
        
        ConnectionConfig cc = new ConnectionConfig(null, null, dest.getUsername(), dest.getPassword(),
                HarvestDestination.POSTGRESQL_DRIVER, dest.getJdbcUrl());
        DataSource ds = DBUtil.getDataSource(cc, true, true);
        this.initdb = new InitDatabase(ds, null, dest.getSchema());
    }

    @Override
    public void run() {

        // make sure wcslib can be loaded
        try {
            log.info("loading ca.nrc.cadc.wcs.WCSLib");
            Class.forName("ca.nrc.cadc.wcs.WCSLib");
        } catch (Throwable t) {
            throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
        }
        
        // make sure erfa can be loaded
        try {
            log.info("loading org.opencadc.erfa.ERFALib");
            Class.forName("org.opencadc.erfa.ERFALib");
        } catch (Throwable t) {
            throw new RuntimeException("FATAL - failed to load ERFALib JNI binding", t);
        }

        boolean init = false;
        if (initdb != null) {
            boolean created = initdb.doInit();
            if (created) {
                init = true; // database is empty so can bypass processing old deletions
            }
            log.info("InitDatabase: OK");
        }

        long sleep = 0;
        boolean done = false;
        while (!done) {
            int ingested = 0;
            for (String collection : collections) {
                log.info(src.getIdentifier(collection) + " -> " + dest);
                
                if (validateMode) {
                    ObservationValidator validator = new ObservationValidator(src, collection, dest, batchSize, numThreads, false);
                    ObservationHarvester obsHarvester = new ObservationHarvester(src, collection, dest, basePublisherID, 
                            batchSize, numThreads, nochecksum);
                    obsHarvester.setSkipped(skipMode, null);
                    try {
                        validator.run();
                        if (validator.getNumMismatches() > 0) {
                            obsHarvester.run(); // retry skipped
                        }
                    } catch (TransientException ex) {
                        log.warn("validate " + src.getIdentifier(collection) + " FAIL", ex);
                    }
                } else {
                    ObservationHarvester obsHarvester = new ObservationHarvester(src, collection, dest, basePublisherID, 
                            batchSize, numThreads, nochecksum);
                    obsHarvester.setSkipped(skipMode, retryErrorMessagePattern);

                    DeletionHarvester obsDeleter = new DeletionHarvester(DeletedObservation.class, src, collection, dest);
                    boolean initDel = init;
                    if (!init) {
                        // check if we have ever harvested before
                        HarvestState hs = obsHarvester.harvestStateDAO.get(obsHarvester.source, obsHarvester.cname);
                        initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
                    }

                    try {
                        // delete observations before harvest to avoid observationURI conflicts from delete+create
                        obsDeleter.setInitHarvestState(initDel);
                        obsDeleter.run();

                        // harvest observations
                        obsHarvester.run();
                        ingested += obsHarvester.getIngested();
                    } catch (TransientException ex) {
                        log.warn("harvest " + src.getIdentifier(collection) + " FAIL", ex);
                        ingested = 0;
                    }
                }
            }

            if (this.exitWhenComplete) {
                log.info("exitWhenComplete=" + exitWhenComplete + ": DONE");
                done = true;
            } else {
                if (ingested > 0 || sleep == 0) {
                    sleep = DEFAULT_IDLE_TIME;
                } else {
                    sleep = Math.min(sleep * 2, maxSleep * 1000L);
                }
                try {
                    log.info("idle sleep: " + (sleep / 1000L) + " sec");
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread sleep interrupted", e);
                }
            }
        }
    }

}
