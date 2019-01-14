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

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * A wrapper that calls the Harvester implementations in the right order.
 *
 * @author pdowler
 */
public class CaomHarvester implements Runnable {
    private static Logger log = Logger.getLogger(CaomHarvester.class);

    private final InitDatabase initdb;

    private final ObservationHarvester obsHarvester;
    private DeletionHarvester obsDeleter;

    /**
     * Harvest everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the process
     * @param nochecksum
     *            disable metadata checksum comparison
     * @param src
     *            source resource
     * @param dest
     *            destination resource (must be a server/database/schema)
     * @param basePublisherID
     *            base to use in generating Plane publisherID values in destination database
     * @param batchSize
     *            number of observations per batch (~memory consumption)
     * @param full
     *            full harvest of all source entities
     * @param skip
     *            attempt retry of all skipped observations
     * @param nthreads
     *            max threads when harvesting from a service
     * 
     * @throws java.io.IOException if failing to read config information (.dbrc)
     */
    public CaomHarvester(boolean dryrun, boolean nochecksum, HarvestResource src, HarvestResource dest, URI basePublisherID, int batchSize,
            boolean full, boolean skip, int nthreads) throws IOException {
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest.getDatabaseServer(), dest.getDatabase());
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest.getDatabase(), dest.getSchema());

        this.obsHarvester = new ObservationHarvester(src, dest, basePublisherID, batchSize, full, dryrun, nochecksum, nthreads);
        obsHarvester.setSkipped(skip);
        

        // deletions in incremental mode only        
        if (!full && !skip && !src.getIdentifier().equals(dest.getIdentifier())) {
            this.obsDeleter = new DeletionHarvester(DeletedObservation.class, src, dest, batchSize * 100, dryrun);
        }

        log.info("     source: " + src.getIdentifier());
        log.info("destination: " + dest.getIdentifier());
    }

    public void setMinDate(Date d) {
        obsHarvester.setMinDate(d);
        if (obsDeleter != null) {
            obsDeleter.setMinDate(d);
        }
    }

    public void setMaxDate(Date d) {
        obsHarvester.setMaxDate(d);
        if (obsDeleter != null) {
            obsDeleter.setMaxDate(d);
        }
    }
    
    /**
     * Enable the plane metadata compute plugin.
     * 
     * @param compute enable Plane metadata computation if true
     */
    public void setCompute(boolean compute) {
        obsHarvester.setComputePlaneMetadata(compute);
    }
    
    /**
     * Enable the generate read access grants plugin with the specified config.
     * 
     * @param config enable read access generation from the specified config file
     */
    public void setGenerateReadAccess(String config) {
        if (config != null) {
            obsHarvester.setGenerateReadAccessTuples(new File(config));
        }
    }

    @Override
    public void run() {

        if (obsHarvester.getComputePlaneMetadata()) {
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

        // delete observations before harvest to avoid observationURI conflicts
        // from delete+create
        if (obsDeleter != null) {
            boolean initDel = init;
            if (!init) {
                // check if we have ever harvested before
                HarvestState hs = obsHarvester.harvestStateDAO.get(obsHarvester.source, obsHarvester.cname);
                initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
            }
            log.info("init: " + obsDeleter.source + " " + obsDeleter.cname);
            obsDeleter.setInitHarvestState(initDel);
            log.debug("************** obsDeleter.run() ****************");
            obsDeleter.run();
        }

        // harvest observations
        if (obsHarvester != null) {
            log.debug("************** obsHarvester.run() ***************");
            obsHarvester.run();
        }
    }
}
