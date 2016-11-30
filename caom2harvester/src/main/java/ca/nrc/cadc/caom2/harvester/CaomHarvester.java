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

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.wcs.Transform;
import java.io.IOException;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 * A wrapper that calls the Harvester implementations in the right order.
 * 
 * @author pdowler
 */
public class CaomHarvester implements Runnable
{
    private static Logger log = Logger.getLogger(CaomHarvester.class);

    private ObservationHarvester obsHarvester;
    private DeletionHarvester obsDeleter;

    private ReadAccessHarvester observationMetaHarvester;
    private ReadAccessHarvester planeDataHarvester;
    private ReadAccessHarvester planeMetaHarvester;
    
    private DeletionHarvester observationMetaDeleter;
    private DeletionHarvester planeDataDeleter;
    private DeletionHarvester planeMetaDeleter;
    
    private boolean init;
    
    private CaomHarvester() { }

    /**
     * Harvest everything.
     *
     * @param dryrun
     * @param src source server,database,schema
     * @param dest destination server,database,schema
     * @param batchSize number of observations per batch (~memory consumption)
     * @param batchFactor multiplier for batchSize when harvesting single-table entities
     * @param full full harvest of all source entities
     * @param skip
     * @param maxDate
     * @throws java.io.IOException
     */
    public CaomHarvester(boolean dryrun, String[] src, String[] dest, int batchSize, int batchFactor,
        boolean full, boolean skip, Date maxDate)
        throws IOException
    {
        Integer entityBatchSize = batchSize*batchFactor;
        
        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setSkipped(skip);
        obsHarvester.setMaxDate(maxDate);

        this.observationMetaHarvester = new ReadAccessHarvester(ObservationMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
        observationMetaHarvester.setSkipped(skip);
        this.planeDataHarvester = new ReadAccessHarvester(PlaneDataReadAccess.class, src, dest, entityBatchSize, full, dryrun);
        planeDataHarvester.setSkipped(skip);
        this.planeMetaHarvester = new ReadAccessHarvester(PlaneMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
        planeMetaHarvester.setSkipped(skip);
        
        if (!full)
        {
            this.obsDeleter = new DeletionHarvester(DeletedObservation.class, src, dest, entityBatchSize, dryrun);
            
            if (!skip)
            {
                this.observationMetaDeleter = new DeletionHarvester(DeletedObservationMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                this.planeMetaDeleter = new DeletionHarvester(DeletedPlaneMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                this.planeDataDeleter = new DeletionHarvester(DeletedPlaneDataReadAccess.class, src, dest, entityBatchSize, dryrun);
            }
        }
    }
    
    public CaomHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize, boolean full, Date maxDate)
        throws IOException
    {
        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
        obsHarvester.setDoCollisionCheck(true);
    }

    public void setInitHarvesters(boolean init)
    {
        this.init = init;
    }

    
    public static CaomHarvester getTestHarvester(boolean dryrun, String[] src, String[] dest, 
            Integer batchSize, Integer batchFactor, boolean full, boolean skip, Date maxdate)
        throws IOException
    {
        CaomHarvester ret = new CaomHarvester(dryrun, src, dest, batchSize, batchFactor, full, skip, maxdate);
        
        ret.obsHarvester = null;
        ret.obsDeleter = null;
        
        ret.observationMetaHarvester = null;
        ret.planeMetaHarvester = null;
        ret.planeDataHarvester = null;
        
        ret.observationMetaDeleter = null;
        ret.planeMetaDeleter = null;
        ret.planeDataDeleter = null;

        return ret;
    }
    
    public void run()
    {
        // make sure wcslib can be loaded
        try { Class.forName("ca.nrc.cadc.wcs.WCSLib"); }
        catch(Throwable t)
        {
            throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
        }

        // delete observations before harvest to avoid observationURI conflicts 
        // from delete+create
        if (obsDeleter != null)
        {
            obsDeleter.setInitHarvestState(init);
            obsDeleter.run();
        }
        if (obsHarvester != null)
        {
            obsHarvester.setInitHarvest(init);
            obsHarvester.run();
        }
        
        // clean up old access control tuples before harvest to avoid conflicts
        // from delete+create
        if (observationMetaDeleter != null)
        {
            observationMetaDeleter.setInitHarvestState(init);
            observationMetaDeleter.run();
        }
        if (planeDataDeleter != null)
        {
            planeDataDeleter.setInitHarvestState(init);
            planeDataDeleter.run();
        }
        if (planeMetaDeleter != null)
        {
            planeMetaDeleter.setInitHarvestState(init);
            planeMetaDeleter.run();
        }
        
        if (init)
            return; // no point in trying to harvest a batch of ReadAccess tuples
        
        // make sure access control tuples are harvested after observations
        // because they update asset tables and fail if asset is missing
        if (observationMetaHarvester != null)
        {
            observationMetaHarvester.run();
        }
        if (planeDataHarvester != null)
        {
            planeDataHarvester.run();
        }
        if (planeMetaHarvester != null)
        {
            planeMetaHarvester.run();
        }
        
        
    }
}
