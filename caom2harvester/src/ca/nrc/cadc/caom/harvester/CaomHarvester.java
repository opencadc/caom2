
package ca.nrc.cadc.caom.harvester;

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

    private Harvester observationMetaHarvester;
    private Harvester planeDataHarvester;
    private Harvester planeMetaHarvester;
    
    private DeletionHarvester observationMetaDeleter;
    private DeletionHarvester planeDataDeleter;
    private DeletionHarvester planeMetaDeleter;
    
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
     * @orphans run harvesting and deletion of (orphaned) WCS entities
     * @throws java.io.IOException
     */
    public CaomHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize, Integer batchFactor,
        boolean full, boolean skip, Date maxDate)
        throws IOException
    {
        Integer entityBatchSize = new Integer(batchSize*batchFactor);
        
        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setSkipped(skip);
        obsHarvester.setMaxDate(maxDate);

        if (!skip) // these don't have redo-skip mode yet
        {
            this.observationMetaHarvester = new ReadAccessHarvester(ObservationMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
            this.planeDataHarvester = new ReadAccessHarvester(PlaneDataReadAccess.class, src, dest, entityBatchSize, full, dryrun);
            this.planeMetaHarvester = new ReadAccessHarvester(PlaneMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
        }
        
        if (!full)
        {
            this.obsDeleter = new DeletionHarvester(DeletedObservation.class, src, dest, entityBatchSize, dryrun);
            
            if (!skip)
            {
                this.observationMetaDeleter = new DeletionHarvester(DeletedObservationMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                this.planeDataDeleter = new DeletionHarvester(DeletedPlaneMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                this.planeMetaDeleter = new DeletionHarvester(DeletedPlaneDataReadAccess.class, src, dest, entityBatchSize, dryrun);
            }
        }
    }
    
    public CaomHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize, boolean full, Date maxDate)
        throws IOException
    {
        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
    }

    public static CaomHarvester getTestHarvester(boolean dryrun, String[] src, String[] dest, 
            Integer batchSize, Integer batchFactor, boolean full)
        throws IOException
    {
        CaomHarvester ret = new CaomHarvester(dryrun, src, dest, batchSize, batchFactor, full, false, null);
        //ret.obsHarvester.setSkipped(true);
        
        //ret.obsHarvester = null;
        
        if (true)
        {
            ret.observationMetaHarvester = null;
            ret.planeMetaHarvester = null;
            ret.planeDataHarvester = null;
        }

        if (true)
        {
            ret.obsDeleter = null;
            ret.observationMetaDeleter = null;
            ret.planeMetaDeleter = null;
            ret.planeDataDeleter = null;
        }
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
        
        // make sure access control tuples are harvested before observations
        if (observationMetaHarvester != null)
            observationMetaHarvester.run();
        if (planeDataHarvester != null)
            planeDataHarvester.run();
        if (planeMetaHarvester != null)
            planeMetaHarvester.run();
        
        // delete observations before harvest to avoid observationURI conflicts 
        // from delete+create
        if (obsDeleter != null)
            obsDeleter.run();

        if (obsHarvester != null)
            obsHarvester.run();

        // clean up old access control tuples, should not have conflict issue 
        // like observations but that is due to how accessControlDA is implemented
        if (observationMetaDeleter != null)
            observationMetaDeleter.run();
        if (planeDataDeleter != null)
            planeDataDeleter.run();
        if (planeMetaDeleter != null)
            planeMetaDeleter.run();
        
        
    }
}
