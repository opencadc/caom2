package ca.nrc.cadc.caom2.harvester.validation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;

/**
 * A wrapper that calls the Harvester implementations in the right order.
 *
 * @author pdowler
 */
public class CaomHarvester implements Runnable
{

    private static Logger log = Logger.getLogger(CaomHarvester.class);

    private InitDatabase initdb;

    private ObservationHarvester obsHarvester;
    // private DeletionHarvester obsDeleter;
    //
    // private ReadAccessHarvester observationMetaHarvester;
    // private ReadAccessHarvester planeDataHarvester;
    // private ReadAccessHarvester planeMetaHarvester;
    //
    // private DeletionHarvester observationMetaDeleter;
    // private DeletionHarvester planeDataDeleter;
    // private DeletionHarvester planeMetaDeleter;

    /**
     * Harvest everything.
     *
     * @param dryrun
     * @param src
     *            source server,database,schema
     * @param dest
     *            destination server,database,schema
     * @param batchSize
     *            number of observations per batch (~memory consumption)
     * @param batchFactor
     *            multiplier for batchSize when harvesting single-table entities
     * @param full
     *            full harvest of all source entities
     * @param skip
     * @param maxDate
     * @throws java.io.IOException
     * @throws URISyntaxException
     */
    public CaomHarvester(boolean dryrun, String[] src, String[] dest, int batchSize, int batchFactor, boolean full,
            boolean skip, Date maxDate) throws IOException, URISyntaxException
    {
        // Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest[0], dest[1]);
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest[1], dest[2]);

        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
    }

    /**
     * Harvest everything.
     *
     * @param dryrun
     * @param resourceId
     *            repo service
     * @param collection
     *            collection to be harvested
     * @param nthreads
     *            number of threads to be used to harvest
     * @param dest
     *            destination server,database,schema
     * @param batchSize
     *            number of observations per batch (~memory consumption)
     * @param batchFactor
     *            multiplier for batchSize when harvesting single-table entities
     * @param full
     *            full harvest of all source entities
     * @param skip
     * @param maxDate
     * @throws java.io.IOException
     * @throws URISyntaxException
     */
    public CaomHarvester(boolean dryrun, String resourceId, String collection, int nthreads, String[] dest,
            int batchSize, int batchFactor, boolean full, boolean skip, Date maxDate)
            throws IOException, URISyntaxException
    {
        // Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest[0], dest[1]);
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest[1], dest[2]);

        this.obsHarvester = new ObservationHarvester(resourceId, collection, nthreads, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
    }

    public CaomHarvester(boolean dryrun, String resourceId, String collection, int nthreads, String[] dest,
            Integer batchSize, boolean full, Date maxDate) throws IOException, URISyntaxException
    {
        this.obsHarvester = new ObservationHarvester(resourceId, collection, nthreads, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
        obsHarvester.setDoCollisionCheck(true);
    }

    public CaomHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize, boolean full, Date maxDate)
            throws IOException, URISyntaxException
    {
        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun);
        obsHarvester.setMaxDate(maxDate);
        obsHarvester.setDoCollisionCheck(true);
    }

    public static CaomHarvester getTestHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize,
            Integer batchFactor, boolean full, boolean skip, Date maxdate) throws IOException, URISyntaxException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void run()
    {
        // make sure wcslib can be loaded
        try
        {
            Class.forName("ca.nrc.cadc.wcs.WCSLib");
        }
        catch (Throwable t)
        {
            throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
        }

        boolean init = false;
        if (initdb != null)
        {
            boolean created = initdb.doInit();
            if (created)
                init = true; // database is empty so can bypass processing old
                             // deletions
        }

        // harvest observations
        if (obsHarvester != null)
        {
            log.info("************** obsHarvester.run() ***************");
            obsHarvester.run();
        }
    }
}