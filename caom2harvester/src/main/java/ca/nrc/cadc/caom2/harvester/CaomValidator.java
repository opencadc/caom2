package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;

public class CaomValidator implements Runnable
{
    /**
     * log
     */
    private static Logger log = Logger.getLogger(CaomValidator.class);
    /**
     * initdb
     */
    private InitDatabase initdb;
    /**
     * obsHarvester
     */
    private ObservationValidator obsValidator;

    /**
     * Validates everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
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
     *            flag that indicates if shipped observations should be dealt
     * @param maxDate
     *            latest date to be using during harvester
     * @throws java.io.IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    public CaomValidator(boolean dryrun, String[] src, String[] dest, int batchSize, int batchFactor, boolean full,
            boolean skip, Date maxDate) throws IOException, URISyntaxException
    {
        // Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest[0], dest[1]);
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest[1], dest[2]);

        this.obsValidator = new ObservationValidator(src, dest, batchSize, full, dryrun);
        // obsValidator.setSkipped(skip);
        obsValidator.setMaxDate(maxDate);

        // this.observationMetaHarvester = new
        // ReadAccessHarvester(ObservationMetaReadAccess.class, src, dest,
        // entityBatchSize, full, dryrun);
        // observationMetaHarvester.setSkipped(skip);
        // this.planeDataHarvester = new
        // ReadAccessHarvester(PlaneDataReadAccess.class, src, dest,
        // entityBatchSize, full,
        // dryrun);
        // planeDataHarvester.setSkipped(skip);
        // this.planeMetaHarvester = new
        // ReadAccessHarvester(PlaneMetaReadAccess.class, src, dest,
        // entityBatchSize, full,
        // dryrun);
        // planeMetaHarvester.setSkipped(skip);
        //
        // if (!full)
        // {
        // this.obsDeleter = new DeletionHarvester(DeletedObservation.class,
        // src, dest, entityBatchSize, dryrun);
        //
        // if (!skip)
        // {
        // this.observationMetaDeleter = new
        // DeletionHarvester(DeletedObservationMetaReadAccess.class, src, dest,
        // entityBatchSize, dryrun);
        // this.planeMetaDeleter = new
        // DeletionHarvester(DeletedPlaneMetaReadAccess.class, src, dest,
        // entityBatchSize, dryrun);
        // this.planeDataDeleter = new
        // DeletionHarvester(DeletedPlaneDataReadAccess.class, src, dest,
        // entityBatchSize, dryrun);
        // }
        // }
    }

    /**
     * Validates everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
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
     *            flag that indicates if shipped observations should be dealt
     * @param maxDate
     *            latest date to be using during harvester
     * @throws java.io.IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    public CaomValidator(boolean dryrun, String resourceId, String collection, int nthreads, String[] dest,
            int batchSize, int batchFactor, boolean full, boolean skip, Date maxDate)
            throws IOException, URISyntaxException
    {
        // Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest[0], dest[1]);
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest[1], dest[2]);

        this.obsValidator = new ObservationValidator(resourceId, collection, nthreads, dest, batchSize, full, dryrun);
        // obsValidator.setSkipped(skip);
        obsValidator.setMaxDate(maxDate);

        // if (!full)
        // {
        // // TODO uncomment when delete service in place
        // // this.obsDeleter = new DeletionHarvester(DeletedObservation.class,
        // // resourceId, collection, nthreads, dest, entityBatchSize,
        // // dryrun);
        //
        // if (!skip)
        // {
        // // TODO uncomment when delete service in place
        // // this.observationMetaDeleter = new
        // // DeletionHarvester(DeletedObservationMetaReadAccess.class,
        // // resourceId,
        // // collection, nthreads, dest, entityBatchSize, dryrun);
        // }
        // }
    }

    /**
     * Validates everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
     * @param resourceId
     *            repo service
     * @param collection
     *            collection to be harvested
     * @param nthreads
     *            number of threads to be used to harvest
     * @param batchSize
     *            number of observations per batch (~memory consumption)
     * @param dest
     *            destination server,database,schema
     * @param full
     *            full harvest of all source entities
     * @param maxDate
     *            latest date to be used during harvester
     * @throws java.io.IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    public CaomValidator(boolean dryrun, String resourceId, String collection, int nthreads, String[] dest,
            Integer batchSize, boolean full, Date maxDate) throws IOException, URISyntaxException
    {
        this.obsValidator = new ObservationValidator(resourceId, collection, nthreads, dest, batchSize, full, dryrun);
        obsValidator.setMaxDate(maxDate);
        obsValidator.setDoCollisionCheck(true);
    }

    /**
     * Validates everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
     * @param src
     *            source server,database,schema
     * @param dest
     *            destination server,database,schema
     * @param batchSize
     *            number of observations per batch (~memory consumption)
     * @param full
     *            full harvest of all source entities
     * @param maxDate
     *            latest date to be used during harvester
     * @throws java.io.IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    public CaomValidator(boolean dryrun, String[] src, String[] dest, Integer batchSize, boolean full, Date maxDate)
            throws IOException, URISyntaxException
    {
        this.obsValidator = new ObservationValidator(src, dest, batchSize, full, dryrun);
        obsValidator.setMaxDate(maxDate);
        obsValidator.setDoCollisionCheck(true);
    }

    /**
     * Validates everything.
     *
     * @param dryrun
     *            true if no changed in the data base are applied during the
     *            process
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
     *            flag that indicates if shipped observations should be dealt
     * @param maxDate
     *            latest date to be using during harvester
     * @return UnsupportedOperationException
     * @throws java.io.IOException
     *             IOException
     * @throws URISyntaxException
     *             URISyntaxException
     */
    public static CaomValidator getTestHarvester(boolean dryrun, String[] src, String[] dest, Integer batchSize,
            Integer batchFactor, boolean full, boolean skip, Date maxDate) throws IOException, URISyntaxException
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
        if (obsValidator != null)
        {
            log.info("************** obsValidator.run() ***************");
            obsValidator.run();
        }
    }
}
