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
     * @param nochecksum
     * @param compute
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
    public CaomValidator(boolean dryrun, boolean nochecksum, boolean compute, HarvestResource src, HarvestResource dest, 
            int batchSize, int batchFactor, boolean full, boolean skip, Date maxDate)
            throws IOException, URISyntaxException
    {
        // Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest.getDatabaseServer(), dest.getDatabase());
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest.getDatabase(), dest.getSchema());

        this.obsValidator = new ObservationValidator(src, dest, batchSize, full, dryrun, nochecksum);
        obsValidator.setMaxDate(maxDate);
    }

    
    @Override
    public void run()
    {
        boolean init = false;
        if (initdb != null)
        {
            boolean created = initdb.doInit();
        }

        if (obsValidator != null)
        {
            obsValidator.run();
        }
    }
}
