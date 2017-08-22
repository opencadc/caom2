package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.caom2.persistence.SybaseSQLGenerator;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;

/**
 *
 * @author pdowler
 */
public abstract class Harvester implements Runnable
{

    private static Logger log = Logger.getLogger(Harvester.class);

    public static final String POSTGRESQL = "postgresql";
    public static final String SYBASE = "sybase";
    public static final String JTDS = "jtds";

    protected boolean dryrun;

    protected String source;
    protected String cname;

    protected Class entityClass;
    protected Integer batchSize;
    protected boolean full;

    protected HarvestResource src;
    protected HarvestResource dest;
    protected HarvestStateDAO harvestState;

    protected Harvester()
    {
    }

    protected Harvester(Class entityClass, HarvestResource src, HarvestResource dest, Integer batchSize, boolean full, boolean dryrun) throws IOException
    {
        this.entityClass = entityClass;
        this.src = src;
        this.dest = dest;
        this.batchSize = batchSize;
        this.full = full;
        this.dryrun = dryrun;
    }

    protected Map<String, Object> getConfigDAO(HarvestResource desc) throws IOException
    {
        if (desc.getDatabaseServer() == null)
            throw new RuntimeException("BUG: getConfigDAO called with ObservationResource[service]");
        
        Map<String, Object> ret = new HashMap<String, Object>();

        // HACK: detect RDBMS backend from JDBC driver
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(desc.getDatabaseServer(), desc.getDatabase());
        String driver = cc.getDriver();
        if (driver == null)
            throw new RuntimeException("failed to find JDBC driver for " + desc.getDatabaseServer() + " " + desc.getDatabase());

        if (driver.contains(SYBASE) || driver.contains(JTDS))
            ret.put(SQLGenerator.class.getName(), SybaseSQLGenerator.class);
        else if (cc.getDriver().contains(POSTGRESQL))
        {
            ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            ret.put("disableHashJoin", Boolean.TRUE);
        }
        else
            throw new IllegalArgumentException("unknown SQL dialect: " + desc.getDatabaseServer());

        ret.put("server", desc.getDatabaseServer());
        ret.put("database", desc.getDatabase());
        ret.put("schema", desc.getSchema());
        return ret;
    }

    /**
     * @param ds
     *            DataSource from the destination DAO class
     * @param c
     *            class being persisted via the destination DAO class
     */
    protected void initHarvestState(DataSource ds, Class c)
    {
        this.cname = c.getSimpleName();

        log.debug("creating HarvestState tracker: " + cname + " in " + dest.getDatabase() + "." + dest.getSchema());
        this.harvestState = new PostgresqlHarvestStateDAO(ds, dest.getDatabase(), dest.getSchema());

        log.debug("creating HarvestSkip tracker: " + cname + " in " + dest.getDatabase() + "." + dest.getSchema());

        this.source = src.getIdentifier();
    }

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    protected String format(Date d)
    {
        if (d == null)
            return "null";
        return df.format(d);
    }

}