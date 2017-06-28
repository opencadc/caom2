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

	protected String[] src;
	protected String[] dest;
	protected HarvestStateDAO harvestState;

	protected Harvester()
	{
	}

	protected Harvester(Class entityClass, String[] src, String[] dest,
			Integer batchSize, boolean full, boolean dryrun) throws IOException
	{
		this.entityClass = entityClass;
		this.src = src;
		this.dest = dest;
		this.batchSize = batchSize;
		this.full = full;
		this.dryrun = dryrun;
	}

	protected Map<String, Object> getConfigDAO(String[] desc) throws IOException
	{
		Map<String, Object> ret = new HashMap<String, Object>();

		// HACK: detect RDBMS backend from JDBC driver
		DBConfig dbrc = new DBConfig();
		ConnectionConfig cc = dbrc.getConnectionConfig(desc[0], desc[1]);
		String driver = cc.getDriver();
		if (driver == null)
			throw new RuntimeException("failed to find JDBC driver for "
					+ desc[0] + " " + desc[1]);

		if (driver.contains(SYBASE) || driver.contains(JTDS))
			ret.put(SQLGenerator.class.getName(), SybaseSQLGenerator.class);
		else if (cc.getDriver().contains(POSTGRESQL))
		{
			ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
			ret.put("disableHashJoin", Boolean.TRUE);
		} else
			throw new IllegalArgumentException(
					"unknown SQL dialect: " + desc[0]);

		ret.put("server", desc[0]);
		ret.put("database", desc[1]);
		ret.put("schema", desc[2]);
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

		log.debug("creating HarvestState tracker: " + cname + " in " + dest[1]
				+ "." + dest[2]);
		this.harvestState = new PostgresqlHarvestStateDAO(ds, dest[1], dest[2]);

		log.debug("creating HarvestSkip tracker: " + cname + " in " + dest[1]
				+ "." + dest[2]);

		if (src != null)
		{
			this.source = src[0] + "." + src[1] + "." + src[2];
		}
	}

	DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT,
			DateUtil.UTC);

	protected String format(Date d)
	{
		if (d == null)
			return "null";
		return df.format(d);
	}

}