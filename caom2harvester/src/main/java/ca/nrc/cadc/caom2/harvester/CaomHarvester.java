
package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.Log4jInit;

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
	private DeletionHarvester obsDeleter;

	private ReadAccessHarvester observationMetaHarvester;
	private ReadAccessHarvester planeDataHarvester;
	private ReadAccessHarvester planeMetaHarvester;

	private DeletionHarvester observationMetaDeleter;
	private DeletionHarvester planeDataDeleter;
	private DeletionHarvester planeMetaDeleter;

	private CaomHarvester()
	{
	}

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
	public CaomHarvester(boolean service, boolean dryrun, String[] src,
			String[] dest, int batchSize, int batchFactor, boolean full,
			boolean skip, Date maxDate) throws IOException, URISyntaxException
	{
		Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client.RepoClient",
				Level.DEBUG);

		Integer entityBatchSize = batchSize * batchFactor;

		DBConfig dbrc = new DBConfig();
		ConnectionConfig cc = dbrc.getConnectionConfig(dest[0], dest[1]);
		DataSource ds = DBUtil.getDataSource(cc);
		this.initdb = new InitDatabase(ds, dest[1], dest[2]);

		this.obsHarvester = new ObservationHarvester(service, src, dest,
				batchSize, full, dryrun);
		obsHarvester.setSkipped(skip);
		obsHarvester.setMaxDate(maxDate);

		this.observationMetaHarvester = new ReadAccessHarvester(service,
				ObservationMetaReadAccess.class, src, dest, entityBatchSize,
				full, dryrun);
		observationMetaHarvester.setSkipped(skip);
		this.planeDataHarvester = new ReadAccessHarvester(service,
				PlaneDataReadAccess.class, src, dest, entityBatchSize, full,
				dryrun);
		planeDataHarvester.setSkipped(skip);
		this.planeMetaHarvester = new ReadAccessHarvester(service,
				PlaneMetaReadAccess.class, src, dest, entityBatchSize, full,
				dryrun);
		planeMetaHarvester.setSkipped(skip);

		if (!full)
		{
			this.obsDeleter = new DeletionHarvester(DeletedObservation.class,
					src, dest, entityBatchSize, dryrun);

			if (!skip)
			{
				this.observationMetaDeleter = new DeletionHarvester(
						DeletedObservationMetaReadAccess.class, src, dest,
						entityBatchSize, dryrun);
				this.planeMetaDeleter = new DeletionHarvester(
						DeletedPlaneMetaReadAccess.class, src, dest,
						entityBatchSize, dryrun);
				this.planeDataDeleter = new DeletionHarvester(
						DeletedPlaneDataReadAccess.class, src, dest,
						entityBatchSize, dryrun);
			}
		}
	}

	public CaomHarvester(boolean service, boolean dryrun, String[] src,
			String[] dest, Integer batchSize, boolean full, Date maxDate)
			throws IOException, URISyntaxException
	{
		this.obsHarvester = new ObservationHarvester(service, src, dest,
				batchSize, full, dryrun);
		obsHarvester.setMaxDate(maxDate);
		obsHarvester.setDoCollisionCheck(true);
	}

	public static CaomHarvester getTestHarvester(boolean service,
			boolean dryrun, String[] src, String[] dest, Integer batchSize,
			Integer batchFactor, boolean full, boolean skip, Date maxdate)
			throws IOException, URISyntaxException
	{
		CaomHarvester ret = new CaomHarvester(service, dryrun, src, dest,
				batchSize, batchFactor, full, skip, maxdate);

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

	@Override
	public void run()
	{
		// make sure wcslib can be loaded
		try
		{
			Class.forName("ca.nrc.cadc.wcs.WCSLib");
		} catch (Throwable t)
		{
			throw new RuntimeException(
					"FATAL - failed to load WCSLib JNI binding", t);
		}

		boolean init = false;
		if (initdb != null)
		{
			boolean created = initdb.doInit();
			if (created)
				init = true; // database is empty so can bypass processing old
								// deletions
		}

		// clean up old access control tuples before harvest to avoid conflicts
		// from delete+create
		if (observationMetaDeleter != null)
		{
			boolean initDel = init;
			if (!init)
			{
				// check if we have ever harvested before
				HarvestState hs = observationMetaHarvester.harvestState.get(
						observationMetaHarvester.source,
						observationMetaHarvester.cname);
				initDel = (hs.curID == null && hs.curLastModified == null); // never
																			// harvested
																			// from
																			// source
																			// before
			}
			observationMetaDeleter.setInitHarvestState(initDel);
			observationMetaDeleter.run();
			log.info("init: " + observationMetaDeleter.cname);
		}
		if (planeDataDeleter != null)
		{
			boolean initDel = init;
			if (!init)
			{
				// check if we have ever harvested before
				HarvestState hs = planeDataHarvester.harvestState.get(
						planeDataHarvester.source, planeDataHarvester.cname);
				initDel = (hs.curID == null && hs.curLastModified == null); // never
																			// harvested
																			// from
																			// source
																			// before
			}
			planeDataDeleter.setInitHarvestState(initDel);
			planeDataDeleter.run();
			log.info("init: " + planeDataDeleter.cname);
		}
		if (planeMetaDeleter != null)
		{
			boolean initDel = init;
			if (!init)
			{
				// check if we have ever harvested before
				HarvestState hs = planeMetaHarvester.harvestState.get(
						planeMetaHarvester.source, planeMetaHarvester.cname);
				initDel = (hs.curID == null && hs.curLastModified == null); // never
																			// harvested
																			// from
																			// source
																			// before
			}
			planeMetaDeleter.setInitHarvestState(initDel);
			planeMetaDeleter.run();
			log.info("init: " + planeMetaDeleter.cname);
		}

		// delete observations before harvest to avoid observationURI conflicts
		// from delete+create
		if (obsDeleter != null)
		{
			boolean initDel = init;
			if (!init)
			{
				// check if we have ever harvested before
				HarvestState hs = obsHarvester.harvestState
						.get(obsHarvester.source, obsHarvester.cname);
				initDel = (hs.curID == null && hs.curLastModified == null); // never
																			// harvested
																			// from
																			// source
																			// before
			}
			log.info("init: " + obsDeleter.source + " " + obsDeleter.cname);
			obsDeleter.setInitHarvestState(initDel);
			obsDeleter.run();
		}

		// harvest observations
		if (obsHarvester != null)
		{
			obsHarvester.run();
		}

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
