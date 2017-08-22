package ca.nrc.cadc.caom2.harvester;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author pdowler
 */
public class Main
{

    private static Logger log = Logger.getLogger(Main.class);

    private static final Integer DEFAULT_BATCH_SIZE = new Integer(100);
    private static final Integer DEFAULT_BATCH_FACTOR = new Integer(2500);
    private static int exitValue = 0;

    public static void main(String[] args)
    {
        try
        {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.harvester", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
            }
            else if (am.isSet("v") || am.isSet("verbose"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.harvester", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.INFO);
            }
            else
            {
                Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.WARN);

            }

            if (am.isSet("h") || am.isSet("help"))
            {
                usage();
                System.exit(0);
            }

            boolean test = am.isSet("test");
            boolean full = am.isSet("full");
            boolean skip = am.isSet("skip");
            boolean dryrun = am.isSet("dryrun");
            boolean validate = am.isSet("validate");
            boolean noChecksum = am.isSet("nochecksum");;

            boolean compute = am.isSet("compute");

            // setup optional authentication for harvesting from a web service
            Subject subject = AuthenticationUtil.getAnonSubject();
            if (am.isSet("netrc"))
            {
                subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            }
            else if (am.isSet("cert"))
            {
                subject = CertCmdArgUtil.initSubject(am);
            }
            AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            log.info("authentication using: " + meth);

            if (full && skip)
            {
                usage();
                log.warn("cannot specify both --full and --skip");
                System.exit(1);
            }

            // required args
            String collection = am.getValue("collection");
            boolean nocol = (collection == null || collection.trim().length() == 0);
            if (nocol)
            {
                log.warn("missing required argument: --collection=<name>");
                usage();
                System.exit(1);
            }
            
            String destination = am.getValue("destination");
            boolean nodest = (destination == null || destination.trim().length() == 0);
            if (nodest)
            {
                log.warn("missing required argument: --destination");
                usage();
                System.exit(1);
            }
            String[] destDS = destination.split("[.]");
            if (destDS.length != 3)
            {
                log.warn("malformed --destination value, found " + destination + " expected: server.database.schema");
                usage();
                System.exit(1);
            }
            HarvestResource dest = new HarvestResource(destDS[0], destDS[1], destDS[2], collection);
            
            // source can be a database or service
            String source = am.getValue("source");
            String resourceID = am.getValue("resourceID");
            
            boolean nosrc = (source == null || source.trim().length() == 0);
            boolean nosrv = (resourceID == null || resourceID.trim().length() == 0);
            if (nosrc && nosrv )
            {
                log.warn("missing required argument: --source=<server.database.schema> | --resourceID=<identifier>");
                usage();
                System.exit(1);
            }
            
            HarvestResource src = null;
            int nthreads = 1;
            if (resourceID != null)
            {
                try
                {
                    src = new HarvestResource(new URI(resourceID), collection);
                    if (am.isSet("threads"))
                        nthreads = Integer.parseInt(am.getValue("threads"));
                }
                catch(URISyntaxException ex)
                {
                    log.warn("invalid value for --resourceID parameter: " + resourceID + " reason: " + ex.toString());
                    usage();
                    System.exit(1);
                }
                catch (NumberFormatException nfe)
                {
                    log.warn("invalid value for --threads parameter: " + am.getValue("threads") + " -- must be an integer" );
                    usage();
                    System.exit(1);
                }
            }
            else
            {
                String[] srcDS = source.split("[.]");
                if (srcDS.length != 3)
                {
                    log.warn("malformed --source value, found " + source + " expected: server.database.schema");
                    usage();
                    System.exit(1);
                }
                src = new HarvestResource(srcDS[0], srcDS[1], srcDS[2], collection);
            }

            Integer batchSize = null;
            Integer batchFactor = null;
            String sbatch = am.getValue("batchSize");
            String sFactor = am.getValue("batchFactor");

            if (sbatch != null && sbatch.trim().length() > 0)
            {
                try
                {
                    batchSize = new Integer(sbatch);
                }
                catch (NumberFormatException nex)
                {
                    usage();
                    log.error("value for --batchSize must be an integer, found: " + sbatch);
                    System.exit(1);
                }
            }
            if (sFactor != null && sFactor.trim().length() > 0)
            {
                try
                {
                    batchFactor = new Integer(sFactor);
                }
                catch (NumberFormatException nex)
                {
                    usage();
                    log.error("value for --batchSize must be an integer, found: " + sbatch);
                    System.exit(1);
                }
            }

            if (batchSize == null)
            {
                log.debug("no --batchSize specified: defaulting to " + DEFAULT_BATCH_SIZE);
                batchSize = DEFAULT_BATCH_SIZE;
            }
            if (batchFactor == null && batchSize != null)
            {
                log.debug("no --batchFactor specified: defaulting to " + DEFAULT_BATCH_FACTOR);
                batchFactor = DEFAULT_BATCH_FACTOR;
            }
            if (!validate)
                log.info("batchSize: " + batchSize + "  batchFactor: " + batchFactor);

            Date maxDate = null;
            String maxDateStr = am.getValue("maxDate");
            if (maxDateStr != null && maxDateStr.trim().length() > 0)
            {
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                try
                {
                    maxDate = df.parse(maxDateStr);
                }
                catch (ParseException ex)
                {
                    log.error("invalid maxDate: " + maxDateStr + " reason: " + ex);
                    usage();
                    System.exit(1);
                }
            }

            Runnable action = null;
            if (!validate)
            {

                try
                {
                    action = new CaomHarvester(dryrun, noChecksum, compute, src, dest, batchSize, batchFactor, full, skip, maxDate, nthreads);
                }
                catch (IOException ioex)
                {

                    log.error("failed to init: " + ioex.getMessage());
                    exitValue = -1;
                    System.exit(exitValue);
                }

            }
            else
            {

                try
                {
                    action = new CaomValidator(dryrun, noChecksum, compute, src, dest, batchSize, batchFactor, full, skip, maxDate);
                }
                catch (IOException ioex)
                {

                    log.error("failed to init: " + ioex.getMessage());
                    exitValue = -1;
                    System.exit(exitValue);
                }
            }
            exitValue = 2; // in case we get killed
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

            if (subject != null)
            {
                Subject.doAs(subject, new RunnableAction(action));
            }

            exitValue = 0; // finished cleanly
        }
        catch (Throwable t)
        {
            log.error("uncaught exception", t);
            exitValue = -1;
            System.exit(exitValue);
        }
        finally
        {
            System.exit(exitValue);
        }
    }

    private static class ShutdownHook implements Runnable
    {

        ShutdownHook()
        {
        }

        @Override
        public void run()
        {
            if (exitValue != 0)
                log.error("terminating with exit status " + exitValue);
        }

    }

    private static void usage()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caom2harvester [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n         --collection=<name> : name of collection to retrieve> (e.g. IRIS)");
        sb.append("\n         --destination=<server.database.schema> : persist output directly to a databsee server");
        
        sb.append("\n\nSource selection: --resourceID=<URI> [--threads=<num threads>] | --source=<server.database.schema>");
        sb.append("\n         --resourceID : harvest from a caom2 repository service (e.g. ivo://cadc.nrc.ca/caom2repo)");
        sb.append("\n         --threads : number  of threads used to read observation documents (default: 1)");
        sb.append("\n         --source : harvest directly from a database server");
                
        sb.append("\n\nOptional modes: [--validate|--skip|--full] (default: incremental harvest)");
        sb.append("\n         --validate : validate all Observation.accMetaChecksum values between source and destination ");
        sb.append("\n         --skip : redo previously skipped (failed) observations (default: false)");
        sb.append("\n         --full : restart at the first (oldest) observation (default: false)");
        
        sb.append("\n\nOptional authentication: [--netrc|--cert=<pem file>] (default: anonymous)");
        sb.append("\n         --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n         --cert=<pem file> : read client certificate from PEM file");
        
        sb.append("\n\nOptional modifiers:");
        sb.append("\n         --maxDate=<maximum Observation.maxLastModfied to consider (UTC timestamp)");
        sb.append("\n         --batchSize=<number of observations per batch> (default: ");
        sb.append(DEFAULT_BATCH_SIZE).append(")");
        sb.append("\n         --batchFactor=<multiplier to batchSize when getting single-table entities> (default: ");
        sb.append(DEFAULT_BATCH_FACTOR).append(")");
        sb.append("\n         --dryrun : check for work but don't do anything");
        sb.append("\n         --compute : compute additional Plane metadata from WCS using the caom2-compute library [deprecated]");
        sb.append("\n         --nochecksum : do not compare computed and harvested Observation.accMetaChecksum (default: require match or fail)");
        log.warn(sb.toString());
    }
}