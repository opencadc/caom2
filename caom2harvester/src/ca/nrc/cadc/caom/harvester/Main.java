
package ca.nrc.cadc.caom.harvester;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class Main 
{
    private static Logger log = Logger.getLogger(Main.class);
    
    private static final Integer DEFAULT_BATCH_SIZE = new Integer(10000);
    private static final Integer DEFAULT_BATCH_FACTOR = new Integer(10);
    
    public static void main(String[] args)
    {
        try
        {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.harvester", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
            }
            else if (am.isSet("v") || am.isSet("verbose"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.harvester", Level.INFO);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.persistence", Level.INFO);
            }
            else
            {
                Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
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
            
            if (full && skip)
            {
                usage();
                log.warn("cannot specify both --full and --skip");
            }

            String src = am.getValue("source");
            String dest = am.getValue("destination");
            boolean nosrc = (src == null || src.trim().length() == 0);
            boolean nodest = (dest == null || dest.trim().length() == 0);
            if (nosrc || nodest)
            {
                usage();
                if (nosrc)  log.warn("missing required argument: --source");
                if (nodest) log.warn("missing required argument: --destination");
                System.exit(1);
            }
            String[] srcDS = src.split("[.]");
            String[] destDS = dest.split("[.]");
            if (srcDS.length != 3 || destDS.length != 3)
            {
                usage();
                if (srcDS.length != 3)
                    log.warn("malformed --source value, found " + src +" expected: server.database.schema"
                            + " e.g. SYBASE.mydb.dbo");
                if (destDS.length != 3)
                    log.warn("malformed --destination value, found " + dest +" expected: server.database.schema"
                            + " e.g. cvodb0.cvodb.caom2");
                System.exit(1);
            }
            
            Integer batchSize = null;
            Integer batchFactor = null;
            String sbatch = am.getValue("batchSize");
            String sFactor = am.getValue("batchFactor");
            
            if (sbatch != null && sbatch.trim().length() > 0)
            {
                try { batchSize = new Integer(sbatch); }
                catch(NumberFormatException nex)
                {
                    usage();
                    log.error("value for --batchSize must be an integer, found: " + sbatch);
                    System.exit(1);
                }
            }
            if (sFactor != null && sFactor.trim().length() > 0)
            {
                try { batchFactor = new Integer(sFactor); }
                catch(NumberFormatException nex)
                {
                    usage();
                    log.error("value for --batchSize must be an integer, found: " + sbatch);
                    System.exit(1);
                }
            }
            
            if (batchSize == null)
            {
                log.warn("no --batchSize specified: defaulting to " + DEFAULT_BATCH_SIZE);
                batchSize = DEFAULT_BATCH_SIZE;
            }
            if (batchFactor == null && batchSize != null)
            {
                log.warn("no --batchFactor specified: defaulting to " + DEFAULT_BATCH_FACTOR);
                batchFactor = DEFAULT_BATCH_FACTOR;
            }
            
            CaomHarvester ch = null;
            try
            {
                if (test)
                    ch = CaomHarvester.getTestHarvester(dryrun, srcDS, destDS, batchSize, batchFactor, full);
                else
                    ch = new CaomHarvester(dryrun, srcDS, destDS, batchSize, batchFactor, full, skip);
                //ch.setInteractive(am.isSet("interactive"));
                
            }
            catch(IOException ioex)
            {
                log.error("failed to init: " + ioex.getMessage());
                System.exit(-1);
            }
            ch.run();
        }
        catch(Throwable t)
        {
            log.error("uncaught exception", t);
            System.exit(-1);
        }
        System.exit(0);
    }
    
    private static void usage()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: caomHarvester [-v|--verbose|-d|--debug]");
        sb.append("\n           --source=<server.database.schema>");
        sb.append("\n           --destination=<server.database.schema>" );
        sb.append("\n           [--full |--skip] [--batchSize=<int>]" );
        sb.append("\n\nOptions:");
        sb.append("\n     --full : restart at the first (oldest) observation (default: false)");
        sb.append("\n     --skip : redo previously skipped (failed) observations (default: false)");
        sb.append("\n\nOptional modifiers:");
        sb.append("\n     --batchSize=<number of observations per batch> (default: 10000)");
        sb.append("\n     --batchFactor=<multiplier to batchSize when getting single-table entities> (default: 10)");
        sb.append("\n     --forceUpdate : force update of destination row even if checksum says it did not change");
        sb.append("\n     --dryrun : check for work but don't do anything");
        //sb.append("\n     --interactive : pause and wait for input after each observation (observation harvesting only)");

        log.warn(sb.toString());
    }
}
