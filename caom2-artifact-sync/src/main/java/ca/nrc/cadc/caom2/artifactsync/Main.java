package ca.nrc.cadc.caom2.artifactsync;

import java.net.URI;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.repo.client.RepoClient;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

/**
 *
 * @author majorb
 */
public class Main
{

    private static Logger log = Logger.getLogger(Main.class);
    private static int exitValue = 0;

    public static void main(String[] args)
    {
        try
        {
            ArgumentMap am = new ArgumentMap(args);

            if (am.isSet("d") || am.isSet("debug"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.artifactsync", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
            }
            else if (am.isSet("v") || am.isSet("verbose"))
            {
                Log4jInit.setLevel("ca.nrc.cadc.caom.artifactsync", Level.INFO);
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

            boolean dryrun = am.isSet("dryrun");

            // setup optional authentication for harvesting from a web service
            Subject subject = null;
            if (am.isSet("netrc"))
            {
                subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            }
            else if (am.isSet("cert"))
            {
                subject = CertCmdArgUtil.initSubject(am);
            }
            if (subject != null)
            {
                AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
                log.info("authentication using: " + meth);
            }

            String sresourceId = am.getValue("resourceID");
            String scollection = am.getValue("collection");

            if (scollection == null)
            {
                log.error("--collection is required");
                System.exit(-1);
            }

            int nthreads = 1;
            try
            {
                nthreads = Integer.parseInt(am.getValue("threads"));
            }
            catch (NumberFormatException nfe)
            {
                log.error("Illegal value for --threads: " + am.getValue("threads"));
                System.exit(-1);
            }

            boolean detectMissing = am.isSet("detect-missing");
            boolean importMissing = am.isSet("import-missing");
            if ((detectMissing && importMissing) || (!detectMissing && !importMissing))
            {
                log.error("Must specify either --detect-missing or --import-missing");
                System.exit(-1);
            }

            exitValue = 2; // in case we get killed
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));

            ArtifactStore artifactStore = null;
            try
            {
                Class<?> asClass = Class.forName(ArtifactStore.class.getName() + "Impl");
                artifactStore = (ArtifactStore) asClass.newInstance();
            }
            catch (Exception e)
            {
                log.error("Failed to load " + ArtifactStore.class.getName() + "Impl", e);
                System.exit(-1);
            }

            Runnable worker = null;
            if (detectMissing)
            {
                if (sresourceId == null)
                {
                    log.error("--resourceID is required");
                }

                URI uresourceID = URI.create(sresourceId);
                RepoClient caom2client = new RepoClient(uresourceID, 1);
                worker = new DetectMissingArtifacts(caom2client, artifactStore, scollection, dryrun);
            }
            else
            {
                worker = new ImportMissingArtifacts(artifactStore, scollection, dryrun, nthreads);
            }

            if (subject != null)
            {
                Subject.doAs(subject, new RunnableAction(worker));
            }
            else // anon
            {
                worker.run();
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
        sb.append("\n\nusage: caom2-artifact-sync [-v|--verbose|-d|--debug] [-h|--help] ...");
        sb.append("\n           <--detect-missing | --import-missing>");
        sb.append("\n           --resourceID= to pick the caom2repo service (e.g. ivo://cadc.nrc.ca/caom2repo)");
        sb.append("\n           --collection= collection to be retrieve. (e.g. IRIS)");
        sb.append("\n           --threads= number  of threads to be used to import artifacts (default: 1)");
        sb.append("\n\nOptional authentication:");
        sb.append("\n     [--netrc|--cert=<pem file>]");
        sb.append("\n     --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n     --cert=<pem file> : read client certificate from PEM file");
        sb.append("\n     --dryrun : check for work but don't do anything");
        log.warn(sb.toString());
    }
}