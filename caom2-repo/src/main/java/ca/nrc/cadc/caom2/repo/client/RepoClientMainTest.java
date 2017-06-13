package ca.nrc.cadc.caom2.repo.client;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.xml.ObservationParsingException;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClientMainTest implements Runnable {

    private static final Logger log = Logger.getLogger(RepoClient.class);

    // constructor takes service identifier arg
    public RepoClientMainTest() {

    }

    public static void main(String[] args) {

        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client.RepoClient", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.net.HttpDownload", Level.TRACE);
        Log4jInit.setLevel("ca.nrc.cadc.reg.client.RegistryClient", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.net.NetrcAuthenticator", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.net.NetrcFile", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.auth.AuthenticationUtil", Level.DEBUG);

        // Create a subject
        Subject s = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
        RepoClientMainTest rct = new RepoClientMainTest();
        if (s != null) {
            Subject.doAs(s, new RunnableAction(rct));
        }

    }

    @Override
    public void run() {
        try {
            RepoClient repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"), "IRIS");

            List<ObservationState> list = repoC.getObservationList("IRIS", null, null, 5);

            for (ObservationState os : list) {
                log.info(os.toString());
            }

            List<Observation> l = repoC.getList(Observation.class, null, null, 5);

            for (Observation o : l) {
                log.info(o.toString());
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ObservationParsingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
