package ca.nrc.cadc.caom2.repo.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
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

        RepoClient repoC = null;
        try {
            repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"), "IRIS", 8);
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Unable to create RepoClient instance for URI ivo://cadc.nrc.ca/caom2repo and collection IRIS");

        }

        List<ObservationState> list = repoC.getObservationList("IRIS", null, null, 5);

        for (ObservationState os : list) {
            log.info(os.toString());
        }

        List<Observation> l = repoC.getList(Observation.class, null, null, 5, 1);

        for (Observation o : l) {
            log.info(o.getID() + " " + o.getID().getLeastSignificantBits() + o.getURI().toString());
        }

        Observation o = repoC.get(new ObservationURI("IRIS", "f005h000"));

        repoC.get(new UUID(l.get(0).getID().getMostSignificantBits(),
                l.get(0).getID().getLeastSignificantBits()));

        repoC.getID(new ObservationURI("IRIS", "f005h000"));

    }

}
