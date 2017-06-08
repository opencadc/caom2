package ca.nrc.cadc.caom2.repo.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClientTest {
	
	private static final Logger log = Logger.getLogger(RepoClient.class);

    // constructor takes service identifier arg
    public RepoClientTest(URI resourceID) {

    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end,
            Integer maxrec) {
        return null;

    }

    public Observation get(ObservationURI obs) {
        return null;

    }

    public List<Observation> getList() {
        return null;

    }

    public Iterator<Observation> observationIterator() {
        return null;

    }
    
  

}
