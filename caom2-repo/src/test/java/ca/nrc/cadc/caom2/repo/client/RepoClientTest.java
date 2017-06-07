package ca.nrc.cadc.caom2.repo.client;

import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;

public class RepoClientTest {

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
