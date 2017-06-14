package ca.nrc.cadc.caom2.repo.client;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.xml.ObservationParsingException;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.net.HttpDownload;

public class WorkerThread implements Callable<Observation> {

    private ObservationState state = null;
    private Subject subject = null;
    private String collection = null;
    private String BASE_HTTP_URL = null;

    public WorkerThread(ObservationState state, Subject subject, String url) {
        this.state = state;
        this.subject = subject;
        this.BASE_HTTP_URL = url;
    }

    @Override
    public Observation call() throws Exception {

        return getObservation();
    }

    public Observation getObservation() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = BASE_HTTP_URL + File.separator + state.getCollection() + File.separator
                + state.getObservationID();
        URL url = null;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to create URL object for " + surl);
        }
        HttpDownload get = new HttpDownload(url, bos);

        if (subject != null) {
            Subject.doAs(subject, new RunnableAction(get));

        } else {
            get.run();
        }

        ObservationReader obsReader = new ObservationReader();
        Observation o = null;

        try {
            o = obsReader.read(bos.toString());
        } catch (ObservationParsingException e) {
            throw new RuntimeException(
                    "Unable to create Observation object for id " + state.getObservationID());
        }
        return o;
    }

}
