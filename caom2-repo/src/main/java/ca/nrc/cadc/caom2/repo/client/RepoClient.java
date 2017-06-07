package ca.nrc.cadc.caom2.repo.client;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClient {

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private URI resourceId = null;
    private RegistryClient rc = null;
    private URL serviceURL = null;
    private URLConnection urlConnection = null;
    private List<ObservationState> observationStates = new ArrayList<ObservationState>();
    private List<Observation> observations = new ArrayList<Observation>();

    // constructor takes service identifier arg
    public RepoClient(URI resourceID) {
        this.resourceId = resourceID;

        rc = new RegistryClient();

        // User RegistryClient to go from resourceID to service URL
        serviceURL = rc.getServiceURL(this.resourceId, Standards.CAOM2REPO_OBS_20, AuthMethod.CERT);

        if (serviceURL != null) {
            log.info("SERVICE URL: " + serviceURL.toString());

            // try {
            // // I am not sure if below part is needed or it is managed by HttpDownload
            // this.urlConnection = serviceURL.openConnection();
            // System.out.println("URL connection opened");
            //
            // this.urlConnection.connect();
            // System.out.println("Connected to URL");
            //
            // } catch (IOException e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // }

        } else {
            log.error("Service URL is NULL");
        }

    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end,
            Integer maxrec) {
        // Use HttpDownload to make the http GET calls (because it handles a lot of the
        // authentication stuff)

        File f = new File("/home/iguerrero/kk/test");
        HttpDownload httpD = new HttpDownload(serviceURL, f);
        httpD.run();

        // log.info("http Download run: " + f.gets);
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

    public static void main(String[] args) {

        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client.RepoClient", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.net.HttpDownload", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.reg.client.RegistryClient", Level.DEBUG);

        log.info("TEST CAOM2REPO");
        try {
            RepoClient repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"));

            repoC.getObservationList(null, null, null, null);

        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
