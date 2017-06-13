package ca.nrc.cadc.caom2.repo.client;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

public class RepoClient {

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private URI resourceId = null;
    private RegistryClient rc = null;
    private URL baseServiceURL = null;
    private List<ObservationState> observationStates = new ArrayList<ObservationState>();
    private List<Observation> observations = new ArrayList<Observation>();
    private Subject subject = null;
    private AuthMethod meth;
    private String collection = null;

    protected final String BASE_HTTP_URL;

    public RepoClient() {
        BASE_HTTP_URL = null;
    }

    // constructor takes service identifier arg
    public RepoClient(URI resourceID, String collection) {
        this.resourceId = resourceID;

        rc = new RegistryClient();

        // setup optional authentication for harvesting from a web service
        // get current subject
        subject = AuthenticationUtil.getCurrentSubject();
        if (subject != null) {
            meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            // User RegistryClient to go from resourceID to service URL
        } else {
            meth = AuthMethod.ANON;

            log.info("No current subject found");
        }

        baseServiceURL = rc.getServiceURL(this.resourceId, Standards.CAOM2REPO_OBS_20, meth);
        BASE_HTTP_URL = baseServiceURL.toExternalForm();
        this.collection = collection;
        log.info("BASE SERVICE URL: " + baseServiceURL.toString());
        log.info("Authentication used: " + meth);

    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end,
            Integer maxrec) throws ParseException {
        // Use HttpDownload to make the http GET calls (because it handles a lot of the
        // authentication stuff)

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = BASE_HTTP_URL + File.separator + collection;
        if (maxrec != null)
            surl = surl + "?maxRec=" + maxrec;
        if (start != null)
            surl = surl + "&start=" + df.format(start);
        if (end != null)
            surl = surl + "&end=" + df.format(end);
        URL url;
        try {
            url = new URL(surl);
            HttpDownload get = new HttpDownload(url, bos);

            if (subject != null) {
                Subject.doAs(subject, new RunnableAction(get));

                log.info("Query run within subject");
            } else {
                get.run();
                log.info("Query run");
            }

        } catch (MalformedURLException e) {
            log.error("Exception in getObservationList: " + e.getMessage());
            e.printStackTrace();
        }

        return transformByteArrayOutputStreamIntoListOfObservationState(collection, bos, df, ',',
                '\n');
    }

    private List<ObservationState> transformByteArrayOutputStreamIntoListOfObservationState(
            String collection, ByteArrayOutputStream bos, DateFormat sdf, char separator,
            char endOfLine) throws ParseException {

        List<ObservationState> list = new ArrayList<ObservationState>();
        String id = null;
        String sdate = null;

        String aux = "";
        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);
            if (c != separator && c != endOfLine) {
                aux += c;
            } else if (c == separator) {
                id = aux;
                aux = "";
            } else if (c == endOfLine) {
                sdate = aux;
                aux = "";
                Date date = sdf.parse(sdate);
                ObservationState os = new ObservationState(collection, id, date, resourceId);
                list.add(os);
            }
        }
        return list;
    }

    public Observation get(ObservationURI obs) {
        return null;

    }

    public Iterator<Observation> observationIterator() {
        return null;

    }

    public void setConfig(Map<String, Object> config1) {

    }

    public List<Observation> getList(Class<Observation> c, Date startDate, Date end,
            int numberOfObservations) throws ParseException, MalformedURLException {

        List<ObservationState> stateList = getObservationList(collection, startDate, end,
                numberOfObservations);

        ByteArrayOutputStream bos = null;

        for (ObservationState os : stateList) {
            String id = os.getObservationID();

            bos = new ByteArrayOutputStream();
            String surl = BASE_HTTP_URL + File.separator + collection + File.separator + id;
            URL url;
            try {
                url = new URL(surl);
                HttpDownload get = new HttpDownload(url, bos);

                if (subject != null) {
                    Subject.doAs(subject, new RunnableAction(get));

                    log.info("Query run within subject");
                } else {
                    get.run();
                    log.info("Query run");
                }

            } catch (MalformedURLException e) {
                log.error("Exception in getList: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
            log.info("ID = " + id + " = \n" + bos.toString());
        }

        return observations;
    }

    public ObservationURI getURI(UUID curID) {
        return null;
    }

    public Observation get(UUID skipID) {
        return null;
    }

}
