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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

public class RepoClient extends DatabaseObservationDAO {

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private URI resourceId = null;
    private RegistryClient rc = null;
    private URL baseServiceURL = null;

    private Subject subject = null;
    private AuthMethod meth;
    private String collection = null;
    private int nthreads = 1;

    protected final String BASE_HTTP_URL;

    public RepoClient() {
        BASE_HTTP_URL = null;
    }

    // constructor takes service identifier arg
    public RepoClient(URI resourceID, String collection, int nthreads) {
        this.nthreads = nthreads;
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

    @Override
    public List<ObservationState> getObservationList(String collection, Date start, Date end,
            Integer maxrec) {
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

        List<ObservationState> list;
        try {
            list = transformByteArrayOutputStreamIntoListOfObservationState(collection, bos, df,
                    ',', '\n');
        } catch (ParseException e) {
            throw new RuntimeException("Unable to list of ObservationState from " + bos.toString());
        }
        return list;
    }

    public Iterator<Observation> observationIterator() {
        return null;

    }

    @Override
    public void setConfig(Map<String, Object> config1) {

    }

    @Override
    public List<Observation> getList(Class<Observation> c, Date startDate, Date end,
            Integer numberOfObservations, int depth) {

        List<Observation> list = new ArrayList<Observation>();

        List<ObservationState> stateList = getObservationList(collection, startDate, end,
                numberOfObservations);

        ByteArrayOutputStream bos = null;
        // Create tasks for each file
        List<Callable<Observation>> tasks = new ArrayList<Callable<Observation>>();

        for (ObservationState os : stateList) {
            tasks.add(new WorkerThread(os, subject, BASE_HTTP_URL, collection, bos));
        }

        // Run tasks in a fixed thread pool
        ExecutorService taskExecutor = Executors.newFixedThreadPool(nthreads);
        List<Future<Observation>> futures;
        try {
            futures = taskExecutor.invokeAll(tasks);
        } catch (InterruptedException e1) {
            throw new RuntimeException("Unable to create ExecutorService");
        }

        for (Future<Observation> f : futures) {
            Observation res = null;
            try {
                res = f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Unable execute thread");
            }
            if (f.isDone()) {
                list.add(res);
            }
        }
        taskExecutor.shutdown();

        List<ObservationState> erroneousObservations = checkResults(list, stateList);

        if (erroneousObservations.size() > 0) {
            String erroneous = "";
            for (ObservationState os : erroneousObservations) {
                erroneous += os.getObservationID() + " ";
            }
            throw new RuntimeException("errors reading observations: " + erroneous);
        }

        return list;
    }

    @Override
    public UUID getID(ObservationURI uri) {
        Observation observation = get(uri, null, 1);
        if (observation != null)
            return observation.getID();
        return null;
    }

    @Override
    public ObservationURI getURI(UUID id) {
        Observation observation = get(null, id, 1);
        if (observation != null)
            return observation.getURI();
        return null;
    }

    @Override
    public Observation get(UUID id) {
        if (id == null)
            throw new IllegalArgumentException("id cannot be null");
        return get(null, id, SQLGenerator.MAX_DEPTH);
    }

    @Override
    public Observation get(ObservationURI uri) {
        if (uri == null)
            throw new IllegalArgumentException("uri cannot be null");
        return get(uri, null, SQLGenerator.MAX_DEPTH);
    }

    private Observation get(ObservationURI uri, UUID id, int depth) {
        Observation o = null;
        List<Observation> list = getList(Observation.class, null, null, null, 1);
        if (id == null && uri == null) {
            throw new RuntimeException("uri and id cannot be null at the same time");
        }
        for (Observation o1 : list) {
            if (id != null) {
                if (o1.getID().equals(id)) {
                    o = o1;
                    break;
                }
            } else if (uri != null) {
                if (o1.getURI().equals(uri)) {
                    o = o1;
                    break;
                }
            }
        }
        return o;

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

    private List<ObservationState> checkResults(List<Observation> observationList,
            List<ObservationState> stateList) {
        List<ObservationState> erroneous = new ArrayList<ObservationState>();

        boolean found = false;
        for (ObservationState os : stateList) {
            found = false;
            for (Observation o : observationList) {
                if (o.getObservationID().equals(os.getObservationID())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                erroneous.add(os);
            }
        }
        return erroneous;
    }

}
