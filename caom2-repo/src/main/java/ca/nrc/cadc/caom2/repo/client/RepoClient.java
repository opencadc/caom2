/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *  $Revision: 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo.client;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.repo.client.transform.AbstractListReader;
import ca.nrc.cadc.caom2.repo.client.transform.DeletionListReader;
import ca.nrc.cadc.caom2.repo.client.transform.ObservationStateListReader;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.CapabilitiesReader;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 * Class in charge of reading the caom2 metadata end points from a repository service.
 * This class looks for a CAOM-2.4 obs endpoint and falls back to a CAOM 2.3 endpoint 
 * if the former is not found.
 * 
 * @author jduran
 *
 */
public class RepoClient {
    private static final Logger log = Logger.getLogger(RepoClient.class);
    private static final Integer DEFAULT_BATCH_SIZE = 50000;

    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    private RegistryClient rc;
    private URI resourceID = null;
    private URL capabilitiesURL = null;

    private URL baseServiceURL = null;
    private URL baseDeletionURL = null;

    private boolean isObsAvailable = false;
    private boolean isDelAvailable = false;

    /**
     * @return the isObsAvailable
     */
    public boolean isObsAvailable() {
        return isObsAvailable;
    }

    /**
     * @return the isDelAvailable
     */
    public boolean isDelAvailable() {
        return isDelAvailable;
    }

    private int nthreads = 1;
    private Comparator<ObservationState> maxLasModifiedComparatorForState = new Comparator<ObservationState>() {
        @Override
        public int compare(ObservationState o1, ObservationState o2) {
            return o1.maxLastModified.compareTo(o2.maxLastModified);
        }
    };
    private Comparator<ObservationResponse> maxLasModifiedComparatorForResponse = new Comparator<ObservationResponse>() {
        @Override
        public int compare(ObservationResponse o1, ObservationResponse o2) {
            if (o1 == null || o2 == null || o1.observationState == null || o2.observationState == null || o1.observationState.maxLastModified == null
                    || o2.observationState.maxLastModified == null) {
                throw new NullPointerException();
            }
            return o1.observationState.maxLastModified.compareTo(o2.observationState.maxLastModified);
        }
    };

    /**
     * Create new CAOM RepoClient.
     *
     * @param resourceID
     *            the service identifier
     * @param nthreads
     *            number of threads to use when getting list of observations
     */
    public RepoClient(URI resourceID, int nthreads) {
        if (resourceID == null) {
            throw new IllegalArgumentException("resourceID cannot be null");
        }
        this.nthreads = nthreads;
        this.resourceID = resourceID;
        this.rc = new RegistryClient();
        init();
    }

    public RepoClient(URL capabilitiesURL, int nthreads) {
        if (capabilitiesURL == null) {
            throw new IllegalArgumentException("capabilitiesURL cannot be null");
        }
        this.nthreads = nthreads;
        this.capabilitiesURL = capabilitiesURL;
        init();
    }

    private void init() {
        Subject s = AuthenticationUtil.getCurrentSubject();
        AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(s);
        if (meth == null) {
            meth = AuthMethod.ANON;
        }

        Capabilities caps;
        if (resourceID != null) {
            try {
                caps = rc.getCapabilities(resourceID);
            } catch (IOException ex) {
                throw new RuntimeException("failed to read capabilities: " + resourceID, ex);
            } catch (ResourceNotFoundException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        } else {
            CapabilitiesReader capabilitiesReader = new CapabilitiesReader();
            try {
                caps = capabilitiesReader.read(capabilitiesURL.openStream());
            } catch (IOException e) {
                throw new RuntimeException("Imposible to read capabilities: " + capabilitiesURL);
            }
            
        }
        
        Capability obs = caps.findCapability(Standards.CAOM2REPO_OBS_24);
        if (obs == null) {
            obs = caps.findCapability(Standards.CAOM2REPO_OBS_23);
        }
        if (obs == null) {
            throw new RuntimeException("observation list capability not found");
        }
        Interface iface = obs.findInterface(meth);
        if (iface == null) {
            throw new RuntimeException("observation list capability does not suppoort auth: " + meth.getValue());
        }
        this.baseServiceURL = iface.getAccessURL().getURL();
        log.debug("observation list URL: " + baseServiceURL.toString());
        log.debug("AuthMethod:  " + meth);
        this.isObsAvailable = true;
        
        Capability del = caps.findCapability(Standards.CAOM2REPO_DEL_23);
        // for now: tolerate missing deletion endpoint
        //if (del == null) {
        //    throw new RuntimeException("deleted observation list capability not found");
        //}
        //Interface iface2 = del.findInterface(meth);
        //if (iface2 == null) {
        //    throw new RuntimeException("deleted observation list capability does not suppoort auth: " + meth.getValue());
        //}
        //this.baseDeletionURL = iface2.getAccessURL().getURL();
        
        if (del != null) {
            Interface iface2 = del.findInterface(meth);
            if (iface2 != null) {
                this.baseDeletionURL = iface2.getAccessURL().getURL();
            }
        }
        if (baseDeletionURL == null) {
            isDelAvailable = false;
            return;
        }
        log.debug("deletion list URL: " + baseDeletionURL.toString());
        log.debug("AuthMethod:  " + meth);
        this.isDelAvailable = true;
    }

    public List<DeletedObservation> getDeleted(String collection, Date start, Date end, Integer maxrec) {
        return readDeletedEntityList(new DeletionListReader(), collection, start, end, maxrec);
        // TODO: make call(s) to the deletion endpoint until requested number of
        // entries (like getObservationList)

        // parse each line into the following 4 values, create
        // DeletedObservation, and add to output list, eg:
        /*
         * UUID id = null; String col = null; String observationID = null; Date
         * lastModified = null; DeletedObservation de = new
         * DeletedObservation(id, new ObservationURI(col, observationID));
         * CaomUtil.assignLastModified(de, lastModified, "lastModified");
         * ret.add(de);
         */
    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end, Integer maxrec) throws AccessControlException {
        return readObservationStateList(new ObservationStateListReader(), collection, start, end, maxrec);
    }

    public List<ObservationResponse> getList(String collection, Date startDate, Date end, Integer numberOfObservations) throws InterruptedException,
            ExecutionException {

        // startDate = null;
        // end = df.parse("2017-06-20T09:03:15.360");
        List<ObservationResponse> list = new ArrayList<>();

        List<ObservationState> stateList = getObservationList(collection, startDate, end, numberOfObservations);

        // Create tasks for each file
        List<Callable<ObservationResponse>> tasks = new ArrayList<>();

        // the current subject usually gets propagated into a thread pool, but
        // gets attached
        // when the thread is created so we explicitly pass it it and do another
        // Subject.doAs in
        // case
        // thread pool management is changed
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        for (ObservationState os : stateList) {
            tasks.add(new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm()));
        }

        ExecutorService taskExecutor = null;
        try {
            // Run tasks in a fixed thread pool
            taskExecutor = Executors.newFixedThreadPool(nthreads);
            List<Future<ObservationResponse>> futures;

            futures = taskExecutor.invokeAll(tasks);

            for (Future<ObservationResponse> f : futures) {
                ObservationResponse res = null;
                res = f.get();

                if (f.isDone()) {
                    list.add(res);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
            throw e;
        } finally {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
        }

        return list;
    }

    public ObservationResponse get(ObservationURI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }

        ObservationState os = new ObservationState(uri);

        // see comment above in getList
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        Worker wt = new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm());
        return wt.getObservation();
    }

    public List<ObservationResponse> get(List<ObservationURI> listURI) throws InterruptedException, ExecutionException {
        if (listURI == null) {
            throw new IllegalArgumentException("list of uri cannot be null");
        }
        // ****************
        List<ObservationResponse> list = new ArrayList<>();
        // Create tasks for each file
        List<Callable<ObservationResponse>> tasks = new ArrayList<>();

        // want to put the result list back in same order as the input list;
        // maxLasModifiedComparatorForResponse sorts by state.maxLastModified
        // so fake it with date values that increase
        Date now = new Date();
        long i = 0;
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        for (ObservationURI uri : listURI) {
            ObservationState os = new ObservationState(uri);
            os.maxLastModified = new Date(now.getTime() + i++);
            tasks.add(new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm()));
        }
        ExecutorService taskExecutor = null;
        try {
            // Run tasks in a fixed thread pool
            taskExecutor = Executors.newFixedThreadPool(nthreads);
            List<Future<ObservationResponse>> futures;

            futures = taskExecutor.invokeAll(tasks);

            for (Future<ObservationResponse> f : futures) {
                ObservationResponse res = null;
                res = f.get();

                if (f.isDone()) {
                    list.add(res);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
            throw e;
        } finally {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
        }
        Collections.sort(list, maxLasModifiedComparatorForResponse);

        // ****************
        return list;
    }

    public ObservationResponse get(String collection, URI uri, Date start) {
        if (uri == null) {
            throw new IllegalArgumentException("uri cannot be null");
        }

        log.debug("******************* getObservationList(collection, start, null, null) " + collection);

        List<ObservationState> list = getObservationList(collection, start, null, null);
        ObservationState obsState = null;
        for (ObservationState os : list) {
            if (!os.getURI().getURI().equals(uri)) {
                continue;
            }
            obsState = os;
            break;
        }

        log.debug("******************* getting to getList " + obsState);

        if (obsState != null) {
            // see comment above in getList
            Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
            Worker wt = new Worker(obsState, subjectForWorkerThread, baseServiceURL.toExternalForm());
            return wt.getObservation();
        } else {
            return null;
        }
    }

    private List<ObservationState> readObservationStateList(ObservationStateListReader transformer, String collection, Date start, Date end, Integer maxrec) {

        List<ObservationState> accList = new ArrayList<>();
        boolean tooBigRequest = maxrec == null || maxrec > DEFAULT_BATCH_SIZE;

        Integer rec = maxrec;
        Integer recCounter;
        if (tooBigRequest) {
            rec = DEFAULT_BATCH_SIZE;
        }
        // Use HttpDownload to make the http GET calls (because it handles a lot
        // of the
        // authentication stuff)
        boolean go = true;
        String surlCommon = baseServiceURL.toExternalForm() + File.separator + collection;

        while (go) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (!tooBigRequest) {
                go = false;// only one go
            }
            String surl = surlCommon;
            surl = surl + "?maxrec=" + (rec + 1);
            if (start != null) {
                surl = surl + "&start=" + df.format(start);
            }
            if (end != null) {
                surl = surl + "&end=" + df.format(end);
            }
            URL url;
            log.debug("URL: " + surl);
            try {
                url = new URL(surl);
                HttpDownload get = new HttpDownload(url, bos);
                get.setFollowRedirects(true);

                get.run();
                int responseCode = get.getResponseCode();
                log.debug("RESPONSE CODE: '" + responseCode + "'");

                // if (responseCode == 302) {
                // // redirected url
                // url = get.getRedirectURL();
                // log.debug("REDIRECTED URL: " + url);
                // bos = new ByteArrayOutputStream();
                // get = new HttpDownload(url, bos);
                // get.run();
                // responseCode = get.getResponseCode();
                // log.debug("RESPONSE CODE (REDIRECTED URL): '" + responseCode
                // + "'");
                //
                // }

                if (get.getThrowable() != null) {
                    if (get.getThrowable() instanceof AccessControlException) {
                        throw (AccessControlException) get.getThrowable();
                    }
                    throw new RuntimeException("failed to get observation list", get.getThrowable());
                }
            } catch (MalformedURLException e) {
                throw new RuntimeException("BUG: failed to generate observation list url", e);
            }

            try {
                // log.debug("RESPONSE = '" + bos.toString() + "'");
                List<ObservationState> partialList = transformer.read(new ByteArrayInputStream(bos.toByteArray()));
                if (partialList != null && !partialList.isEmpty() && !accList.isEmpty() && accList.get(accList.size() - 1).equals(partialList.get(0))) {
                    partialList.remove(0);
                }

                if (partialList != null) {
                    accList.addAll(partialList);
                    log.debug("adding " + partialList.size() + " elements to accList. Now there are " + accList.size());
                }

                bos.close();

                if (accList.size() > 0) {
                    start = accList.get(accList.size() - 1).maxLastModified;
                }

                recCounter = accList.size();
                if (maxrec != null && maxrec - recCounter > 0 && maxrec - recCounter < rec) {
                    rec = maxrec - recCounter;
                }
                log.debug("dynamic batch (rec): " + rec);
                log.debug("counter (recCounter): " + recCounter);
                log.debug("maxrec: " + maxrec);
                // log.debug("start: " + start.toString());
                // log.debug("end: " + end.toString());

                if (partialList != null) {
                    log.debug("partialList.size(): " + partialList.size());

                    if (partialList.size() < rec || (end != null && start != null && start.equals(end))) {
                        log.debug("************** go false");

                        go = false;
                    }
                }
            } catch (ParseException | URISyntaxException | IOException e) {
                throw new RuntimeException("Unable to list of ObservationState from " + bos.toString(), e);
            }
        }
        return accList;

    }

    // pdowler: was going to use this so the HttpDownload would pass the
    // InputStream directly to the reader
    // but will take additional refactoring
    private class StreamingListReader<T> implements InputStreamWrapper {

        AbstractListReader<T> reader;
        List<T> result;
        Exception fail;

        public StreamingListReader(AbstractListReader<T> reader) {
            this.reader = reader;
        }

        @Override
        public void read(InputStream in) throws IOException {
            try {
                this.result = reader.read(in);
            } catch (ParseException ex) {
                this.fail = ex;
            } catch (URISyntaxException ex) {
                this.fail = ex;
            }
        }
    }

    private List<DeletedObservation> readDeletedEntityList(DeletionListReader transformer, String collection, Date start, Date end, Integer maxrec) {

        List<DeletedObservation> accList = new ArrayList<>();
        List<DeletedObservation> partialList = null;
        boolean tooBigRequest = maxrec == null || maxrec > DEFAULT_BATCH_SIZE;

        Integer rec = maxrec;
        Integer recCounter;
        if (tooBigRequest) {
            rec = DEFAULT_BATCH_SIZE;
        }
        // Use HttpDownload to make the http GET calls (because it handles a lot
        // of the
        // authentication stuff)
        boolean go = true;
        String surlCommon = baseDeletionURL.toExternalForm() + File.separator + collection;

        while (go) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (!tooBigRequest) {
                go = false;// only one go
            }
            String surl = surlCommon;
            surl = surl + "?maxRec=" + (rec + 1);
            if (start != null) {
                surl = surl + "&start=" + df.format(start);
            }
            if (end != null) {
                surl = surl + "&end=" + df.format(end);
            }
            URL url;
            log.debug("URL: " + surl);
            try {
                url = new URL(surl);
                HttpDownload get = new HttpDownload(url, bos);
                get.setFollowRedirects(true);

                get.run();
                int responseCode = get.getResponseCode();
                log.debug("RESPONSE CODE: '" + responseCode + "'");

                if (responseCode == 302) {
                    // redirected url
                    url = get.getRedirectURL();
                    log.debug("REDIRECTED URL: " + url);
                    bos = new ByteArrayOutputStream();
                    get = new HttpDownload(url, bos);
                    responseCode = get.getResponseCode();
                    log.debug("RESPONSE CODE (REDIRECTED URL): '" + responseCode + "'");

                }

                if (get.getThrowable() != null) {
                    if (get.getThrowable() instanceof AccessControlException) {
                        throw (AccessControlException) get.getThrowable();
                    }
                    throw new RuntimeException("failed to get observation list", get.getThrowable());
                }
            } catch (MalformedURLException e) {
                throw new RuntimeException("BUG: failed to generate observation list url", e);
            }

            try {
                // log.debug("RESPONSE = '" + bos.toString() + "'");
                partialList = transformer.read(new ByteArrayInputStream(bos.toByteArray()));
                // partialList =
                // transformByteArrayOutputStreamIntoListOfObservationState(bos,
                // df, '\t', '\n');
                if (partialList != null && !partialList.isEmpty() && !accList.isEmpty() && accList.get(accList.size() - 1).equals(partialList.get(0))) {
                    partialList.remove(0);
                }

                if (partialList != null) {
                    accList.addAll(partialList);
                    log.debug("adding " + partialList.size() + " elements to accList. Now there are " + accList.size());
                }

                bos.close();
            } catch (ParseException | URISyntaxException | IOException e) {
                throw new RuntimeException("Unable to list of ObservationState from " + bos.toString(), e);
            }

            if (accList.size() > 0) {
                start = accList.get(accList.size() - 1).getLastModified();
            }

            recCounter = accList.size();
            if (maxrec != null && maxrec - recCounter > 0 && maxrec - recCounter < rec) {
                rec = maxrec - recCounter;
            }

            int i = 0;
            for (DeletedObservation de : accList) {
                log.debug("accList.get( " + i++ + ") = " + de.getLastModified());
            }
            log.debug("accList.size() = " + accList.size());
            log.debug("dynamic batch (rec): " + rec);
            log.debug("maxrec: " + maxrec);
            log.debug("start: " + start);
            log.debug("end: " + end);

            if (partialList != null) {
                log.debug("partialList.size(): " + partialList.size());

                if (partialList.size() < rec || (end != null && start != null && start.equals(end))) {
                    log.debug("************** go false");

                    go = false;
                }
            }
        }
        return partialList;
    }
}