/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

public class RepoClient
{

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private static final URI standardID = Standards.CAOM2REPO_OBS_23;

    private final DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    private URI resourceID = null;
    private URL baseServiceURL = null;

    private int nthreads = 1;

    private RepoClient()
    {
    }

    /**
     * Create new CAOM RepoClient.
     *
     * @param resourceID
     *            the service identifier
     * @param nthreads
     *            number of threads to use when getting list of observations
     */
    public RepoClient(URI resourceID, int nthreads)
    {
        this.nthreads = nthreads;
        this.resourceID = resourceID;
    }

    private void init()
    {
        RegistryClient rc = new RegistryClient();

        Subject s = AuthenticationUtil.getCurrentSubject();
        AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(s);
        if (meth == null)
            meth = AuthMethod.ANON;
        this.baseServiceURL = rc.getServiceURL(this.resourceID, standardID, meth);
        if (baseServiceURL == null)
            throw new RuntimeException("not found: " + resourceID + " + " + standardID + " + " + meth);

        log.debug("service URL: " + baseServiceURL.toString());
        log.debug("AuthMethod:  " + meth);
    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end, Integer maxrec)
            throws AccessControlException
    {
        init();

        // Use HttpDownload to make the http GET calls (because it handles a lot
        // of the
        // authentication stuff)

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        String surl = baseServiceURL.toExternalForm() + File.separator + collection;
        if (maxrec != null)
            surl = surl + "?maxRec=" + maxrec;
        if (start != null)
            surl = surl + "&start=" + df.format(start);
        if (end != null)
            surl = surl + "&end=" + df.format(end);
        URL url;
        try
        {
            url = new URL(surl);
            HttpDownload get = new HttpDownload(url, bos);

            get.run();
            if (get.getThrowable() != null)
            {
                if (get.getThrowable() instanceof AccessControlException)
                    throw (AccessControlException) get.getThrowable();
                throw new RuntimeException("failed to get observation list", get.getThrowable());
            }
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException("BUG: failed to generate observation list url", e);
        }

        List<ObservationState> list = null;
        try
        {
            list = transformByteArrayOutputStreamIntoListOfObservationState(bos, df, '\t', '\n');

        }
        catch (ParseException | IOException e)
        {
            throw new RuntimeException("Unable to list of ObservationState from " + bos.toString(), e);
        }
        return list;
    }

    public Iterator<Observation> observationIterator()
    {
        return null;

    }

    public void setConfig(Map<String, Object> config1)
    {

    }

    public List<WorkerResponse> getList(String collection, Date startDate, Date end, Integer numberOfObservations)
            throws InterruptedException, ExecutionException
    {
        init();

        // startDate = null;
        // end = df.parse("2017-06-20T09:03:15.360");
        List<WorkerResponse> list = new ArrayList<WorkerResponse>();

        List<ObservationState> stateList = getObservationList(collection, startDate, end, numberOfObservations);

        // Create tasks for each file
        List<Callable<WorkerResponse>> tasks = new ArrayList<Callable<WorkerResponse>>();

        // the current subject usually gets propagated into a thread pool, but
        // gets attached
        // when the thread is created so we explicitly pass it it and do another
        // Subject.doAs in
        // case
        // thread pool management is changed
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        for (ObservationState os : stateList)
        {
            tasks.add(new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm()));
        }

        ExecutorService taskExecutor = null;
        try
        {
            // Run tasks in a fixed thread pool
            taskExecutor = Executors.newFixedThreadPool(nthreads);
            List<Future<WorkerResponse>> futures;

            futures = taskExecutor.invokeAll(tasks);

            for (Future<WorkerResponse> f : futures)
            {
                WorkerResponse res = null;
                res = f.get();

                if (f.isDone())
                {
                    list.add(res);
                }
            }
        }
        catch (InterruptedException | ExecutionException e)
        {
            log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: "
                    + e.getCause().toString());
            throw e;
        }
        finally
        {
            if (taskExecutor != null)
            {
                taskExecutor.shutdown();
            }
        }

        return list;
    }

    public WorkerResponse get(ObservationURI uri)
    {
        init();
        if (uri == null)
            throw new IllegalArgumentException("uri cannot be null");

        ObservationState os = new ObservationState(uri);

        // see comment above in getList
        Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
        Worker wt = new Worker(os, subjectForWorkerThread, baseServiceURL.toExternalForm());
        return wt.getObservation();
    }

    public WorkerResponse get(String collection, URI uri, Date start)
    {
        if (uri == null)
            throw new IllegalArgumentException("uri cannot be null");

        init();

        log.info("******************* getObservationList(collection, start, null, null) " + collection);

        List<ObservationState> list = getObservationList(collection, start, null, null);
        ObservationState obsState = null;
        for (ObservationState os : list)
        {
            if (!os.getURI().getURI().equals(uri))
            {
                continue;
            }
            obsState = os;
            break;
        }

        log.info("******************* getting to getList " + obsState);

        if (obsState != null)
        {
            // see comment above in getList
            Subject subjectForWorkerThread = AuthenticationUtil.getCurrentSubject();
            Worker wt = new Worker(obsState, subjectForWorkerThread, baseServiceURL.toExternalForm());
            return wt.getObservation();
        }
        else
        {
            return null;
        }
    }

    private List<ObservationState> transformByteArrayOutputStreamIntoListOfObservationState(
            final ByteArrayOutputStream bos, DateFormat sdf, char separator, char endOfLine)

            throws ParseException, IOException
    {
        init();

        List<ObservationState> list = new ArrayList<ObservationState>();

        // Reader reader = new Reader()
        // {
        // @Override
        // public int read(char[] cbuf, int off, int len) throws IOException
        // {
        // int res = 0;
        // int j = 0;
        //
        // cbuf = new char[len];
        //
        // for (int i = off; i < off + len; i++)
        // {
        // if (i < bos.size())
        // {
        // cbuf[j++] = (char) bos.toByteArray()[i];
        // res++;
        // } else
        // {
        // res = -1;
        // break;
        // }
        //
        // }
        //
        // return res;
        // }
        //
        // @Override
        // public void close() throws IOException
        // {
        // bos.close();
        //
        // }
        // };
        // CsvReader tsvreader = new CsvReader(reader);
        // tsvreader.setDelimiter('0');
        //
        // while (tsvreader.readRecord())
        // {
        // String collection = tsvreader.get(0);
        // String id = tsvreader.get(1);
        // Date date = sdf.parse(tsvreader.get(2));
        // ObservationState os = new ObservationState(collection, id, date,
        // resourceId);
        // list.add(os);
        // }
        //
        // tsvreader.close();

        // list = new ArrayList<ObservationState>();
        String id = null;
        String sdate = null;
        String collection = null;

        String aux = "";
        boolean readingCollection = true;
        boolean readingId = false;

        for (int i = 0; i < bos.toString().length(); i++)
        {
            char c = bos.toString().charAt(i);
            if (c != separator && c != endOfLine)
            {
                aux += c;
            }
            else if (c == separator)
            {
                if (readingCollection)
                {
                    collection = aux;
                    readingCollection = false;
                    readingId = true;
                    aux = "";

                }
                else if (readingId)
                {
                    id = aux;
                    readingCollection = false;
                    readingId = false;
                    aux = "";
                }

            }
            else if (c == endOfLine)
            {
                sdate = aux;
                aux = "";
                Date date = sdf.parse(sdate);

                ObservationState os = new ObservationState(new ObservationURI(collection, id));
                os.maxLastModified = date;

                list.add(os);
                readingCollection = true;
                readingId = false;

            }
        }
        return list;

    }
}