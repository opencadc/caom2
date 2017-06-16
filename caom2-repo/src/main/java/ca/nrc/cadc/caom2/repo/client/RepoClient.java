/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

public class RepoClient
{

	private static final Logger log = Logger.getLogger(RepoClient.class);
	private final DateFormat df = DateUtil
			.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
	private URI resourceId = null;
	private RegistryClient rc = null;
	private URL baseServiceURL = null;

	private Subject subject = null;
	private AuthMethod meth;
	private String collection = null;
	private int nthreads = 1;

	protected final String BASE_HTTP_URL;

	public RepoClient()
	{
		BASE_HTTP_URL = null;
	}

	// constructor takes service identifier arg
	public RepoClient(URI resourceID, String collection, int nthreads)
	{
		this.nthreads = nthreads;
		this.resourceId = resourceID;

		rc = new RegistryClient();

		// setup optional authentication for harvesting from a web service
		// get current subject
		// Create a subject
		subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));

		// subject = AuthenticationUtil.getCurrentSubject();
		if (subject != null)
		{
			meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
			// User RegistryClient to go from resourceID to service URL
		} else
		{
			meth = AuthMethod.ANON;

			log.info("No current subject found");
		}

		baseServiceURL = rc.getServiceURL(this.resourceId,
				Standards.CAOM2REPO_OBS_23, meth);
		BASE_HTTP_URL = baseServiceURL.toExternalForm();
		this.collection = collection;
		log.info("BASE SERVICE URL: " + baseServiceURL.toString());
		log.info("Authentication used: " + meth);

	}

	public List<ObservationState> getObservationList(String collection,
			Date start, Date end, Integer maxrec)
	{
		// Use HttpDownload to make the http GET calls (because it handles a lot
		// of the
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
		try
		{
			url = new URL(surl);
			HttpDownload get = new HttpDownload(url, bos);

			if (subject != null)
			{
				Subject.doAs(subject, new RunnableAction(get));

				log.info("Query run within subject");
			} else
			{
				get.run();
				log.info("Query run");
			}

		} catch (MalformedURLException e)
		{
			log.error("Exception in getObservationList: " + e.getMessage());
			e.printStackTrace();
		}

		List<ObservationState> list;
		try
		{
			list = transformByteArrayOutputStreamIntoListOfObservationState(bos,
					df, '\t', '\n');
		} catch (ParseException e)
		{
			throw new RuntimeException(
					"Unable to list of ObservationState from " + bos.toString()
							+ ": exception = " + e.getMessage());
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

	public List<Observation> getList(Class<Observation> c, Date startDate,
			Date end, Integer numberOfObservations, int depth)
	{

		List<Observation> list = new ArrayList<Observation>();

		List<ObservationState> stateList = getObservationList(collection,
				startDate, end, numberOfObservations);

		ByteArrayOutputStream bos = null;
		// Create tasks for each file
		List<Callable<Observation>> tasks = new ArrayList<Callable<Observation>>();

		for (ObservationState os : stateList)
		{
			tasks.add(new WorkerThread(os, subject, BASE_HTTP_URL));
		}

		// Run tasks in a fixed thread pool
		ExecutorService taskExecutor = Executors.newFixedThreadPool(nthreads);
		List<Future<Observation>> futures;
		try
		{
			futures = taskExecutor.invokeAll(tasks);
		} catch (InterruptedException e1)
		{
			throw new RuntimeException("Unable to create ExecutorService");
		}

		for (Future<Observation> f : futures)
		{
			Observation res = null;
			try
			{
				res = f.get();
			} catch (InterruptedException | ExecutionException e)
			{
				throw new RuntimeException("Unable execute thread");
			}
			if (f.isDone())
			{
				list.add(res);
			}
		}
		taskExecutor.shutdown();

		List<ObservationState> erroneousObservations = checkResults(list,
				stateList);

		if (erroneousObservations.size() > 0)
		{
			String erroneous = "";
			for (ObservationState os : erroneousObservations)
			{
				erroneous += os.getObservationID() + " ";
			}
			throw new RuntimeException(
					"errors reading observations: " + erroneous);
		}

		return list;
	}

	public UUID getID(ObservationURI uri)
	{
		Observation observation = get(uri, null, 1);
		if (observation != null)
			return observation.getID();
		return null;
	}

	public ObservationURI getURI(UUID id)
	{
		Observation observation = get(null, id, 1);
		if (observation != null)
			return observation.getURI();
		return null;
	}

	public Observation get(UUID id)
	{
		if (id == null)
			throw new IllegalArgumentException("id cannot be null");
		// TODO: redo in a more efficient way
		return get(null, id, 1);
	}

	public Observation get(ObservationURI uri)
	{
		if (uri == null)
			throw new IllegalArgumentException("uri cannot be null");
		ObservationState os = new ObservationState(uri.getCollection(),
				uri.getObservationID(), null, null);
		WorkerThread wt = new WorkerThread(os, subject, BASE_HTTP_URL);
		return wt.getObservation();
	}

	private Observation get(ObservationURI uri, UUID id, int depth)
	{
		if (id == null && uri == null)
		{
			throw new RuntimeException(
					"uri and id cannot be null at the same time");
		}
		Observation o = null;
		if (uri != null)
		{
			o = get(uri);
		} else if (id != null)
		{
			o = get(id);
		}
		return o;
	}

	private List<ObservationState> transformByteArrayOutputStreamIntoListOfObservationState(
			ByteArrayOutputStream bos, DateFormat sdf, char separator,
			char endOfLine) throws ParseException
	{

		List<ObservationState> list = new ArrayList<ObservationState>();
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
			} else if (c == separator)
			{
				if (readingCollection)
				{
					collection = aux;
					readingCollection = false;
					readingId = true;
					aux = "";

				} else if (readingId)
				{
					id = aux;
					readingCollection = false;
					readingId = false;
					aux = "";
				}

			} else if (c == endOfLine)
			{
				sdate = aux;
				aux = "";
				Date date = sdf.parse(sdate);
				ObservationState os = new ObservationState(collection, id, date,
						resourceId);
				list.add(os);
				readingCollection = true;
				readingId = false;

			}
		}
		return list;
	}

	private List<ObservationState> checkResults(
			List<Observation> observationList, List<ObservationState> stateList)
	{
		List<ObservationState> erroneous = new ArrayList<ObservationState>();

		boolean found = false;
		for (ObservationState os : stateList)
		{
			found = false;
			for (Observation o : observationList)
			{
				if (o.getObservationID().equals(os.getObservationID()))
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				erroneous.add(os);
			}
		}
		return erroneous;
	}

}
