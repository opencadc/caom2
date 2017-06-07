package ca.nrc.cadc.caom2.repo.client;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClient  {

    private static final Logger log = Logger.getLogger(RepoClient.class);
    private URI resourceId = null;
    private RegistryClient rc = null;
    private URL serviceURL = null;
    private URLConnection urlConnection = null;
    private List<ObservationState> observationStates = new ArrayList<ObservationState>();
    private List<Observation> observations = new ArrayList<Observation>();
    private Subject subject = null;

    // constructor takes service identifier arg
    public RepoClient(URI resourceID) {
        this.resourceId = resourceID;

        rc = new RegistryClient();
        
        log.info("******"+System.getProperty("user.home"));




            

            // setup optional authentication for harvesting from a web service
                
            subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));

            if (subject != null)
            {
            	AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
                // User RegistryClient to go from resourceID to service URL
                serviceURL = rc.getServiceURL(this.resourceId, Standards.CAOM2REPO_OBS_20, meth);
                log.info("SERVICE URL: " + serviceURL.toString());
                log.info("authentication using: " + meth);
            } else {
            	log.error("Subject is NULL");
            }
              

         

    }

    public List<ObservationState> getObservationList(String collection, Date start, Date end,
            Integer maxrec) {
        // Use HttpDownload to make the http GET calls (because it handles a lot of the
        // authentication stuff)

    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //HttpDownload get = new HttpDownload(serviceURL, bos);
    	URL url;
		try {
			url = new URL("http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2repo/auth/IRIS?maxrec=5");

    	
    	HttpDownload get = new HttpDownload(url, bos);
    	
        if (subject != null)
        {    
            Subject.doAs(subject, new RunnableAction(get));
            
            log.info("Get run within subject");
        } else {
        	get.run();
        	log.info("Subject is null");
        }
        
        log.info("Got from URL: "+ bos.toString());
        
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
        Log4jInit.setLevel("ca.nrc.cadc.net.HttpDownload", Level.TRACE);
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
