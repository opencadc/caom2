/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

package ca.nrc.cadc.caom2.pkg;

import org.opencadc.pkg.server.PackageItem;
import org.opencadc.pkg.server.PackageRunner;
import org.opencadc.pkg.server.PackageRunnerException;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.log.WebServiceLogInfo;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URISyntaxException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.security.auth.Subject;
import org.apache.log4j.Logger;


public class Caom2PackageRunner extends PackageRunner {
    private static final Logger log = Logger.getLogger(Caom2PackageRunner.class);

    private final URI tapID;
    private List<String> idList;
    private String packageName;

    public Caom2PackageRunner()
    { 
        ServiceConfig sc = new ServiceConfig();
        this.tapID = sc.getTapServiceID();
    }

    @Override
    protected String getPackageName() {
        return this.packageName;
    }


    @Override
     public Iterator<PackageItem> getItems() throws IOException, PackageRunnerException {

        // obtain credentials from CDP if the user is authorized
        AccessControlContext accessControlContext = AccessController.getContext();
        Subject subject = Subject.getSubject(accessControlContext);
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(subject);

        String runID = this.job.getID();
        if (this.job.getRunID() != null) {
            runID = this.job.getRunID();
        }

        List<String> idList = ParameterUtil.findParameterValues("ID", this.job.getParameterList());

        CaomTapQuery query = new CaomTapQuery(tapID, runID);

        CaomArtifactResolver artifactResolver = new CaomArtifactResolver();
        artifactResolver.setAuthMethod(authMethod); // override auth method for proxied calls
        artifactResolver.setRunID(runID);

        List<PackageItem> packageItems = new ArrayList<PackageItem>();

        for (String suri : idList) {

            try {
                // If this fails, the id will be skipped
                URI uri = new URI(suri);

                PlaneURI puri;
                ArtifactQueryResult result;

                // If either query fails, the id will be skipped
                if (PublisherID.SCHEME.equals(uri.getScheme())) {
                    PublisherID p = new PublisherID(uri);
                    result = query.performQuery(p, true);
                    puri = toPlaneURI(p);
                } else {
                    puri = new PlaneURI(uri);
                    result = query.performQuery(puri, true);
                }

                List<Artifact> artifacts = result.getArtifacts();
                stripPreviews(artifacts);

                // set package name is done here because the plane URI is
                // used to create it if there is only one
                if (!StringUtil.hasText(packageName)) {
                    if (idList.size() == 1) {
                        // For a single id, package name is derived from
                        // Publisher URI
                        this.packageName = getFilenamefromURI(puri);
                    } else {
                        // Otherwise, make a unique name using job ID
                        StringBuilder sb = new StringBuilder();
                        sb.append("cadc-download-");
                        sb.append(this.job.getID());
                        this.packageName = sb.toString();
                    }
                }

                if (artifacts.isEmpty()) {
                    // either the input ID was: not found, access-controlled, or has no artifacts
                    log.info(this.packageName + "no files available for ID=" + suri);
                } else {
                    for (Artifact a : artifacts) {
                        URL url = artifactResolver.getURL(a.getURI());

                        String artifactName = a.getURI().getSchemeSpecificPart();
                        log.debug("new PackageItem: " + a.getURI() + " from " + url);
                        log.debug("package entry filename " + artifactName);

                        PackageItem newItem = new PackageItem(url, artifactName);
                        packageItems.add(newItem);
                    }
                }
            } catch (URISyntaxException uriEx) {
                // Skip this entry and continue to process
                log.info("invalid plane URI: " + suri + "skipping...");

            } catch (ResourceNotFoundException resourceEx) {
                // Skip this entry and continue to process
                log.info("plane not found: " + suri + "skipping...");
            } catch (CertificateException certEx) {
                // Stop -
                log.info("invalid certificate");
                throw new RuntimeException("invalid certificate", certEx);
            }
        }

        return packageItems.iterator();
    }


    // temporary hack to support both caom and ivo uris in getFilenamefromURI
    // used by a unit test
    static PlaneURI toPlaneURI(PublisherID pid)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("caom:");
        String collection = pid.getResourceID().getPath();
        int i = collection.lastIndexOf("/");
        if (i >= 0)
            collection = collection.substring(i+1);
        sb.append(collection).append("/");
        sb.append(pid.getURI().getQuery());
        return new PlaneURI(URI.create(sb.toString()));
    }
    
    // used in an int-test
    public static String getFilenamefromURI(PlaneURI uri)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(uri.getParent().getCollection()).append("-");
        sb.append(uri.getParent().getObservationID()).append("-");
        sb.append(uri.getProductID());
        return sb.toString();
    }
    
    public void stripPreviews(List<Artifact> artifacts)
    {
        ListIterator<Artifact> iter = artifacts.listIterator();
        while (iter.hasNext())
        {
            Artifact a = iter.next();
            if ( ProductType.PREVIEW.equals(a.getProductType())
                || ProductType.THUMBNAIL.equals(a.getProductType()) )
            {
                iter.remove();
                log.debug("stripPreviews: removed " + a.getProductType() + " " + a.getURI());
            }
        }
    }


    private class ObservationNotFoundException extends Exception
    {
        public ObservationNotFoundException(ObservationURI uri)
        {
            super("not found: " + uri.getURI().toASCIIString());
        }
    }

}
