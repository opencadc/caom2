/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.uws.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.pkg.server.PackageItem;
import org.opencadc.pkg.server.PackageRunner;


public class CaomPackageRunner extends PackageRunner {
    private static final Logger log = Logger.getLogger(CaomPackageRunner.class);

    private final URI tapID;
    private List<PublisherID> publisherIDList;
    private String runID;

    public CaomPackageRunner()
    { 
        ServiceConfig sc = new ServiceConfig();
        this.tapID = sc.getTapServiceID();
    }

    @Override
    protected void initPackage() throws IllegalArgumentException {

        // Validate the IDs passed in through the Job
        // Set up a list of URIs to process
        List<String> idList = ParameterUtil.findParameterValues("ID", this.job.getParameterList());
        publisherIDList = new ArrayList<PublisherID>();

        runID = this.job.getID();
        if (this.job.getRunID() != null) {
            runID = this.job.getRunID();
        }

        if (idList.size() == 0) {
            log.info("No IDs found in Job " + runID + " nothing to process.");
        }

        // Validate the quality of the URIs in idList
        for (String strURI : idList) {
            try {
                URI nextURI = new URI(strURI);
                // Validation within PublisherID ctor throws IllegalArgumentExceptions
                // which will be thrown by initPackage()
                PublisherID nextID = new PublisherID(nextURI);
                publisherIDList.add(nextID);
            } catch (URISyntaxException uriErr) {
                throw new IllegalArgumentException("invalid URI found: " + strURI, uriErr);
            }
        }

        // set the packageName based on size of the idList
        if (publisherIDList.size() == 1) {
            PublisherID publisherID = publisherIDList.get(0);
            this.packageName = getFilenamefromURI(publisherID);

        } else {
            // Otherwise, make a unique name using job ID
            StringBuilder sb = new StringBuilder();
            sb.append("cadc-download-");
            sb.append(runID);
            this.packageName = sb.toString();

        }
    }

    @Override
     public Iterator<PackageItem> getItems() throws IOException {


        List<PackageItem> packageItems = new ArrayList<PackageItem>();
        try {

            // obtain credentials from CDP if the user is authorized
            AccessControlContext accessControlContext = AccessController.getContext();
            Subject subject = Subject.getSubject(accessControlContext);
            AuthMethod authMethod = AuthenticationUtil.getAuthMethod(subject);
            AuthMethod proxyAuthMethod = authMethod;

            if ( CredUtil.checkCredentials() )
            {
                proxyAuthMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            }

            // runID is checked in initPackage()
            CaomTapQuery query = new CaomTapQuery(tapID, runID);

            CaomArtifactResolver artifactResolver = new CaomArtifactResolver();
            artifactResolver.setAuthMethod(proxyAuthMethod);
            artifactResolver.setRunID(runID);

            for (PublisherID publisherID : publisherIDList) {
                try {

                    // If query fails, the id will be skipped
                    ArtifactQueryResult result = query.performQuery(publisherID, true);

                    List<Artifact> artifacts = result.getArtifacts();
                    stripPreviews(artifacts);

                    if (artifacts.isEmpty()) {
                        // either the input ID was: not found, access-controlled, or has no artifacts
                        log.info(this.packageName + ": no files available for ID =" + publisherID.getURI().toASCIIString());
                    } else {
                        for (Artifact a : artifacts) {
                            URL url = artifactResolver.getURL(a.getURI());

                            String artifactName = a.getURI().getSchemeSpecificPart();
                            log.debug("new PackageItem: " + a.getURI() + " from " + url);
                            log.debug("package entry filename " + artifactName);

                            PackageItem newItem = new PackageItem(artifactName, url);
                            packageItems.add(newItem);
                        }
                    }
                } catch (ResourceNotFoundException resourceEx) {
                    // Skip this entry and continue to process
                    log.info("plane not found: " + publisherID.getURI().toASCIIString() + "skipping...");
                }
            }
        }
        catch (CertificateException certEx) {
            // Stop - can be thrown by CredUtil check or TAP query inside for loop
            log.info("invalid delegated client certificate");
            new AccessControlException("invalid delegated client certificate");
        }

        return packageItems.iterator();
    }

    /**
     * Build a filename from the given Publisher ID.
     * Publisher ID is expected to be in this format:
     * ivo://{authority}/{path}/{collection}?{observationID}/{productID}
     * @param pid - Publisher ID instance to parse to make the filename.
     * @return - filename built of uri components.
     */
    protected static String getFilenamefromURI(PublisherID pid)
    {
        String collection = pid.getResourceID().getPath();
        int i = collection.lastIndexOf("/");
        if (i >= 0) {
            collection = collection.substring(i + 1);
        }

        String pidQuery = pid.getURI().getQuery();
        log.debug("query string being parsed for obsID and productID: " + pidQuery);
        String[] queryparts = pidQuery.split("/");

        String observationID = queryparts[0];
        String productID = queryparts[1];

        StringBuilder sb = new StringBuilder();
        sb.append(collection).append("-");
        sb.append(observationID).append("-");
        sb.append(productID);
        return sb.toString();
    }
    
    private void stripPreviews(List<Artifact> artifacts)
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

}
