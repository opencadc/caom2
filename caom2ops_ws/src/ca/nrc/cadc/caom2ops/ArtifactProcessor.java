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

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.util.CutoutUtil;
import ca.nrc.cadc.datalink.DataLink;
import ca.nrc.cadc.datalink.util.AuthMethod;
import ca.nrc.cadc.datalink.util.CaomSchemeHandler;
import ca.nrc.cadc.datalink.util.SchemeHandler;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Convert Artifacts to DataLinks.
 * 
 * @author pdowler
 */
public class ArtifactProcessor
{
    private static final Logger log = Logger.getLogger(ArtifactProcessor.class);

    private static URI DOWNLOAD;
    private static URI CUTOUT;

    private static URI CUTOUT_SERVICE;

    static
    {
        try
        {
            DOWNLOAD = new URI(DataLink.DOWNLOAD);
            CUTOUT = new URI(DataLink.CUTOUT);
            CUTOUT_SERVICE = new URI("ivo://cadc.nrc.ca/cutout");
        }
        catch(URISyntaxException bug)
        {
            log.error("BUG", bug);
        }
    }

    private AuthMethod authMethod;
    private RegistryClient registryClient;
    private String runID;
    private SchemeHandler schemeHandler;
    private boolean downloadOnly;

    public ArtifactProcessor(String runID, RegistryClient registryClient)
    {
        this.runID = runID;
        this.registryClient = registryClient;
        this.schemeHandler = new CaomSchemeHandler();
    }

    public void setAuthMethod(AuthMethod authMethod)
    {
        this.authMethod = authMethod;
        schemeHandler.setAuthMethod(authMethod);
    }
    
    /**
     * Force DataLink generation to only include file download links. This is used
     * when passing the output off to the ManifestWriter instead of creating all the
     * links and having the writer filter them.
     * 
     * @param downloadOnly
     */
    public void setDownloadOnly(boolean downloadOnly)
    {
        this.downloadOnly = downloadOnly;
    }

    public List<DataLink> process(URI uri, List<Artifact> artifacts)
    {
        List<DataLink> ret = new ArrayList<DataLink>(artifacts.size());
        for (Artifact a : artifacts)
        {
            // direct download links
            try
            {
                URL download = getDownloadURL(a);
                DataLink dl = new DataLink(uri, download);
                dl.serviceType = DOWNLOAD;
                // semantics
                dl.contentType = a.contentType;
                dl.contentLength = a.contentLength;
                findProductTypes(a, dl.productTypes);
                ret.add(dl);
            }
            catch(MalformedURLException ex)
            {
                throw new RuntimeException("failed to generate download URL for " + a.getURI(), ex);
            }

            if (!downloadOnly)
                // service links
                try
                {
                    URL cutout = getCutoutURL(a);
                    if (cutout != null)
                    {
                        DataLink dl = new DataLink(uri, cutout);
                        dl.serviceType = CUTOUT;
                        dl.contentType = a.contentType; // unchanged
                        dl.contentLength = null; // unknown
                        findProductTypes(a, dl.productTypes);
                        ret.add(dl);
                    }
                    else
                        log.debug(a.getURI() + ": no cutout URL");

                }
                catch(MalformedURLException ex)
                {
                    throw new RuntimeException("failed to generate cutout URL for " + a.getURI(), ex);
                }
        }
        return ret;
    }

    protected void findProductTypes(Artifact a, List<ProductType> pts)
    {
        if (a.productType != null && !pts.contains(a.productType))
            pts.add(a.productType);
        for (Part p : a.getParts())
        {
            if (p.productType != null && !pts.contains(p.productType))
                pts.add(p.productType);
            for (Chunk c : p.getChunks())
            {
                if (c.productType != null && !pts.contains(c.productType))
                    pts.add(c.productType);
            }
        }
    }

    /**
     * Convert a URI to a URL. TBD: This method fails if the SchemeHandler returns multiple URLs,
     * but in principle we could make multiple DataLinks out of it.
     *
     * @param uri
     * @return url
     */
    protected URL getDownloadURL(Artifact a)
        throws MalformedURLException
    {
        URL url = schemeHandler.getURL(a.getURI());

        if ( StringUtil.hasText(runID) )
        {
            String appendQS = "?runid=";
            String qs = url.getQuery();
            if (qs != null && qs.length() > 0)
                appendQS = "&runid=";
            String surl = url.toExternalForm() + appendQS + runID;
            return new URL(surl);
        }
        return url;
    }

    

    protected URL getCutoutURL(Artifact a)
        throws MalformedURLException
    {
        String proto = "http";
        if ( AuthMethod.CERT.equals(authMethod))
            proto = "https";
        if ( canCutout(a) )
        {
            StringBuilder sb = new StringBuilder();
            sb.append("?");
            if (runID != null)
                sb.append("runid=").append(runID).append("&");
            sb.append("uri=").append(a.getURI()).append("&");
            URL ret = registryClient.getServiceURL(CUTOUT_SERVICE, proto, sb.toString());
            return ret;
        }
        return null;
    }

    // determine if artifact has sufficient WCS for cutout in any of the chunks
    private boolean canCutout(Artifact a)
    {
        for (Part p : a.getParts())
        {
            for (Chunk c : p.getChunks())
            {
                if ( CutoutUtil.canCutout(c) )
                    return true;
            }
        }
        return false;
    }
}
