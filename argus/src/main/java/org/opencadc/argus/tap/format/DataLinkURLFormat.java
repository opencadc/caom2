/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

package org.opencadc.argus.tap.format;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.DefaultTableWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class DataLinkURLFormat implements Format<Object> {

    private static final Logger log = Logger.getLogger(DataLinkURLFormat.class);

    // HACK: need this to try to lookup local datalink service from config
    private static final String DATALINK_COL_ID = "caomPublisherID";
    
    private transient URL baseLinksURL;
    private transient boolean baseLinksConfigChecked = false;

    public DataLinkURLFormat() {
    }

    @Override
    public Object parse(String s) {
        throw new UnsupportedOperationException("TAP Formats cannot parse strings.");
    }

    @Override
    public String format(Object o) {
        if (o == null) {
            return "";
        }

        String s = (String) o;
        URL linkURL;
        
        // try local config
        linkURL = getLocalDataLink(s);
        if (linkURL != null) {
            return linkURL.toExternalForm();
        }
        
        // try data collection with aux capability
        linkURL = resolvePublisherID(s);
        if (linkURL != null) {
            return linkURL.toExternalForm();
        }
        
        // fall through to unresolved
        return s;
    }

    // try to resolve the publisherID by looking up the data collection in the registry
    private URL resolvePublisherID(String publisherID) {

        // TODO: create URI and use scheme + authority + path?
        int i = publisherID.indexOf('?');
        if (i <= 0) {
            return null; // unrecognizable string structure
        }

        String rid = publisherID.substring(0, i);
        try {
            URI resourceID = new URI(rid);
            if (!"ivo".equals(resourceID.getScheme())) {
                throw new RuntimeException("BUG or CONFIG: expected publisherID to have 'ivo' scheme, got: " + resourceID.getScheme());  
            }
            
            RegistryClient rc = new RegistryClient();
            Subject caller = AuthenticationUtil.getCurrentSubject();
            AuthMethod cur = AuthenticationUtil.getAuthMethod(caller);
            URL baseURL = rc.getServiceURL(resourceID, Standards.DATALINK_LINKS_11, cur);
            if (baseURL == null) {
                return null; // no aux capability for data collection
            }
            StringBuilder sb = new StringBuilder();
            sb.append(baseURL.toExternalForm());
            sb.append("?ID=").append(NetUtil.encode(publisherID));
            return new URL(sb.toString());
        } catch (URISyntaxException ex) {
            throw new RuntimeException("CONFIG: extracted data collection identifier is not valid URI: " + rid, ex);
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate datalink URL for " + publisherID, ex);
        }
    }

    // get the locally configured datalink service from config
    private URL getLocalDataLink(String publisherID) {
        log.debug("getLocalDataLink: " + publisherID + " columnID=" + DATALINK_COL_ID);
        try {
            
            if (baseLinksURL == null && !baseLinksConfigChecked) {
                this.baseLinksURL = DefaultTableWriter.getAccessURL(DATALINK_COL_ID, Standards.DATALINK_LINKS_11);
                baseLinksConfigChecked = true; // try config once only
            }
            URL baseURL = baseLinksURL;
            if (baseURL == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(baseURL.toExternalForm());
            sb.append("?ID=").append(NetUtil.encode(publisherID));
            return new URL(sb.toString());
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate datalink URL for " + publisherID, ex);
        } catch (IOException ex) {
            throw new RuntimeException("CONFIG: failed to read config for " + DATALINK_COL_ID);
        } 
    }
}
