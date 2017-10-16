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
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class VOSpaceResolver implements StorageResolver {
    private static final Logger log = Logger.getLogger(VOSpaceResolver.class);

    public static final String SCHEME = "vos";
    public static final String CUTOUT_VIEW = "ivo://cadc.nrc.ca/vospace/view#cutout";
    public static final String PROTOCOL_HTTP_GET = "ivo://ivoa.net/vospace/core#httpget";
    public static final String PROTOCOL_HTTPS_GET = "ivo://ivoa.net/vospace/core#httpsget";
    public static final String pullFromVoSpaceValue = "pullFromVoSpace";

    protected AuthMethod authMethod;

    public VOSpaceResolver() {
        this.authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
    }

    @Override
    public URL toURL(URI uri) {
        this.validateScheme(uri);

        try {
            String s = this.createURL(uri);
            URL url = null;
            if (s != null) {
                url = new URL(s);
            }
            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG", ex);
        }
    }

    public void setAuthMethod(AuthMethod authMethod) {
        this.authMethod = authMethod;
    }

    @Override
    public String getSchema() {
        return SCHEME;
    }

    protected void validateScheme(URI uri) {
        if (!SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException("invalid scheme in " + uri);
        }
    }

    protected String createURL(URI uri) {
        // Temporary: because the VOSpace client doesn't support
        // cutouts through document posting, create cutout urls
        // using synctrans
        try {
            URI vuri = getVOSURI(uri);
            RegistryClient registryClient = new RegistryClient();
            URL baseURL = registryClient.getServiceURL(getServiceURI(vuri), Standards.VOSPACE_SYNC_21, authMethod);

            String scheme = baseURL.getProtocol();
            String protocol = null;
            if (scheme.equalsIgnoreCase("http")) {
                protocol = PROTOCOL_HTTP_GET;
            } else {
                protocol = PROTOCOL_HTTPS_GET;
            }

            StringBuilder query = new StringBuilder();
            query.append(baseURL);

            query.append("?");
            query.append("TARGET=").append(NetUtil.encode(vuri.toString()));
            query.append("&");
            query.append("DIRECTION=").append(NetUtil.encode(pullFromVoSpaceValue));
            query.append("&");
            query.append("PROTOCOL=").append(NetUtil.encode(protocol));

            return query.toString();
        } catch (Throwable t) {
            throw new RuntimeException("failed to convert " + uri, t);
        }
    }

    private URI getVOSURI(URI uri) {
        URI vosURI = null;

        String path = uri.getPath();
        if (path != null && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        String authority = uri.getAuthority();
        if (authority == null) {
            throw new IllegalArgumentException("missing authority in URI: " + uri.toString());
        }
        
        try {
            vosURI = new URI(uri.getScheme(), authority, path, uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("URI malformed: " + uri.toString());
        }

        // Check the scheme is vos
        if (vosURI.getScheme() == null || !vosURI.getScheme().equalsIgnoreCase(SCHEME)) {
            throw new IllegalArgumentException("URI scheme must be vos: " + uri.toString());
        }

        return vosURI;
    }

    /**
     * return the service URI which is with ivo:// scheme. e.g. for VOSURI("vos://cadc.nrc.ca!vospace/zhangsa/nodeWithPropertiesA"), it returns:
     * URI("ivo://cadc.nrc.ca/vospace")
     *
     * @author Sailor Zhang, 2010-07-15
     */
    private URI getServiceURI(URI vuri) {
        String authority = vuri.getAuthority();
        authority = authority.replace('!', '/');
        authority = authority.replace('~', '/');
        String str = "ivo://" + authority;
        try {
            return new URI(str);
        } catch (URISyntaxException bug) {
            throw new RuntimeException("BUG: failed to create service URI from: " + vuri);
        }
    }
}
