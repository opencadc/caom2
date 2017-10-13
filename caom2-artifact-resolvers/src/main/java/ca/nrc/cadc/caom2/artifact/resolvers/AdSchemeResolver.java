/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2017.                            (c) 2017.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

/**
 * SchemeHandler implementation for the Archive Directory (ad) system. This class can convert an AD URI into a URL. This is an alternate version that uses the
 * RegistryClient to find the data web service base URL.
 *
 * @author pdowler
 */
public class AdSchemeResolver implements StorageResolver {
    private static final Logger log = Logger.getLogger(AdSchemeResolver.class);

    public static final String SCHEME = "ad";

    private static final String DATA_URI = "ivo://cadc.nrc.ca/data";

    private RegistryClient rc;
    private URI dataURI;
    protected AuthMethod authMethod;

    public AdSchemeResolver() {
        this.rc = new RegistryClient();
        try {
            this.dataURI = new URI(DATA_URI);
        } catch (URISyntaxException bug) {
            throw new RuntimeException("BUG - failed to create data web service URI", bug);
        }
        this.authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
    }

    public URL toURL(URI uri) {
        if (!SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException("invalid scheme in " + uri);
        }

        try {
            String path = getPath(uri);
            AuthMethod am = this.authMethod;
            if (am == null) {
                am = AuthMethod.ANON;
            }
            URL serviceURL = rc.getServiceURL(dataURI, Standards.DATA_10, am);
            URL url = this.toURL(serviceURL, path);
            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG", ex);
        }
    }

    protected URL toURL(URL serviceURL, String path) throws MalformedURLException {
        return new URL(serviceURL.toExternalForm() + path);
    }
    
    public void setAuthMethod(AuthMethod authMethod) {
        this.authMethod = authMethod;
    }

    public String getSchema() {
        return SCHEME;
    }

    private String getPath(URI uri) {
        String[] path = uri.getSchemeSpecificPart().split("/");
        if (path.length != 2) {
            throw new IllegalArgumentException("malformed AD URI, expected 2 path componets, found " + path.length);
        }
        String arc = path[0];
        String fid = path[1];

        StringBuilder sb = new StringBuilder();
        sb.append("/");
        sb.append(encodeString(arc));
        sb.append("/");
        sb.append(encodeString(fid));

        return sb.toString();
    }

    private static String encodeString(String str) {
        try {
            return URLEncoder.encode(str, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException("BUG", ex);
        }
    }
}
