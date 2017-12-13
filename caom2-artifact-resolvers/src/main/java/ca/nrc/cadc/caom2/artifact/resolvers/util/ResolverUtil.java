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

package ca.nrc.cadc.caom2.artifact.resolvers.util;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Logger;

public class ResolverUtil {
    private static final Logger log = Logger.getLogger(ResolverUtil.class);
    private static final String INVALID_URI = "Invalid URI: ";
    private static final String CANT_BE_NULL = "URI can't be null";
    private static final String INVALID_SCHEME = " Got scheme: %s. Expected: %s";
    private static final String CANT_CREATE_URL = "Cannot create URL: ";
    private static final String BASEURL_EMPTY = "Base URL can't be null . ";
    private static final String CANNOT_GET_URL = "Can't generate URL from URI.";

    public static void validate(URI uri, String scheme) {
        if (uri == null) {
            throw new IllegalArgumentException(INVALID_URI + CANT_BE_NULL);
        }

        if (!scheme.equals(uri.getScheme())) {
            throw new IllegalArgumentException(INVALID_URI + uri + String.format(INVALID_SCHEME, uri.getScheme(), scheme));
        }
    }

    public static URL createURLFromPath(URI uri, String baseURL) throws IllegalArgumentException  {
        URL newURL = null;

        if (uri != null) {
            String path = uri.getSchemeSpecificPart();
            if (baseURL == null || baseURL.isEmpty()) {
                throw new IllegalArgumentException(CANT_CREATE_URL + BASEURL_EMPTY + uri.toString());
            }

            String s = baseURL + path;
            try {
                newURL = new URL(s);
            } catch (MalformedURLException ex) {
                throw new IllegalArgumentException(CANNOT_GET_URL + s, ex);
            }

            log.debug(uri + " --> " + newURL);
            return newURL;
        } else {
            throw new IllegalArgumentException(CANT_CREATE_URL + CANT_BE_NULL);
        }

    }
}
