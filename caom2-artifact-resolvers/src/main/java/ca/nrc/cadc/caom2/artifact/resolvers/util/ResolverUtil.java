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

import java.net.URI;
import org.apache.log4j.Logger;


public class ResolverUtil {
    private static final Logger log = Logger.getLogger(ResolverUtil.class);
    private static String INVALID_URI = "Invalid URI: ";
    private static String CANT_BE_NULL = "URI can't be null";
    private static String INVALID_SCHEME = " Got scheme: %s. Expected: %s";
    private static String CANT_CREATE_URL = "Cannot create URL: ";
    private static String BASEURL_EMPTY = "Base URL can't be null . ";

    public static void validate(URI uri, String scheme) {
        if (uri == null) {
            log.error(INVALID_URI + CANT_BE_NULL);
            throw new IllegalArgumentException(INVALID_URI + CANT_BE_NULL);
        }

        if (!scheme.equals(uri.getScheme())) {
            log.error(INVALID_URI + uri + String.format(INVALID_SCHEME, uri.getScheme(), scheme));
            throw new IllegalArgumentException(INVALID_URI + uri + String.format(INVALID_SCHEME, uri.getScheme(), scheme));
        }
    }

    public static String createURLFromPath(URI uri, String baseURL) throws IllegalArgumentException {
        String newURL = "";

        if (uri != null) {
            String path = uri.getSchemeSpecificPart();
            if (baseURL == null || baseURL.isEmpty()) {
                log.error(CANT_CREATE_URL + BASEURL_EMPTY + uri.toString());
                throw new IllegalArgumentException(CANT_CREATE_URL + BASEURL_EMPTY + uri.toString());
            }
            newURL = baseURL + path;
        } else {
            log.error(CANT_CREATE_URL + CANT_BE_NULL);
            throw new IllegalArgumentException(CANT_CREATE_URL + CANT_BE_NULL);
        }

        return newURL;
    }
}
