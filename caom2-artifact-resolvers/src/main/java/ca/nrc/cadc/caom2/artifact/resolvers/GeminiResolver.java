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

import ca.nrc.cadc.net.StorageResolver;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Logger;

/**
 * This class can convert a GEMINI URI into a URL.
 *
 * @author jeevesh
 */
public class GeminiResolver implements StorageResolver {
    public static final String SCHEME = "gemini";
    public static final String FILE_URI = "file";
    public static final String PREVIEW_URI = "preview";
    private static final Logger log = Logger.getLogger(GeminiResolver.class);
    private static final String BASE_URL = "https://archive.gemini.edu";
    private static final String CANNOT_GENERATE_URL = "Can't generate URL from URI.";

    public GeminiResolver() {
    }

    @Override
    public URL toURL(URI uri) {
        this.validate(uri);
        String urlStr = "";
        try {
            String path = getPath(uri);
            urlStr = BASE_URL + path;

            URL url = null;
            if (urlStr != null) {
                url = new URL(urlStr);
            }

            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: could not generate URL from uri " + urlStr, ex);
        }
    }

    private String getPath(URI uri) {
        String[] path = uri.getSchemeSpecificPart().split("/");
        if (path.length != 2) {
            throw new IllegalArgumentException("Malformed URI. Expected 2 path components, found " + path.length);
        }

        String requestType = path[0];
        if (!(requestType.equals(FILE_URI) || requestType.equals(PREVIEW_URI))) {
            throw new IllegalArgumentException("Invalid URI. Expected 'file' or 'preview' and got " + requestType);
        }

        String fileName = path[1];

        StringBuilder sb = new StringBuilder();
        sb.append("/");
        sb.append(requestType);
        sb.append("/");
        sb.append(fileName);

        return sb.toString();
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    protected void validate(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException(CANNOT_GENERATE_URL + " URI can't be null.");
        }

        if (!SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException("Invalid scheme in " + uri + ". Expected " + SCHEME + ".");
        }
    }

}

