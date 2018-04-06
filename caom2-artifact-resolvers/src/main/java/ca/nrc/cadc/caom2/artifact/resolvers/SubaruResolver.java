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

import ca.nrc.cadc.caom2.artifact.resolvers.util.ResolverUtil;
import ca.nrc.cadc.net.NetUtil;
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
public class SubaruResolver implements StorageResolver {
    private static final Logger log = Logger.getLogger(SubaruResolver.class);
    private static final String SCHEME = "subaru";
    private static final String RAW_DATA_URI = "raw";
    private static final String PREVIEW_URI = "preview";

    private static final String BASE_DATA_URL = "http://www.canfar.net/maq/subaru?frameinfo=";

    private static final String PREVIEW_BASE_URL = "http://smoka.nao.ac.jp/qlis/ImagePNG";
    private static final String PREVIEW_URL_QUERY = "grayscale=linear&mosaic=true&frameid=";

    public SubaruResolver() {
    }

    /**
     * Convert the specified URI to one or more URL(s).
     *
     * @param uri the URI to convert
     * @return a URL to the identified resource
     * @throws IllegalArgumentException if the scheme is not equal to the value from getScheme()
     *                                  the uri is malformed such that a URL cannot be generated, or the uri is null
     */
    @Override
    public URL toURL(URI uri) {
        ResolverUtil.validate(uri, SCHEME);
        return createURLFromPath(uri);
    }

    /**
     * Validate path portion of URI for structure and create URL if possible
     *
     * @param uri
     * @return URL
     * @throws IllegalArgumentException if the scheme is not equal to the value from getScheme()
     *                                  the uri is malformed such that a URL cannot be generated, or the uri is null
     */
    private URL createURLFromPath(URI uri) {
        URL newUrl = null;
        String[] path = uri.getSchemeSpecificPart().split("/");
        // data uri has 3 parts, preview has 2

        if (path.length < 2 || path.length > 3) {
            throw new IllegalArgumentException("Malformed URI. Expected 2 or 3 path components, found " + path.length);
        }

        String sb = "";
        String requestType = path[0];
        if (path.length == 2 && requestType.equals(PREVIEW_URI)) {
            // Returns a web page reference
            // expected URI input is subaru:preview/<FRAMEID>
            sb = PREVIEW_BASE_URL + "?" + PREVIEW_URL_QUERY + path[1];
        } else if (path.length == 3 && requestType.equals(RAW_DATA_URI)) {
            // expected URI input is subaru:raw/YYYY-MM-dd/<FRAMEID>
            // expected URL output is http://www.canfar.net/maq/subaru?frameinfo=YYYY-MM-dd/FRAMEID
            sb = BASE_DATA_URL +  NetUtil.encode(path[1] + "/" + path[2]);
        } else {
            throw new IllegalArgumentException("Invalid URI: " + requestType);
        }

        try {
            newUrl = new URL(sb);
            log.debug(uri + " --> " + newUrl);
            return newUrl;
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Malformed URI: could not generate URL from uri " + sb, ex);
        }

    }

    /**
     * Returns the scheme for the storage resolver.
     *
     * @return a String representing the schema.
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

}

