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
import ca.nrc.cadc.net.StorageResolver;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * This class can convert a MAST URI into a URL.
 *
 * @author jeevesh
 */
public class MastResolver implements StorageResolver
{
    public static final String SCHEME = "mast";
    private static final Logger log = Logger.getLogger(MastResolver.class);
    private static final String MAST_BASE_ARTIFACT_URL = "https://masttest.stsci.edu/partners/download/file";
    private static final String CANNOT_GET_URL = "Can't generate URL from URI.";

    private AuthMethod authMethod;

    public MastResolver()
    {
    }

    @Override
    public URL toURL(URI uri)
    {
        this.validateScheme(uri);
        String s = "";
        try
        {
            s = this.createURL(uri);
            URL url = null;
            if (s != null)
            {
                url = new URL(s);
            }
            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex)
        {
            throw new RuntimeException("BUG: could not generate URL from uri " + s, ex);
        }
    }

    protected String createURL(URI uri) throws IllegalArgumentException {

        String newURL = "";

        String schemeStr = uri.getScheme();

        if (schemeStr.equals(SCHEME))
        {
            String path = uri.getSchemeSpecificPart();
            if (path.isEmpty())
            {
                log.error(CANNOT_GET_URL + " Path portion of URI is empty: " + uri.toString());
                throw new IllegalArgumentException(CANNOT_GET_URL + "Path is empty." + uri.toString());
            }
            newURL = MAST_BASE_ARTIFACT_URL + "/" + path;
        } else
        {
            log.error(CANNOT_GET_URL + " Invalid scheme (should be 'mast'): " + uri.toString());
            throw new IllegalArgumentException(CANNOT_GET_URL + " Invalid scheme (should be 'mast'): " + uri.toString());
        }

        return newURL;
    }

    public URL toURL(URI uri, List<String> cutouts) throws UnsupportedOperationException
    {
        // MAST doesn't support cutouts, so if the request hast to go to
        // that service instead of serving from the local instance,
        throw new UnsupportedOperationException(CANNOT_GET_URL + "Cutouts not yet supported for MAST URIs.");
    }

    public void setAuthMethod(AuthMethod authMethod)
    {
        this.authMethod = authMethod;
    }

    private String getPath(URI uri)
    {
        return uri.getSchemeSpecificPart();
    }


    @Override
    public String getSchema() {
        return SCHEME;
    }

    protected void validateScheme(URI uri)
    {
        if (uri == null)
        {
            log.error(CANNOT_GET_URL + " URI can't be null.");
            throw new IllegalArgumentException(CANNOT_GET_URL + " URI can't be null.");
        }

        if (!SCHEME.equals(uri.getScheme()))
        {
            throw new IllegalArgumentException("invalid scheme in " + uri);
        }
    }


}

