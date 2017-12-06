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
import ca.nrc.cadc.net.StorageResolver;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Logger;

/**
 * This class can convert a MAST URI into a URL.
 *
 * @author jeevesh
 */
public class NoaoResolver implements StorageResolver {
    public static final String SCHEME = "noao";
    private static final Logger log = Logger.getLogger(NoaoResolver.class);
    // Initial
    //    noao:kp973912.fits.gz
    private static final String BASE_ARTIFACT_URL = "http://nsaserver.sdm.noao.edu:7003/?fileRef=";
    private static final String CANNOT_GET_URL = "Can't generate URL from URI.";

    public NoaoResolver() {
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URL toURL(URI uri) {
        ResolverUtil.validate(uri, SCHEME);
        String s = "";
        try {
            s = ResolverUtil.createURLFromPath(uri, BASE_ARTIFACT_URL);
            URL url = null;
            url = new URL(s);

            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException(CANNOT_GET_URL + s, ex);
        }
    }

}

