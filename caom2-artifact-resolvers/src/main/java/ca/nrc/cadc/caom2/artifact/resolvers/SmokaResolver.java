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
 * This class can convert a GEMINI URI into a URL.
 *
 * @author jeevesh
 */
public class SmokaResolver implements StorageResolver {
    private static final String SCHEME = "subaru";
    private static final String FILE_URI = "file";
    private static final String PREVIEW_URI = "preview";
    private static final String FILE_URL_QUERY = "object=&resolver=SIMBAD&coordsys=Equatorial&equinox=J2000&fieldofview=auto"
        + "&RadOrRec=radius&longitudeC=&latitudeC=&radius=10.0&longitudeF=&latitudeF=&longitudeT=&latitudeT"
        + "=&date_obs=&exptime=&observer=&prop_id=&frameid=&dataset=&asciitable=Table"
        + "&frameorshot=Frame&action=Search&instruments=SUP&instruments=HSC&multiselect_0=SUP&multiselect_0=HSC"
        + "&multiselect_0=SUP&multiselect_0=HSC&obs_mod=IMAG&obs_mod=SPEC&obs_mod=IPOL&multiselect_1=IMAG&multiselect_1=SPEC"
        + "&multiselect_1=IPOL&multiselect_1=IMAG&multiselect_1=SPEC&multiselect_1=IPOL&data_typ=OBJECT&multiselect_2=OBJECT"
        + "&multiselect_2=OBJECT&bandwidth_type=FILTER&band=&dispcol=FRAMEID&dispcol=DATE_OBS&dispcol=FITS_SIZE&dispcol=OBS_MODE"
        + "&dispcol=DATA_TYPE&dispcol=OBJECT&dispcol=FILTER&dispcol=WVLEN&dispcol=DISPERSER&dispcol=RA2000&dispcol=DEC2000"
        + "&dispcol=UT_START&dispcol=EXPTIME&dispcol=OBSERVER&dispcol=EXP_ID&orderby=FRAMEID&diff=100&output_equinox=J2000&from=0"
        + "&exp_id="; //&exp_id=SUPE01318470 or similar for last entry here.
    private static final String PREVIEW_URL_QUERY = "grayscale=linear&mosaic=true&frameid=";
    private static final Logger log = Logger.getLogger(SmokaResolver.class);
    private static final String BASE_URL = "http://smoka.nao.ac.jp";
    private static final String PREVIEW_BASE_URL = BASE_URL + "/qlis/ImagePNG";
    private static final String FILE_BASE_URL = BASE_URL + "/fssearch";

    public SmokaResolver() {
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
        if (path.length != 2) {
            throw new IllegalArgumentException("Malformed URI. Expected 2 path components, found " + path.length);
        }

        String requestType = path[0];
        String fileName = path[1];
        String sb = "";

        if (requestType.equals(FILE_URI)) {
            // Returns a quick search-style URL for SMOKA Search page
            sb = FILE_BASE_URL + "?" + FILE_URL_QUERY + fileName;
        } else if (requestType.equals(PREVIEW_URI)) {
            // Returns a web page reference
            sb = PREVIEW_BASE_URL + "?" + PREVIEW_URL_QUERY + fileName;
        } else {
            throw new IllegalArgumentException("Invalid URI. Expected 'file' or 'preview' and got " + requestType);
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

