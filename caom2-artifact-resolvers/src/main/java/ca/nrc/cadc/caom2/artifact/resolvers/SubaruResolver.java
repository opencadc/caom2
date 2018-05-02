/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.caom2.artifact.resolvers.util.ResolverUtil;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.StorageResolver;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.log4j.Logger;

/**
 * This class can convert a SUBARU URI into a URL.
 *
 */
public class SubaruResolver implements StorageResolver {
    private static final Logger log = Logger.getLogger(SubaruResolver.class);
    private static final String SCHEME = "subaru";
    private static final String RAW_DATA_URI = "raw";
    private static final String PREVIEW_URI = "preview";

    private static final String BASE_DATA_URL = "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/maq/subaru?frameinfo=";

    // Suprime-Cam
    private static final String PREVIEW_BASE_URL = "http://smoka.nao.ac.jp";
    private static final String PREVIEW_URL_PATH = "/qlis/ImagePNG";
    private static final String PREVIEW_URL_QUERY = "grayscale=linear&mosaic=true&frameid=";

    // HSC (Hyper Suprime-Cam)
    // Suprime-Cam
    //    http://smoka.nao.ac.jp/shot/HSC/2015-01-17/HSCA018210.png
    private static final String HSC_PREVIEW_URL_PATH = "/shot/HSC/";


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
     * @param uri       The URI of the given artifact.
     * @return URL      The converted URL.
     * @throws IllegalArgumentException if the scheme is not equal to the value from getScheme()
     *                                  the uri is malformed such that a URL cannot be generated, or the uri is null
     */
    private URL createURLFromPath(final URI uri) {
        final String[] path = uri.getSchemeSpecificPart().split("/");
        // data uri has 3 parts, preview can have 2 (pre-HSC format) or 3
        if (path.length < 2 || path.length > 3) {
            throw new IllegalArgumentException("Malformed URI. Expected 2 or 3 path components, found " + path.length);
        }

        String sb = "";
        final String requestType = path[0];

        if (requestType.equals(PREVIEW_URI)) {
            String frameid = "";
            // TODO: when support for 2-part URI is removed, this code can be simplified.
            if (path.length == 3) {
                frameid = path[2];
            } else {
                frameid = path[1];
            }

            // Returns a web page reference
            // expected URI: subaru:preview/<FRAMEID> or subaru:/preview/<date>/<FRAMEID>

            // URIs for HSC and Suprime-cam resolve to different URLs
            // First three characters of FRAMEID are checked, looking for 'HSC'
            if (frameid.matches("HSC\\w*")) {
                // Last 2 characters of frameid in this case is not used in the URL
                // Chop and go.
                sb = PREVIEW_BASE_URL + HSC_PREVIEW_URL_PATH + path[1] + "/"
                    + frameid.substring(0,frameid.length() - 2) + ".png";
            } else {
                // Assume is Suprime-cam
                // To support older preview URI formats, the length of URI is checked
                // TODO: when support for 2-part preview URI is removed, this
                // check should be removed.
                sb = PREVIEW_BASE_URL + PREVIEW_URL_PATH + "?" + PREVIEW_URL_QUERY
                    + ((path.length == 3) ? NetUtil.encode(path[2] + " " + path[1]) : path[1]);
            }


        } else if (path.length == 3 && requestType.equals(RAW_DATA_URI)) {
            // expected URI input is subaru:raw/YYYY-MM-dd/<FRAMEID>
            // expected URL output is http://www.cadc-ccda.hia-iha.nrc-cnrc.gc
            // .ca/maq/subaru?frameinfo=YYYY-MM-dd/FRAMEID
            sb = BASE_DATA_URL + NetUtil.encode(path[1] + "/" + path[2]);
        } else {
            throw new IllegalArgumentException("Invalid URI: " + requestType);
        }

        try {
            final URL newUrl = new URL(sb);
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

