/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2.util;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.dali.InvalidPolygonException;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.util.HexUtil;
import java.net.URI;
import java.util.Collection;
import java.util.Set;

/**
 *
 * @author pdowler
 */
public final class CaomValidator {
    private CaomValidator() {
    }

    /**
     * Perform all validation of the content of an observation.
     * 
     * @param obs
     * @throws IllegalArgumentException
     * @throws InvalidPolygonException
     */
    public static void validate(Observation obs) throws InvalidPolygonException {
        validateKeywords(obs);
        validatePlanes(obs);
        validateChecksumURIs(obs);
    }
    
    /**
     * Utility method so constructors can validate arguments.
     * 
     * @param caller
     * @param name
     * @param test
     * @throws IllegalArgumentException 
     */
    public static void assertNotNull(Class caller, String name, Object test)
            throws IllegalArgumentException {
        if (test == null) {
            throw new IllegalArgumentException(caller.getSimpleName() + ": null " + name);
        }
    }
    
    /**
     * Utility method so constructors can validate arguments.
     * 
     * @param caller
     * @param name
     * @param test
     * @throws IllegalArgumentException 
     */
    public static void assertNotEmpty(Class caller, String name, String test)
            throws IllegalArgumentException {
        assertNotNull(caller, name, test);
        if (test.isEmpty()) {
            throw new IllegalArgumentException(caller.getSimpleName() + ": zero-length string value in " + name);
        }
    }

    /**
     * Utility method so constructors can validate arguments.
     * 
     * @param caller
     * @param name
     * @param test
     * @throws IllegalArgumentException 
     */
    public static void assertNotEmpty(Class caller, String name, Collection test)
            throws IllegalArgumentException {
        assertNotNull(caller, name, test);
        if (test.isEmpty()) {
            throw new IllegalArgumentException(caller.getSimpleName() + ": empty collection in " + name);
        }
    }

    /**
     * Keywords can contain any valid UTF-8 character except the pipe (|). The
     * pipe character is reserved for use as a separator in persistence
     * implementations so the list of keywords can be serialized in a single
     * string to support querying.
     * 
     * @param caller
     * @param name
     * @param val
     * @throws IllegalArgumentException 
     */
    public static void assertValidKeyword(Class caller, String name,
            String val) {
        assertNotNull(caller, name, val);
        boolean pipe = (val.indexOf('|') >= 0);
        if (!pipe) {
            return;
        }
        throw new IllegalArgumentException(caller.getSimpleName() + ": invalid "
                + name + ": " + val + "-- keyword not contain pipe (|)");
    }

    /**
     * A valid path component has no space ( ), slash (/), escape (\), or
     * percent (%) characters.
     * 
     * @param caller
     * @param name
     * @param test
     * @throws IllegalArgumentException 
     */
    public static void assertValidPathComponent(Class caller, String name,
            String test) {
        assertNotNull(caller, name, test);
        boolean space = (test.indexOf(' ') >= 0);
        boolean slash = (test.indexOf('/') >= 0);
        boolean escape = (test.indexOf('\\') >= 0);
        boolean percent = (test.indexOf('%') >= 0);

        if (!space && !slash && !escape && !percent) {
            return;
        }
        throw new IllegalArgumentException(caller.getSimpleName() + ": invalid "
                + name + ": " + test
                + " -- value may not contain space ( ), slash (/), escape (\\), or percent (%)");
    }

    /**
     * Numeric validation.
     * 
     * @param caller
     * @param name
     * @param test 
     * @throws IllegalArgumentException 
     */
    public static void assertPositive(Class caller, String name, double test) {
        if (test <= 0.0) {
            throw new IllegalArgumentException(
                    caller.getSimpleName() + ": " + name + " must be > 0.0");
        }
    }
    
    /**
     * Numeric validation.
     * 
     * @param caller
     * @param name
     * @param test 
     * @throws IllegalArgumentException 
     */
    public static void assertPositive(Class caller, String name, long test) {
        if (test <= 0L) {
            throw new IllegalArgumentException(
                    caller.getSimpleName() + ": " + name + " must be > 0.0");
        }
    }
    
    public static void assertValidIdentifier(Class caller, String name, URI uri) {
        String scheme = uri.getScheme();

        if ("caom".equals(scheme)) {
            assertValidCaomURI(caller, name, uri);
            return;
        }
        if ("ivo".equals(scheme)) {
            assertValidIvorn(caller, name, uri);
            return;
        }

        throw new IllegalArgumentException(
            caller.getSimpleName() + ": " + name + " scheme must be caom|ivo: " + uri.toASCIIString());
    }

    private static void assertValidCaomURI(Class caller, String name, URI uri) {
        String auth = uri.getAuthority();
        if (auth != null) {
            throw new IllegalArgumentException(
                caller.getSimpleName() + ": " + name + " caom scheme does not allow authority: " 
                        + uri.toASCIIString());
        }
        String ssp = uri.getSchemeSpecificPart();
        if (ssp == null) {
            throw new IllegalArgumentException(
                caller.getSimpleName() + ": " + name + " caom scheme requires scheme-specific part: " 
                        + uri.toASCIIString());
        }
        String[] cop = ssp.split("/");
        if (cop.length > 3) {
            throw new IllegalArgumentException(
                caller.getSimpleName() + ": " + name + " caom scheme requires 2-3 path components in scheme-specific part: " 
                        + uri.toASCIIString());
        }
        if (cop.length > 1) {
            String collection = cop[0];
            String observationID = cop[1];
            CaomValidator.assertNotNull(caller, "caom: {collection}", collection);
            CaomValidator.assertValidPathComponent(caller, "caom: {collection}", collection);
            CaomValidator.assertNotNull(caller, "caom: {observationID}", observationID);
            CaomValidator.assertValidPathComponent(caller, "caom: {observationID}", observationID);
        }
        if (cop.length == 3) {
            String productID = cop[2];
            CaomValidator.assertNotNull(caller, "caom: {productID}", productID);
            CaomValidator.assertValidPathComponent(caller, "caom: {productID}", productID);
        }
    }

    private static void assertValidIvorn(Class caller, String name, URI uri) {
        String path = uri.getPath();
        String query = uri.getQuery();
        if (path == null) {
            throw new IllegalArgumentException(
                caller.getSimpleName() + ": " + name + " ivo scheme requires a path: " 
                        + uri.toASCIIString());
        }
        if (query == null) {
            throw new IllegalArgumentException(
                caller.getSimpleName() + ": " + name + " ivo scheme requires a query string: " 
                        + uri.toASCIIString());
        }
        String[] cop = query.split("/");
        for (String pc : cop) {
            CaomValidator.assertValidPathComponent(caller, "ivo: path components in query", pc);
        }
    }

    /**
     * Checksum URI validation.
     * 
     * @param uri 
     * @throws IllegalArgumentException 
     */
    public static void assertValidChecksumURI(URI uri) {
        String scheme = uri.getScheme();
        String sval = uri.getSchemeSpecificPart();
        if (scheme == null || sval == null) {
            throw new IllegalArgumentException("invalid Artifact.contentChecksum: " 
                + uri + " -- expected <algorithm>:<hex value>");
        }
        try {
            byte[] b = HexUtil.toBytes(sval);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("invalid Artifact.contentChecksum: " 
                + uri + " contains invalid hex chars -- expected <algorithm>:<hex value>");
        }
    }
    
    private static void validateKeywords(String name, Set<String> vals) {
        for (String s : vals) {
            assertValidKeyword(CaomValidator.class, name, s);
        }
    }

    // validate all keywords fields
    private static void validateKeywords(Observation obs) {
        if (obs.proposal != null) {
            validateKeywords("proposal.keywords", obs.proposal.getKeywords());
        }
        if (obs.target != null) {
            validateKeywords("target.keywords", obs.target.getKeywords());
        }
        if (obs.telescope != null) {
            validateKeywords("telescope.keywords", obs.telescope.getKeywords());
        }
        if (obs.instrument != null) {
            validateKeywords("instrument.keywords",
                    obs.instrument.getKeywords());
        }

        for (Plane p : obs.getPlanes()) {
            if (p.provenance != null) {
                validateKeywords("provenance.keywords",
                        p.provenance.getKeywords());
            }
        }
    }

    private static void validatePlanes(Observation obs) throws InvalidPolygonException {
        for (Plane p : obs.getPlanes()) {
            if (p.position != null) {
                p.position.validate();
            }
            if (p.energy != null) {
                p.energy.validate();
            }
            if (p.time != null) {
                p.time.validate();
            }
            if (p.polarization != null) {
                p.polarization.validate();
            }
            if (p.custom != null) {
                p.custom.validate();
            }
            if (p.visibility != null) {
                p.visibility.validate();
            }
            if (p.observable != null) {
                p.observable.validate();
            }
        }
    }
    
    private static void validateChecksumURIs(Observation obs) {
        for (Plane p : obs.getPlanes()) {
            for (Artifact a : p.getArtifacts()) {
                if (a.contentChecksum != null) {
                    assertValidChecksumURI(a.contentChecksum);
                }
            }
        }
    }
}
