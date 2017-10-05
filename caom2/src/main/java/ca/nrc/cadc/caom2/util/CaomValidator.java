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
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.types.Polygon;
import java.util.Set;

/**
 *
 * @author pdowler
 */
public final class CaomValidator {
    private CaomValidator() {
    }

    public static void assertNotNull(Class caller, String name, Object test) throws IllegalArgumentException {
        if (test == null) {
            throw new IllegalArgumentException(caller.getSimpleName() + ": null " + name);
        }
    }

    /**
     * Keywords can contain any valid UTF-8 character except the pipe (|). The pipe character is reserved for use as a separator in persistence implementations
     * so the list of keywords can be serialized in a single string to support querying.
     * 
     * @param caller
     * @param name
     * @param val
     */
    public static void assertValidKeyword(Class caller, String name, String val) {
        assertNotNull(caller, name, val);
        boolean pipe = (val.indexOf('|') >= 0);
        if (!pipe) {
            return;
        }
        throw new IllegalArgumentException(caller.getSimpleName() + ": invalid " + name + ": may not contain pipe (|)");
    }

    /**
     * A valid path component has no space ( ), slash (/), escape (\), or percent (%) characters.
     * 
     * @param caller
     * @param name
     * @param test
     */
    public static void assertValidPathComponent(Class caller, String name, String test) {
        assertNotNull(caller, name, test);
        boolean space = (test.indexOf(' ') >= 0);
        boolean slash = (test.indexOf('/') >= 0);
        boolean escape = (test.indexOf('\\') >= 0);
        boolean percent = (test.indexOf('%') >= 0);

        if (!space && !slash && !escape && !percent) {
            return;
        }
        throw new IllegalArgumentException(
                caller.getSimpleName() + ": invalid " + name + ": may not contain space ( ), slash (/), escape (\\), or percent (%)");
    }

    public static void assertPositive(Class caller, String name, double test) {
        if (test <= 0.0) {
            throw new IllegalArgumentException(caller.getSimpleName() + ": " + name + " must be > 0.0");
        }
    }

    private static void validateKeywords(String name, Set<String> vals) {
        for (String s : vals) {
            assertValidKeyword(CaomValidator.class, name, s);
        }
    }

    /**
     * Validate the keywords fields and make sure they don't contain invalid characters (currently space and single-quote).
     * 
     * @param obs
     */
    public static void validateKeywords(Observation obs) {
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
            validateKeywords("instrument.keywords", obs.instrument.getKeywords());
        }

        for (Plane p : obs.getPlanes()) {
            if (p.provenance != null) {
                validateKeywords("provenance.keywords", p.provenance.getKeywords());
            }
        }
    }

    /**
     * Validate Artifact.productType for consistency with Observation.intent. Observations with intent=science have no artifacts with productType=calibration.
     * Observations with intent=calibration have no artifacts with productType=science.
     * 
     * @param obs
     */
    public static void validateIntent(Observation obs) {
        if (obs.intent == null) {
            return;
        }

        ProductType ban = ProductType.CALIBRATION;
        if (ObservationIntentType.CALIBRATION.equals(obs.intent)) {
            ban = ProductType.SCIENCE;
        }
        for (Plane p : obs.getPlanes()) {
            for (Artifact a : p.getArtifacts()) {
                if (ban.equals(a.getProductType())) {
                    throw new IllegalArgumentException(
                            "Observation.intent = " + obs.intent + " but artifact " + a.getURI().toASCIIString() + " has productType = " + a.getProductType());
                }
            }
        }
    }

    /**
     * Validate Plane.position.bounds, Plane.energy,bounds, and Plane.time.bounds for valid polygon and interval respectively.
     * 
     * @param obs
     */
    public static void validatePlanes(Observation obs) {
        for (Plane p : obs.getPlanes()) {
            if (p.position != null && p.position.bounds != null && p.position.bounds instanceof Polygon) {
                Polygon poly = (Polygon) p.position.bounds;
                poly.validate();
            }
            if (p.energy != null && p.energy.bounds != null) {
                p.energy.bounds.validate();
            }
            if (p.time != null && p.time.bounds != null) {
                p.time.bounds.validate();
            }
        }
    }

    /**
     * Perform all validation of the content of an observation.
     * 
     * @param obs
     */
    public static void validate(Observation obs) {
        validateKeywords(obs);

        validateIntent(obs);

        validatePlanes(obs);
    }
}
