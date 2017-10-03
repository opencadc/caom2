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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.util.CaomValidator;
import java.io.Serializable;
import java.net.URI;

/**
 *
 * @author pdowler
 */
public class PlaneURI implements Comparable<PlaneURI>, Serializable {
    private static final long serialVersionUID = 201202091030L;

    private URI uri;

    private PlaneURI() {
    }

    public PlaneURI(URI uri) {
        if (!ObservationURI.SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "invalid scheme: " + uri.getScheme());
        }
        String ssp = uri.getSchemeSpecificPart();
        CaomValidator.assertNotNull(getClass(), "scheme-specific-part", ssp);
        String[] cop = ssp.split("/");
        if (cop.length == 3) {
            String collection = cop[0];
            String observationID = cop[1];
            String productID = cop[2];
            CaomValidator.assertNotNull(getClass(), "collection", collection);
            CaomValidator.assertValidPathComponent(getClass(), "collection",
                    collection);
            CaomValidator.assertNotNull(getClass(), "observationID",
                    observationID);
            CaomValidator.assertValidPathComponent(getClass(), "observationID",
                    observationID);
            CaomValidator.assertNotNull(getClass(), "productID", productID);
            CaomValidator.assertValidPathComponent(getClass(), "productID",
                    productID);
            this.uri = URI.create(ObservationURI.SCHEME + ":" + collection + "/"
                    + observationID + "/" + productID);
        } else {
            throw new IllegalArgumentException("input URI has " + cop.length
                    + " parts (" + ssp
                    + "), expected 3: caom:<collection>/<observationID>/<productID>");
        }
    }

    public PlaneURI(ObservationURI parent, String productID) {
        CaomValidator.assertNotNull(getClass(), "parent", parent);
        CaomValidator.assertNotNull(getClass(), "productID", productID);
        CaomValidator.assertValidPathComponent(getClass(), "productID",
                productID);
        this.uri = URI
                .create(parent.getURI().toASCIIString() + "/" + productID);

    }

    @Override
    public String toString() {
        return getURI().toASCIIString();
    }

    public URI getURI() {
        return uri;
    }

    public ObservationURI getParent() {
        String collection = uri.getSchemeSpecificPart().split("/")[0];
        String observationID = uri.getSchemeSpecificPart().split("/")[1];
        return new ObservationURI(collection, observationID);
    }

    public String getProductID() {
        return uri.getSchemeSpecificPart().split("/")[2];
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof PlaneURI) {
            PlaneURI u = (PlaneURI) o;
            return (this.hashCode() == u.hashCode());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public int compareTo(PlaneURI u) {
        return this.uri.compareTo(u.uri);
    }
}
