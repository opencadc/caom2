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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author pdowler
 */
public class ProductType extends VocabularyTerm
        implements CaomEnum<String>, Serializable {
    private static final long serialVersionUID = 2017040200800L;

    private static final URI CAOM = URI.create("http://www.opencadc.org/caom2/ProductType");

    // IVOA DataLink terms
    public static ProductType THIS = new ProductType("this");

    public static ProductType AUXILIARY = new ProductType("auxiliary");
    public static ProductType BIAS = new ProductType("bias");
    public static ProductType CALIBRATION = new ProductType("calibration");
    public static ProductType CODERIVED = new ProductType("coderived");
    public static ProductType COUNTERPART = new ProductType("counterpart");
    public static ProductType DARK = new ProductType("dark");
    public static ProductType ERROR = new ProductType("error");
    public static ProductType FLAT = new ProductType("flat");
    public static ProductType NOISE = new ProductType("noise");
    public static ProductType PREVIEW = new ProductType("preview");
    public static ProductType PREVIEW_IMAGE = new ProductType("preview-image");
    public static ProductType PREVIEW_PLOT = new ProductType("preview-plot");
    public static ProductType THUMBNAIL = new ProductType("thumbnail");
    public static ProductType WEIGHT = new ProductType("weight");
    
    // DataLink terms explicitly not included
    // cutout
    // derivation
    // progenitor
    // detached-header
    // package
    
    // CAOM specific terms
    public static ProductType SCIENCE = new ProductType("science");
    public static ProductType INFO = new ProductType("info");
    
    
    /**
     * @deprecated
     */
    public static ProductType CATALOG = new ProductType("catalog");

    public static final ProductType[] values() {
        return new ProductType[] { SCIENCE, CALIBRATION, AUXILIARY, INFO,
                                   PREVIEW, CATALOG, NOISE, WEIGHT, 
                                   THUMBNAIL, BIAS, DARK, FLAT };
    }

    private ProductType(String term) {
        super(CAOM, term, true);
    }

    protected ProductType(URI namespace, String term) {
        super(namespace, term, false);
    }

    public static ProductType toValue(String s) {
        for (ProductType d : values()) {
            if (d.getValue().equals(s)) {
                return d;
            }
        }

        // custom term
        try {
            URI u = new URI(s);
            String t = u.getFragment();
            if (t == null) {
                throw new IllegalArgumentException(
                        "invalid value (no term/fragment): " + s);
            }
            String[] ss = u.toASCIIString().split("#");
            URI ns = new URI(ss[0]);
            return new ProductType(ns, t);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("invalid value: " + s, ex);
        }
    }
}
