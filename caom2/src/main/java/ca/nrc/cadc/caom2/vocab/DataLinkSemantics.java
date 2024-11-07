/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package ca.nrc.cadc.caom2.vocab;

import ca.nrc.cadc.caom2.CaomEnum;
import ca.nrc.cadc.caom2.VocabularyTerm;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author pdowler
 */
public class DataLinkSemantics extends VocabularyTerm
        implements CaomEnum<String>, Serializable {
    private static final long serialVersionUID = 2017040200800L;

    //private static final URI CAOM = URI.create("http://www.opencadc.org/caom2/ProductType");
    private static final URI DATALINK_NS = URI.create("http://www.ivoa.net/rdf/datalink/core");
    
    // IVOA DataLink terms
    public static DataLinkSemantics THIS = new DataLinkSemantics("this");

    public static DataLinkSemantics AUXILIARY = new DataLinkSemantics("auxiliary");
    public static DataLinkSemantics BIAS = new DataLinkSemantics("bias");
    public static DataLinkSemantics CALIBRATION = new DataLinkSemantics("calibration");
    public static DataLinkSemantics CODERIVED = new DataLinkSemantics("coderived");
    public static DataLinkSemantics DARK = new DataLinkSemantics("dark");
    public static DataLinkSemantics DOCUMENTATION = new DataLinkSemantics("documentation");
    public static DataLinkSemantics ERROR = new DataLinkSemantics("error");
    public static DataLinkSemantics FLAT = new DataLinkSemantics("flat");
    public static DataLinkSemantics NOISE = new DataLinkSemantics("noise");
    public static DataLinkSemantics PREVIEW = new DataLinkSemantics("preview");
    public static DataLinkSemantics PREVIEW_IMAGE = new DataLinkSemantics("preview-image");
    public static DataLinkSemantics PREVIEW_PLOT = new DataLinkSemantics("preview-plot");
    public static DataLinkSemantics THUMBNAIL = new DataLinkSemantics("thumbnail");
    public static DataLinkSemantics WEIGHT = new DataLinkSemantics("weight");
    
    // DataLink terms explicitly not included because they are not applicable to Artifact
    // counterpart
    // cutout
    // derivation
    // detached-header
    // package
    // proc
    // progenitor
    
    private static final DataLinkSemantics[] VALUES = new DataLinkSemantics[] {
        THIS, AUXILIARY, BIAS, CALIBRATION,
        CODERIVED, DARK, DOCUMENTATION, ERROR, FLAT, NOISE, 
        PREVIEW, PREVIEW_IMAGE, PREVIEW_PLOT,
        THUMBNAIL, WEIGHT
    };

    public static final DataLinkSemantics[] values() {
        return VALUES;
    }

    private DataLinkSemantics(String term) {
        super(DATALINK_NS, term, true);
    }

    protected DataLinkSemantics(URI namespace, String term) {
        super(namespace, term, false);
    }

    public static DataLinkSemantics toValue(String s) {
        for (DataLinkSemantics d : VALUES) {
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
            return new DataLinkSemantics(ns, t);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("invalid value: " + s, ex);
        }
    }
}
