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

package ca.nrc.cadc.caom2;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * An extensible vocabulary masquerading as an enumeration, or the other way
 * around.
 * 
 * @author pdowler
 */
public class DataProductType extends VocabularyTerm implements CaomEnum<String> {
    private static final URI OBSCORE = URI.create("http://www.ivoa.net/std/ObsCore");
    private static final URI CAOM = URI.create("http://www.opencadc.org/caom2/DataProductType");

    /**
     * ObsCore-1.0 image.
     */
    public static final DataProductType IMAGE = new DataProductType("image");
    
    /**
     * ObsCore-1.0 spectrum.
     */
    public static final DataProductType SPECTRUM = new DataProductType("spectrum");
    
    /**
     * ObsCore-1.0 timeseries.
     */
    public static final DataProductType TIMESERIES = new DataProductType("timeseries");
    
    /**
     * ObsCore-1.0 visibility.
     */
    public static final DataProductType VISIBILITY = new DataProductType("visibility");
    
    /**
     * ObsCore-1.0 event.
     */
    public static final DataProductType EVENT = new DataProductType("event");
    
    /**
     * Incorrect value from ObsCore WD that was actually used.
     */
    @Deprecated
    private static final DataProductType EVENTLIST = new DataProductType("eventlist");
    
    /**
     * ObsCore-1.0 cube.
     */
    public static final DataProductType CUBE = new DataProductType("cube");
    

    /**
     * ObsCore-1.1 measurements.
     */
    public static final DataProductType MEASUREMENTS = new DataProductType("measurements");
    
    /**
     * ObsCore-1.1 sed.
     */
    public static final DataProductType SED = new DataProductType("sed");

    /**
     * CAOM catalog extension. Catalog is a subclass of measurements.
     */
    public static final DataProductType CATALOG = new DataProductType(CAOM, "catalog");

    public static final DataProductType[] values() {
        return new DataProductType[] { IMAGE, SPECTRUM, TIMESERIES, VISIBILITY,
                                       CUBE, SED, MEASUREMENTS, CATALOG, EVENT };
    }

    private DataProductType(String value) {
        super(OBSCORE, value, true);
    }

    private DataProductType(URI namespace, String value) {
        super(namespace, value);
    }

    public static DataProductType toValue(String s) {
        for (DataProductType d : values()) {
            if (d.getValue().equals(s)) {
                return d;
            }
        }

        // backwards compat
        if (EVENTLIST.getValue().equals(s)) {
            return EVENT; // correct term
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
            return new DataProductType(ns, t);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("invalid value: " + s, ex);
        }
    }
}
