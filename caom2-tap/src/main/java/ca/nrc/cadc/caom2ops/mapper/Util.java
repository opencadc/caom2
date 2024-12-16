/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.caom2.util.CaomUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author yeunga
 */
public class Util extends CaomUtil {

    public static Object getObject(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        return data.get(col);
    }

    public static String getString(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (String) o;
    }

    public static URI getURI(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (URI) o;
    }
    
    public static List<URI> getURIList(List<Object> data, Integer col)
            throws URISyntaxException {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        String s = (String) o;
        String[] ss = s.split(" ");
        List<URI> ret = new ArrayList<>();
        for (String u : ss) {
            ret.add(new URI(u));
        }
        return ret;
    }

    public static Boolean getBoolean(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        // TAP-specific hack
        if (o instanceof Integer) {
            Integer i = (Integer) o;
            if (i == 1) {
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }
        return (Boolean) o;
    }

    public static UUID getUUID(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (UUID) o;
    }

    public static Long getLong(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (Long) o;
    }

    public static Integer getInteger(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (Integer) o;
    }

    public static Float getFloat(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (Float) o;
    }

    public static Double getDouble(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (Double) o;
    }

    public static List<Double> getDoubleList(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        double[] vals = (double[]) o;
        List<Double> ret = new ArrayList<Double>(vals.length);
        for (double d : vals) {
            ret.add(d);
        }
        return ret;
    }

    public static Date getDate(List<Object> data, Integer col) {
        if (col == null) {
            return null;
        }
        Object o = data.get(col);
        if (o == null) {
            return null;
        }
        return (Date) o;
    }
}
