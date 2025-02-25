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

package org.opencadc.caom2.wcs;

import org.opencadc.caom2.util.CaomValidator;
import java.io.Serializable;

/**
 *
 * @author pdowler
 */
public class CoordFunction2D implements Serializable {
    private static final long serialVersionUID = 201202091500L;

    // immutable state
    private double cd11;
    private double cd12;
    private double cd21;
    private double cd22;
    private Dimension2D dimension;
    private Coord2D refCoord;

    public static final String[] CTOR_UTYPES = { "dimension", "refCoord",
                                                 "cd11", "cd12", "cd21", "cd22" };

    public CoordFunction2D(Dimension2D dimension, Coord2D refCoord, double cd11,
            double cd12, double cd21, double cd22) {
        CaomValidator.assertNotNull(getClass(), "dimension", dimension);
        CaomValidator.assertNotNull(getClass(), "refCoord", refCoord);
        this.dimension = dimension;
        this.refCoord = refCoord;
        this.cd11 = cd11;
        this.cd12 = cd12;
        this.cd21 = cd21;
        this.cd22 = cd22;
    }

    public Dimension2D getDimension() {
        return dimension;
    }

    public Coord2D getRefCoord() {
        return refCoord;
    }

    public double getCd11() {
        return cd11;
    }

    public double getCd12() {
        return cd12;
    }

    public double getCd21() {
        return cd21;
    }

    public double getCd22() {
        return cd22;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append(dimension.naxis1).append(",");
        sb.append(dimension.naxis2).append(",");
        sb.append(refCoord.getCoord1().pix).append(",");
        sb.append(refCoord.getCoord2().pix).append(",");
        sb.append(refCoord.getCoord1().val).append(",");
        sb.append(refCoord.getCoord2().val).append(",");
        sb.append(cd11).append(",");
        sb.append(cd12).append(",");
        sb.append(cd21).append(",");
        sb.append(cd22).append("]");
        return sb.toString();
    }
}
