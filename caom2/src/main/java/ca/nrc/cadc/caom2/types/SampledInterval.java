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

package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.util.CaomValidator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pdowler
 */
public class SampledInterval implements Serializable {
    private static final long serialVersionUID = 201708241230L;

    private double lower;
    private double upper;
    private List<Interval> samples = new ArrayList<Interval>();

    public static final String[] CTOR_UTYPES = { "lower", "upper" };

    private SampledInterval() {
    }

    public SampledInterval(double lower, double upper) {

        this.lower = lower;
        this.upper = upper;
    }

    public SampledInterval(double lower, double upper, List<Interval> samples) {
        this.lower = lower;
        this.upper = upper;
        CaomValidator.assertNotNull(SampledInterval.class, "samples", samples);
        this.samples.addAll(samples);
        validate();
    }

    public final void validate() {
        if (upper < lower) {
            throw new IllegalArgumentException(
                    "invalid interval (upper < lower): " + lower + "," + upper);
        }
        CaomValidator.assertNotNull(SampledInterval.class, "samples", samples);
        if (samples.isEmpty()) {
            throw new IllegalArgumentException(
                    "invalid interval (samples cannot be empty)");
        }

        Interval prev = null;
        for (Interval si : samples) {
            if (si.getLower() < lower) {
                throw new IllegalArgumentException(
                        "invalid interval: sample extends below lower bound: "
                                + si + " vs " + lower);
            }
            if (si.getUpper() > upper) {
                throw new IllegalArgumentException(
                        "invalid interval: sample extends above upper bound: "
                                + si + " vs " + upper);
            }

            if (prev != null) {
                if (si.getLower() <= prev.getUpper()) {
                    throw new IllegalArgumentException(
                            "invalid interval: sample overlaps previous sample: "
                                    + si + " vs " + prev);
                }
            }
            prev = si;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Interval[").append(lower).append(",").append(upper);
        if (!samples.isEmpty()) {
            sb.append(" samples[ ");
            for (Interval si : samples) {
                sb.append("[").append(si.lower).append(",").append(si.upper)
                        .append("] ");
            }
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    public double getLower() {
        return lower;
    }

    public double getUpper() {
        return upper;
    }

    public List<Interval> getSamples() {
        return samples;
    }

    public double getWidth() {
        return (upper - lower);
    }

    public static SampledInterval intersection(SampledInterval i1, SampledInterval i2) {
        if (i1.lower > i2.upper || i1.upper < i2.lower) {
            return null; // no overlap
        }

        double lb = Math.max(i1.lower, i2.lower);
        double ub = Math.min(i1.upper, i2.upper);
        return new SampledInterval(lb, ub);
    }
}
