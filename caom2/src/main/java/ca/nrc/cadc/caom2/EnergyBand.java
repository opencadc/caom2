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

import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.util.EnergyConverter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pdowler
 */
public enum EnergyBand implements CaomEnum<String> {
    RADIO("Radio"), MILLIMETER("Millimeter"), INFRARED("Infrared"), OPTICAL(
            "Optical"), UV(
                    "UV"), EUV("EUV"), XRAY("X-ray"), GAMMARAY("Gamma-ray");

    private String value;

    private EnergyBand(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public int checksum() {
        return value.hashCode();
    }

    public static EnergyBand toValue(String s) {
        for (EnergyBand eb : values()) {
            if (eb.value.equals(s)) {
                return eb;
            }
        }
        throw new IllegalArgumentException("invalid value: " + s);
    }

    /**
     * Compute the EnergyBand from the wavelength coverage. This finds the band
     * that overlaps the largest fraction of the specified interval; the current
     * implementation ignores the sub-intervals.
     *
     * @param bounds
     * @return
     */
    public static EnergyBand getEnergyBand(Interval bounds) {
        if (bounds == null) {
            return null;
        }

        double frac = 0.0;
        EnergyBandWrapper eb = null;
        for (EnergyBandWrapper b : energyBands) {
            double f = getOverlapFraction(b, bounds);
            if (f > frac) {
                frac = f;
                eb = b;
            }
        }
        if (eb == null) {
            return null;
        }
        return eb.band;
    }

    @Override
    public String toString() {
        return "EnergyBand[" + value + "]";
    }

    private static final List<EnergyBandWrapper> energyBands = new ArrayList<EnergyBandWrapper>();

    static {
        EnergyConverter ec = new EnergyConverter();
        // radio: freq < 30 GHz or wave > 10mm
        energyBands.add(new EnergyBandWrapper(EnergyBand.RADIO,
                ec.convert(10.0, "WAVE", "mm"), Double.MAX_VALUE));

        // millimeter: 0.1-10 mm
        energyBands.add(new EnergyBandWrapper(EnergyBand.MILLIMETER,
                ec.convert(0.1, "WAVE", "mm"), ec.convert(10.0, "WAVE", "mm")));

        // infrared: 1-100 um
        energyBands.add(new EnergyBandWrapper(EnergyBand.INFRARED,
                ec.convert(1.0, "WAVE", "um"),
                ec.convert(100.0, "WAVE", "um")));

        // optical: 300-1000 nm
        energyBands.add(new EnergyBandWrapper(EnergyBand.OPTICAL,
                ec.convert(300.0, "WAVE", "nm"),
                ec.convert(1000.0, "WAVE", "nm")));

        // uv: 100-300 nm
        energyBands.add(new EnergyBandWrapper(EnergyBand.UV,
                ec.convert(100.0, "WAVE", "nm"),
                ec.convert(300.0, "WAVE", "nm")));

        // euv: 10-100 nm
        energyBands.add(new EnergyBandWrapper(EnergyBand.EUV,
                ec.convert(10.0, "WAVE", "nm"),
                ec.convert(100.0, "WAVE", "nm")));

        // xray: 0.12-120 keV
        energyBands.add(new EnergyBandWrapper(EnergyBand.XRAY,
                ec.convert(0.12, "ENER", "keV"),
                ec.convert(120.0, "ENER", "keV")));

        // gamma: 120-1e6 keV
        energyBands.add(new EnergyBandWrapper(EnergyBand.GAMMARAY,
                ec.convert(120.0, "ENER", "keV"),
                ec.convert(1.0e6, "ENER", "keV")));
    }

    private static final class EnergyBandWrapper implements Serializable {
        private static final long serialVersionUID = 201207191400L;
        EnergyBand band;
        double lb;
        double ub;

        EnergyBandWrapper(EnergyBand band, double a, double b) {
            this.band = band;
            if (a < b) {
                lb = a;
                ub = b;
            } else {
                lb = b;
                ub = a;
            }
        }
    }

    // fraction of e that overlaps b
    private static double getOverlapFraction(EnergyBandWrapper b, Interval ei) {
        // no overlap
        if (b.ub < ei.getLower() || ei.getUpper() < b.lb) {
            return 0.0;
        }

        // partial overlap below
        if (ei.getLower() < b.lb) {
            return (ei.getUpper() - b.lb) / ei.getWidth();
        }

        // partial overlap above
        if (b.ub < ei.getUpper()) {
            return (b.ub - ei.getLower()) / ei.getWidth();
        }

        // contained
        return 1.0;
    }
}
