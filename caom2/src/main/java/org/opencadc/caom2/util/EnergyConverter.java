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

package org.opencadc.caom2.util;

import ca.nrc.cadc.util.ArrayUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert wavelength, frequency, and energy to wavelength in meters.
 *
 * @version $Version$
 * @author pdowler
 */
public class EnergyConverter {
    private static final String[] allUnits;

    private static final String[] freqUnits = new String[] { "Hz", "kHz", "MHz", "GHz" };
    private static final double[] freqMult = new double[] { 1.0, 1.0e3, 1.0e6, 1.0e9 };

    private static final String[] enUnits = new String[] { "eV", "keV", "MeV", "GeV" };
    private static final double[] enMult = new double[] { 1.0, 1.0e3, 1.0e6, 1.0e9 };

    private static final String[] waveUnits = new String[] { "m", "cm", "mm",
                                                             "um", "µm", "nm", "Angstrom", "A" }; // A  deprecated
    private static final double[] waveMult = new double[] { 1.0, 1.0e-2, 1.0e-3,
                                                            1.0e-6, 1.0e-6, 1.0e-9, 1.0e-10, 1.0e-10, };

    // Lay out the actual units only once, then coalesce them.
    static {
        final List<String> allUnitList = new ArrayList<String>(
                Arrays.asList(freqUnits));
        allUnitList.addAll(Arrays.asList(enUnits));
        allUnitList.addAll(Arrays.asList(waveUnits));

        allUnits = allUnitList.toArray(new String[allUnitList.size()]);
    }

    public static final String CORE_SPECSYS = "BARYCENT";
    public static final String CORE_CTYPE = "WAVE";
    public static final String CORE_UNIT = "m";

    public static final String BASE_UNIT_FREQ = "Hz";

    private static final double c = 2.9979250e8; // m/sec
    private static final double h = 6.62620e-27; // erg/sec
    private static final double eV = 1.602192e-12; // erg

    public String[] getSupportedUnits() {
        return allUnits;
    }

    /**
     * Convert the supplied value/units to a value expressed in core energy
     * units.
     */
    public double convert(double value, String ctype, String cunit) {
        // TODO: check ctype instead of just relying on units
        return toMeters(value, cunit);
    }

    /*
     * public double convert(double value, String ctype, String fromUnit, String
     * toUnit) { double valueM = toMeters(value, fromUnit);
     * 
     * int i = ArrayUtil.matches("^" + toUnit + "$", freqUnits, true); if ( i !=
     * -1 ) return waveToFreq(valueM, i);
     * 
     * i = ArrayUtil.matches("^" + toUnit + "$", enUnits, true); if ( i != -1 )
     * return waveToEnergy(valueM, i);
     * 
     * i = ArrayUtil.matches("^" + toUnit + "$", waveUnits, true); if (i != -1)
     * return waveToWave(valueM, i);
     * 
     * throw new IllegalArgumentException("unknown units: " + toUnit); }
     */

    public double convertSpecsys(double value, String specsys) {
        return value; // no-op
    }

    /**
     * Convert the energy value d from the specified units to wavelength in
     * meters.
     * 
     * @param d
     * @param units
     * @return wavelength in meters
     */
    public double toMeters(double d, String units) {
        int i = ArrayUtil.matches("^" + units + "$", freqUnits, true);
        if (i != -1) {
            return freqToMeters(d, i);
        }

        i = ArrayUtil.matches("^" + units + "$", enUnits, true);
        if (i != -1) {
            return energyToMeters(d, i);
        }

        i = ArrayUtil.matches("^" + units + "$", waveUnits, true);
        if (i != -1) {
            return wavelengthToMeters(d, i);
        }

        throw new IllegalArgumentException("Unknown units: " + units);
    }

    /**
     * Convert the energy value d from the specified units to frequency in Hz.
     * 
     * @param d
     * @param units
     * @return frequency in Hz
     */
    public double toHz(double d, String units) {
        int i = ArrayUtil.matches("^" + units + "$", freqUnits, true);
        if (i != -1) {
            return freqToHz(d, i);
        }

        i = ArrayUtil.matches("^" + units + "$", enUnits, true);
        if (i != -1) {
            return energyToHz(d, i);
        }

        i = ArrayUtil.matches("^" + units + "$", waveUnits, true);
        if (i != -1) {
            return wavelengthToHz(d, i);
        }

        throw new IllegalArgumentException("unknown units: " + units);
    }

    /**
     * Compute the range of energy values to a wavelength width in meters.
     * 
     * @param d1
     * @param d2
     * @param units
     * @return delta lambda in meters
     */
    public double toDeltaMeters(double d1, double d2, String units) {
        double w1 = toMeters(d1, units);
        double w2 = toMeters(d2, units);
        return Math.abs(w2 - w1);
    }

    /**
     * Compute the range of energy values to a frequency width in Hz.
     * 
     * @param d1
     * @param d2
     * @param units
     * @return delta nu in Hz
     */
    public double toDeltaHz(double d1, double d2, String units) {
        double f1 = toHz(d1, units);
        double f2 = toHz(d2, units);
        return Math.abs(f2 - f1);
    }

    private double freqToMeters(double d, int i) {
        final double nu = d * freqMult[i];
        return c / nu;
    }

    private double energyToMeters(double d, int i) {
        final double e = eV * d * enMult[i];
        return c * h / e;
    }

    private double wavelengthToMeters(double d, int i) {
        return d * waveMult[i];
    }

    private double freqToHz(double d, int i) {
        return d * freqMult[i];
    }

    private double energyToHz(double d, int i) {
        double w = energyToMeters(d, i);
        return c / w;
    }

    private double wavelengthToHz(double d, int i) {
        double w = d * waveMult[i];
        return c / w;
    }

    /*
     * private double waveToWave(double d, int i) { return d / waveMult[i]; }
     * 
     * private double waveToFreq(double d, int i) { double nu = d * c; nu /=
     * freqMult[i]; return nu; }
     * 
     * private double waveToEnergy(double d, int i) { double e = c * h / d; e /=
     * enMult[i]; return e; }
     */

    /*
     * public static void main(String[] args) { EnergyConverter euc = new
     * EnergyConverter();
     * 
     * 
     * for (String u : waveUnits) { System.out.println("absolute: 5"+u+" = " +
     * euc.toMeters(5.0, u) + "m == " + euc.toHz(5.0, u) + "Hz");
     * System.out.println("relative: 4-6"+u+" = " + euc.toDeltaMeters(4.0, 6.0,
     * u) + "m == " + euc.toDeltaHz(4.0, 6.0, u) + "Hz"); }
     * 
     * System.out.println("========"); for (String u : freqUnits) {
     * System.out.println("absolute: 5"+u+" = " + euc.toMeters(5.0, u) + "m == "
     * + euc.toHz(5.0, u) + "Hz"); System.out.println("relative: 4-6"+u+" = " +
     * euc.toDeltaMeters(4.0, 6.0, u) + "m == " + euc.toDeltaHz(4.0, 6.0, u) +
     * "Hz"); }
     * 
     * System.out.println("========"); for (String u : enUnits) {
     * System.out.println("absolute: 5"+u+" = " + euc.toMeters(5.0, u) + "m == "
     * + euc.toHz(5.0, u) + "Hz"); System.out.println("relative: 4-6"+u+" = " +
     * euc.toDeltaMeters(4.0, 6.0, u) + "m == " + euc.toDeltaHz(4.0, 6.0, u) +
     * "Hz"); }
     * 
     * double d = 5.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); d = 10.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); d = 40.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); d = 70.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); d = 110.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); d = 120.0; System.out.println(d + "MHz = " + euc.toMeters(d,
     * "MHz")); }
     */
}
