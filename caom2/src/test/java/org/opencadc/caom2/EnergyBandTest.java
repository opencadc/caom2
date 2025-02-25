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

package org.opencadc.caom2;

import org.opencadc.caom2.EnergyBand;
import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.util.Log4jInit;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class EnergyBandTest {

    private static final Logger log = Logger.getLogger(EnergyBandTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
    }

    //@Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testRoundtrip() {
        try {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (EnergyBand c : EnergyBand.values()) {
                log.debug("testing: " + c);
                String s = c.getValue();
                EnergyBand c2 = EnergyBand.toValue(s);
                Assert.assertEquals(c, c2);
            }

            try {
                EnergyBand c = EnergyBand.toValue("NoSuchEnergyBand");
                Assert.fail("expected IllegalArgumentException, got: " + c);
            } catch (IllegalArgumentException expected) {
                log.debug("caught expected exception: " + expected);
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testChecksum() {
        try {
            // correctness is a 100% duplicate of the enum code itself, but
            // we can test uniqueness
            Set<Integer> values = new TreeSet<Integer>();
            for (EnergyBand c : EnergyBand.values()) {
                int i = c.checksum();
                boolean added = values.add(i);
                Assert.assertTrue("added " + i, added);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFromNullInterval() {
        try {
            Interval<Double> i = null;
            List<EnergyBand> e = EnergyBand.getEnergyBand(i);
            Assert.assertNotNull(e);
            Assert.assertTrue(e.isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testZeroWavelength() {
        try {
            Interval<Double> i = new Interval<Double>(0.0, Double.MIN_VALUE);
            List<EnergyBand> e = EnergyBand.getEnergyBand(i);
            Assert.assertNotNull(e);
            Assert.assertTrue(e.isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFromNoOverlap() {
        try {
            Interval<Double> i = new Interval<Double>(-20.0, -10.0);
            List<EnergyBand> e = EnergyBand.getEnergyBand(i);
            Assert.assertNotNull(e);
            Assert.assertTrue(e.isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFromPartialOverlapInterval() {
        try {
            Interval<Double> inter;
            List<EnergyBand> e;

            inter = new Interval<Double>(200e-9, 900e-9);
            e = EnergyBand.getEnergyBand(inter);
            Assert.assertNotNull(e);
            Assert.assertTrue(e.contains(EnergyBand.OPTICAL));  // 6/7 optical
            Assert.assertTrue(e.contains(EnergyBand.UV));       // 1/7 shorter wavelength

            inter = new Interval<Double>(400e-9, 1100e-9);
            e = EnergyBand.getEnergyBand(inter);
            Assert.assertNotNull(e);
            Assert.assertTrue(e.contains(EnergyBand.OPTICAL));  // 6/7 optical
            Assert.assertTrue(e.contains(EnergyBand.INFRARED)); // 1/7 longer wavelength

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
