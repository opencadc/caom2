/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;


import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Time;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ComputeUtilTest 
{
    private static final Logger log = Logger.getLogger(ComputeUtilTest.class);

    public ComputeUtilTest() { }
    
    @Test
    public void testTransientState()
    {
        try
        {
            Observation o = new SimpleObservation("STUFF", "nonsense");
            Plane plane = new Plane("foo");
            
            int defCode = plane.getStateCode();
            log.debug("testTransientState: " + defCode);
            int nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            
            Assert.assertEquals("default code", defCode, nonTransientCode);
            int notComputed = plane.getStateCode(true);
            log.debug("testTransientState: " + notComputed);
            Assert.assertEquals("not computed", defCode, notComputed);

            ComputeUtil.clearTransientState(plane);
            ComputeUtil.computeTransientState(o, plane);
            int idCode = plane.getStateCode(true);
            Assert.assertTrue("computed identifiers", defCode != idCode);

            assignPos(plane);
            ComputeUtil.computeTransientState(o, plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            int compCode = plane.getStateCode(true);
            log.debug("testTransientState: " + compCode);
            Assert.assertTrue("computed position", defCode != compCode);

            ComputeUtil.clearTransientState(plane);
            assignEnergy(plane);
            ComputeUtil.computeTransientState(o, plane);

            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            compCode = plane.getStateCode(true);
            log.debug("testTransientState: " + compCode);
            Assert.assertTrue("computed position", defCode != compCode);

            ComputeUtil.clearTransientState(plane);
            assignTime(plane);
            ComputeUtil.computeTransientState(o, plane);
            
            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            compCode = plane.getStateCode(true);
            log.debug("testTransientState: " + compCode);
            Assert.assertTrue("computed position", defCode != compCode);

            ComputeUtil.clearTransientState(plane);
            assignPol(plane);
            ComputeUtil.computeTransientState(o, plane);
            
            nonTransientCode = plane.getStateCode(false);
            log.debug("testTransientState: " + nonTransientCode);
            Assert.assertEquals("non-transient only", defCode, nonTransientCode);
            compCode = plane.getStateCode(true);
            log.debug("testTransientState: " + compCode);
            Assert.assertTrue("computed position", defCode != compCode);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void assignPos(Plane p)
    {
        p.position = new Position();
        p.position.resolution = 0.01;
    }

    private void assignEnergy(Plane p)
    {
        p.energy = new Energy();
        p.energy.bandpassName = "foo123";
    }

    private void assignTime(Plane p)
    {
        p.time = new Time();
        p.time.exposure = new Double(123.0);
    }

    private void assignPol(Plane p)
    {
        Polarization pol = new Polarization();
        pol.states = new ArrayList<PolarizationState>();
        pol.states.add(PolarizationState.I);
        pol.dimension = new Integer(1);
        p.polarization = pol;
    }
}
