/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.torkeep;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.SimpleObservation;

/**
 *
 * @author pdowler
 */
public class RepoActionTest {
    private static final Logger log = Logger.getLogger(RepoActionTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.torkeep", Level.INFO);
        Log4jInit.setLevel("org.opencadc.caom2", Level.INFO);
    }

    public RepoActionTest() { 
    }
    
    @Test
    public void testAssignPublisherID() throws Exception {
        Observation obs = new SimpleObservation("FOO", URI.create("caom:FOO/bar"), SimpleObservation.EXPOSURE);
        Plane p1 = new Plane(URI.create("caom:FOO/bar/bar1"));
        Plane p2 = new Plane(URI.create("caom:FOO/bar/bar2"));
        obs.getPlanes().add(p1);
        obs.getPlanes().add(p2);
        
        final String basePublisherID = "ivo://opencadc.org/";
        
        log.info("caom scheme:");
        final URI expect1 = URI.create(basePublisherID + obs.getCollection() + "?bar/bar1");
        final URI expect2 = URI.create(basePublisherID + obs.getCollection() + "?bar/bar2");
        RepoAction.assignPublisherID(obs, basePublisherID);
        log.info(p1.getURI() + " -> " + p1.publisherID);
        log.info(p2.getURI() + " -> " + p2.publisherID);
        Assert.assertEquals(expect1, p1.publisherID);
        Assert.assertEquals(expect2, p2.publisherID);
        
        log.info("caom scheme - no basePublisherID:");
        try {
            RepoAction.assignPublisherID(obs, null);
            Assert.fail("expected IllegalArgumentException, got publisherID=" 
                + p1.publisherID + " and " + p2.publisherID);
        } catch (IllegalArgumentException ex) {
            log.info("caught expected: " + ex);
        }
        
        log.info("caom scheme - Plane.uri not an extension of Observation.uri:");
        obs.getPlanes().clear();
        p1 = new Plane(URI.create("caom:FOO/bar1"));
        p2 = new Plane(URI.create("caom:FOO/bar2"));
        obs.getPlanes().add(p1);
        obs.getPlanes().add(p2);
        final URI ex1 = URI.create(basePublisherID + obs.getCollection() + "?bar1");
        final URI ex2 = URI.create(basePublisherID + obs.getCollection() + "?bar2");
        RepoAction.assignPublisherID(obs, basePublisherID);
        log.info(p1.getURI() + " -> " + p1.publisherID);
        log.info(p2.getURI() + " -> " + p2.publisherID);
        Assert.assertEquals(ex1, p1.publisherID);
        Assert.assertEquals(ex2, p2.publisherID);
        
        log.info("ivo scheme - ignore basePublisherID and copy Plane.uri:");
        obs.getPlanes().clear();
        p1 = new Plane(URI.create("ivo://authority/FOO?bar/bar1"));
        p2 = new Plane(URI.create("ivo://authority/FOO?bar/bar2"));
        obs.getPlanes().add(p1);
        obs.getPlanes().add(p2);
        RepoAction.assignPublisherID(obs, basePublisherID);
        log.info(p1.getURI() + " -> " + p1.publisherID);
        log.info(p2.getURI() + " -> " + p2.publisherID);
        Assert.assertEquals(p1.getURI(), p1.publisherID);
        Assert.assertEquals(p2.getURI(), p2.publisherID);
        
        log.info("ivo scheme - no basePublisherID needed at all:");
        obs.getPlanes().clear();
        p1 = new Plane(URI.create("ivo://authority/FOO?bar/bar1"));
        p2 = new Plane(URI.create("ivo://authority/FOO?bar/bar2"));
        obs.getPlanes().add(p1);
        obs.getPlanes().add(p2);
        RepoAction.assignPublisherID(obs, null);
        log.info(p1.getURI() + " -> " + p1.publisherID);
        log.info(p2.getURI() + " -> " + p2.publisherID);
        Assert.assertEquals(p1.getURI(), p1.publisherID);
        Assert.assertEquals(p2.getURI(), p2.publisherID);
        
        log.info("Plane.uri not an extension of Observation.uri:");
        obs.getPlanes().clear();
        p1 = new Plane(URI.create("ivo://authority/FOO?bar1"));
        p2 = new Plane(URI.create("ivo://authority/FOO?bar2"));
        obs.getPlanes().add(p1);
        obs.getPlanes().add(p2);
        RepoAction.assignPublisherID(obs, null);
        log.info(p1.getURI() + " -> " + p1.publisherID);
        log.info(p2.getURI() + " -> " + p2.publisherID);
        Assert.assertEquals(p1.getURI(), p1.publisherID);
        Assert.assertEquals(p2.getURI(), p2.publisherID);
    }
}
