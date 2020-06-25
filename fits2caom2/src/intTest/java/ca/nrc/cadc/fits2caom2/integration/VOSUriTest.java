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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.fits2caom2.integration;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.security.PrivilegedExceptionAction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;

/**
 *
 * @author jburke
 */
public class VOSUriTest extends AbstractTest
{
    private static final Logger log = Logger.getLogger(VOSUriTest.class);

    private static final String VOS_URI_BLAST_250 = "vos://cadc.nrc.ca~vospace/" +
        "CADCRegtest1/DONOTDELETE_FITS2CAOM2_TESTFILES/BLAST_250.fits";
    private static final String VOS_URI_BLAST_350 = "vos://cadc.nrc.ca~vospace/" +
        "CADCRegtest1/DONOTDELETE_FITS2CAOM2_TESTFILES/BLAST_350.fits";

    private static File SSL_CERT;

    public VOSUriTest()
    {
        super();
    }

    @BeforeClass
    public static void setUpClass()
    {
        Log4jInit.setLevel("ca.nrc.cadc.fits2caom2", Level.INFO);
        SSL_CERT = new File(System.getProperty("user.home") + "/.pub/proxy.pem");
    }

    @Test
    public void testSingleVOSUri()
    {
        try
        {
            log.debug("testSingleVOSUri");

            final String[] args = new String[]
            {
                "--collection=TEST",
                "--observationID=VOSpaceFile",
                "--productID=productID",
                "--uri=" + VOS_URI_BLAST_250,
                "--default=src/int-test/resources/simplefits.default"
            };

            Subject subject = SSLUtil.createSubject(SSL_CERT);
            Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    doTest(args);
                    doTest(args, "build/tmp/SimpleFitsTest.xml");

                    return null;
                }
            });

            // check that CDi_j worked
            ObservationReader or = new ObservationReader();
            Observation o = or.read(new FileReader("build/tmp/SimpleFitsTest.xml"));
            Assert.assertNotNull(o);
            Chunk c = o.getPlanes().iterator().next().getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            Assert.assertNotNull("chunk.position", c.position);
            Assert.assertNotNull("chunk.position.axis.function", c.position.getAxis().function);

            log.info("testSingleVOSUri passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMultipleVOSUri()
    {
        try
        {
            log.debug("testMultipleVOSUri");

            final String[] args = new String[]
            {
                "--collection=TEST",
                "--observationID=VOSpaceFile",
                "--productID=productID",
                "--uri=" + VOS_URI_BLAST_250 +"," + VOS_URI_BLAST_350,
                "--default=src/int-test/resources/simplefits.default"
            };

            Subject subject = SSLUtil.createSubject(SSL_CERT);
            Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    doTest(args);
                    doTest(args, "build/tmp/SimpleFitsTest.xml");

                    return null;
                }
            });

            // check that CDi_j worked
            ObservationReader or = new ObservationReader();
            Observation o = or.read(new FileReader("build/tmp/SimpleFitsTest.xml"));
            Assert.assertNotNull(o);
            Chunk c = o.getPlanes().iterator().next().getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            Assert.assertNotNull("chunk.position", c.position);
            Assert.assertNotNull("chunk.position.axis.function", c.position.getAxis().function);

            log.info("testMultipleVOSUri passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testVOSUriWithLocal()
    {
        try
        {
            log.debug("testVOSUriWithLocal");

            final String[] args = new String[]
            {
                "--collection=TEST",
                "--observationID=VOSpaceFile",
                "--productID=productID",
                "--uri=" + VOS_URI_BLAST_250,
                "--local=src/int-test/resources/mef.fits",
                "--default=src/int-test/resources/multiextensionfits.default"
            };

            Subject subject = SSLUtil.createSubject(SSL_CERT);
            Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    doTest(args);
                    doTest(args, "build/tmp/SimpleFitsTest.xml");

                    return null;
                }
            });

            // check that CDi_j worked
            ObservationReader or = new ObservationReader();
            Observation o = or.read(new FileReader("build/tmp/SimpleFitsTest.xml"));
            Assert.assertNotNull(o);
            Chunk c = o.getPlanes().iterator().next().getArtifacts().iterator().next().getParts().iterator().next().getChunks().iterator().next();
            Assert.assertNotNull("chunk.position", c.position);
            Assert.assertNotNull("chunk.position.axis.function", c.position.getAxis().function);

            log.info("testVOSUriWithLocal passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
