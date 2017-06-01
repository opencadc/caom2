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
package ca.nrc.cadc.caom2.xml;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CompositeObservation;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Provenance;
import ca.nrc.cadc.caom2.Requirements;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class ObservationReaderWriterTest
{
    private static Logger log = Logger.getLogger(ObservationReaderWriterTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.xml", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.xml", Level.INFO);
    }
    
    public ObservationReaderWriterTest() { }

    @Test
    public void testTemplate()
    {
        try
        {
            
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    // this "test" writes out a pretty complete document to use in comparison with python round-trip
    // and python meta checksum computations
    //@Test
    public void doWriteCompleteComposite()
    {
        try
        {
            Caom2TestInstances ti = new Caom2TestInstances();
            ti.setComplete(true);
            ti.setDepth(5);
            ti.setChildCount(2);
            Observation o = ti.getCompositeObservation();
            
            long t1 = new Date().getTime();
            long t2 = t1 + 2000l;
            
            MessageDigest digest = MessageDigest.getInstance("MD5");
            CaomUtil.assignLastModified(o, new Date(t1), "lastModified");
            CaomUtil.assignLastModified(o, new Date(t2), "maxLastModified");
            URI ocs = o.computeMetaChecksum(true, digest);
            URI oacs = o.computeAccMetaChecksum(true, digest);
            CaomUtil.assignMetaChecksum(o, ocs, "metaChecksum");
            CaomUtil.assignMetaChecksum(o, oacs, "accMetaChecksum");
            for (Plane pl : o.getPlanes())
            {
                CaomUtil.assignLastModified(pl, new Date(t1), "lastModified");
                CaomUtil.assignLastModified(pl, new Date(t2), "maxLastModified");
                CaomUtil.assignMetaChecksum(pl, pl.computeMetaChecksum(true, digest), "metaChecksum");
                CaomUtil.assignMetaChecksum(pl, pl.computeAccMetaChecksum(true, digest), "accMetaChecksum");
                for (Artifact ar : pl.getArtifacts())
                {
                    CaomUtil.assignLastModified(ar, new Date(t1), "lastModified");
                    CaomUtil.assignLastModified(ar, new Date(t2), "maxLastModified");
                    CaomUtil.assignMetaChecksum(ar, ar.computeMetaChecksum(true, digest), "metaChecksum");
                    CaomUtil.assignMetaChecksum(ar, ar.computeAccMetaChecksum(true, digest), "accMetaChecksum");
                    for (Part pa : ar.getParts())
                    {
                        CaomUtil.assignLastModified(pa, new Date(t1), "lastModified");
                        CaomUtil.assignLastModified(pa, new Date(t2), "maxLastModified");
                        CaomUtil.assignMetaChecksum(pa, pa.computeMetaChecksum(true, digest), "metaChecksum");
                        CaomUtil.assignMetaChecksum(pa, pa.computeAccMetaChecksum(true, digest), "accMetaChecksum");
                        for (Chunk ch : pa.getChunks())
                        {
                            CaomUtil.assignLastModified(ch, new Date(t1), "lastModified");
                            CaomUtil.assignLastModified(ch, new Date(t2), "maxLastModified");
                            CaomUtil.assignMetaChecksum(ch, ch.computeMetaChecksum(true, digest), "metaChecksum");
                            CaomUtil.assignMetaChecksum(ch, ch.computeAccMetaChecksum(true, digest), "accMetaChecksum");
                        }
                    }
                }
            }
            
            File f = new File("sample-composite-caom23.xml");
            FileOutputStream fos = new FileOutputStream(f);
            ObservationWriter w = new ObservationWriter();
            w.write(o, fos);
            fos.close();
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSupportAllVersions()
    {
        try
        {
            Observation obs = new SimpleObservation("FOO", "bar");
            
            ObservationReader validatingReader = new ObservationReader();
            ObservationReader nonvalidatingReader = new ObservationReader(false);
            
            ObservationWriter w20 = new ObservationWriter("caom2", XmlConstants.CAOM2_0_NAMESPACE, false);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            w20.write(obs, bos);
            String caom20 = bos.toString();
            log.info("caom-2.0 XML:\n" + caom20);
            assertTrue(caom20.contains(XmlConstants.CAOM2_0_NAMESPACE));
            Observation obs20 = validatingReader.read(caom20);
            compareObservations(obs, obs20);
            
            // add timestamps
            long t1 = new Date().getTime();
            long t2 = t1 + 2000l;
            CaomUtil.assignLastModified(obs, new Date(t1), "lastModified");
            bos = new ByteArrayOutputStream();
            w20.write(obs, bos);
            caom20 = bos.toString();
            log.info("caom-2.0 XML:\n" + caom20);
            assertTrue(caom20.contains(XmlConstants.CAOM2_0_NAMESPACE));
            obs20 = validatingReader.read(caom20);
            compareObservations(obs, obs20);
            
            
            ObservationWriter w21 = new ObservationWriter("caom2", XmlConstants.CAOM2_1_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w21.write(obs, bos);
            String caom21 = bos.toString();
            log.info("caom-2.1 XML:\n" + caom21);
            assertTrue(caom21.contains(XmlConstants.CAOM2_1_NAMESPACE));
            Observation obs21 = validatingReader.read(caom21);
            compareObservations(obs, obs21);
            
            // new writer
            w21 = new ObservationWriter("caom2", XmlConstants.CAOM2_1_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w21.write(obs, bos);
            caom21 = bos.toString();
            log.info("caom-2.1 XML:\n" + caom21);
            assertTrue(caom21.contains(XmlConstants.CAOM2_1_NAMESPACE));
            obs21 = validatingReader.read(caom21);
            compareObservations(obs, obs21);
            
            ObservationWriter w22 = new ObservationWriter("caom2", XmlConstants.CAOM2_2_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w22.write(obs, bos);
            String caom22 = bos.toString();
            log.info("caom-2.2 XML:\n" + caom22);
            assertTrue(caom22.contains(XmlConstants.CAOM2_2_NAMESPACE));
            Observation obs22 = nonvalidatingReader.read(caom22);
            compareObservations(obs, obs22);
            
            // new writer
            w22 = new ObservationWriter("caom2", XmlConstants.CAOM2_2_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w22.write(obs, bos);
            caom22 = bos.toString();
            log.info("caom-2.2 XML:\n" + caom22);
            assertTrue(caom22.contains(XmlConstants.CAOM2_2_NAMESPACE));
            obs22 = nonvalidatingReader.read(caom22);
            compareObservations(obs, obs22);

            ObservationWriter w23 = new ObservationWriter("caom2", XmlConstants.CAOM2_3_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w23.write(obs, bos);
            String caom23 = bos.toString();
            log.info("caom-2.3 XML:\n" + caom23);
            assertTrue(caom23.contains(XmlConstants.CAOM2_3_NAMESPACE));
            Observation obs23 = nonvalidatingReader.read(caom23);
            compareObservations(obs, obs23);
            
            // add maxLastModified and meta checksums
            CaomUtil.assignLastModified(obs, new Date(t2), "maxLastModified");
            MessageDigest digest = MessageDigest.getInstance("MD5");
            URI ocs = obs.computeMetaChecksum(true, digest);
            URI oacs = obs.computeAccMetaChecksum(true, digest);
            CaomUtil.assignMetaChecksum(obs, ocs, "metaChecksum");
            CaomUtil.assignMetaChecksum(obs, oacs, "accMetaChecksum");
            bos = new ByteArrayOutputStream();
            w23.write(obs, bos);
            caom23 = bos.toString();
            log.info("caom-2.3 XML:\n" + caom23);
            assertTrue(caom23.contains(XmlConstants.CAOM2_3_NAMESPACE));
            obs23 = nonvalidatingReader.read(caom23);
            compareObservations(obs, obs23);
            
        }
        //catch(ObservationParsingException expected)
        //{
        //    log.info("caught expected exception: " + expected);
        //}
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidLongID()
    {
        try
        {
            Observation obs = new SimpleObservation("FOO", "bar");
            ObservationWriter w = new ObservationWriter("caom2", XmlConstants.CAOM2_0_NAMESPACE, false);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            w.write(obs, bos);
            String str = bos.toString();
            String invalid = str.replace("caom2:id=\"", "caom2:id=\"x");
            log.debug("invalid XML: " + invalid);
            ObservationReader r = new ObservationReader();
            Observation obs2 = r.read(invalid);
        }
        catch(ObservationParsingException expected)
        {
            log.debug("caught expected exception: " + expected);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testInvalidUUID()
    {
        try
        {
            Observation obs = new SimpleObservation("FOO", "bar");
            ObservationWriter w = new ObservationWriter();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            w.write(obs, bos);
            String str = bos.toString();
            String invalid = str.replace("0000", "xxxx");
            log.debug("invalid XML: " + invalid);
            ObservationReader r = new ObservationReader();
            Observation obs2 = r.read(invalid);
        }
        catch(ObservationParsingException expected)
        {
            log.debug("caught expected exception: " + expected);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCleanWhitespace()
    {
        try
        {
            ObservationReader r = new ObservationReader();
            
            String actual;
            
            actual = r.cleanWhitespace(null);
            assertNull(actual);
            
            actual = r.cleanWhitespace("");
            assertNull(actual);
            
            actual = r.cleanWhitespace("foo");
            assertEquals("foo", actual);
            
            actual = r.cleanWhitespace("  trimmed  ");
            assertEquals("trimmed", actual);
            
            actual = r.cleanWhitespace("  trim outside only  ");
            assertEquals("trim outside only", actual);
            
            actual = r.cleanWhitespace("  trim  multiple \t inside  ");
            assertEquals("trim multiple inside", actual);
            
            actual = r.cleanWhitespace("  trim\njunk\rinside\tphrase  ");
            assertEquals("trim junk inside phrase", actual);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testReadNull()
    {
        try
        {
            ObservationReader r = new ObservationReader();
            
            try { r.read((String) null); }
            catch(IllegalArgumentException expected) { }
            
            try { r.read((InputStream) null); }
            catch(IllegalArgumentException expected) { }
            
            try { r.read((Reader) null); }
            catch(IllegalArgumentException expected) { }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCleanWhitespaceRoundTrip()
    {
        try
        {
            ObservationReader r = new ObservationReader();
            SimpleObservation observation = new SimpleObservation("FOO", "bar");
            observation.telescope = new Telescope("bar\tbaz\n1.0");
            StringBuilder sb = new StringBuilder();
            ObservationWriter writer = new ObservationWriter();
            writer.setWriteEmptyCollections(false);
            writer.write(observation, sb);
            log.debug(sb.toString());

            // do not validate the XML.
            ObservationReader reader = new ObservationReader(false);
            Observation returned = reader.read(sb.toString());
            
            assertEquals("FOO", returned.getURI().getCollection());
            assertEquals("bar", returned.getURI().getObservationID());
            assertNotNull("has telescope", returned.telescope);
            assertEquals("bar baz 1.0", returned.telescope.getName());

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testMinimalSimple()
    {
        try
        {
            
            for (int i = 1; i < 6; i++)
            {
                log.info("testMinimalSimple: depth = " + i);
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                SimpleObservation observation = getMinimalSimple(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
                
                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getMinimalSimple(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCompleteSimple()
    {
        try
        {
            log.debug("testCompleteSimple");
            for (int i = 1; i <= 5; i++)
            {
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                SimpleObservation observation = getCompleteSimple(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
                
                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getCompleteSimple(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
            }

            SimpleObservation observation = getCompleteSimple(5, true);
            testObservation(observation, false, "c2", null, true); // custom ns prefix, default namespace

            // nullify optional fields introduced after 2.0 so the comparison will work
            observation.requirements = null;
            for (Plane p : observation.getPlanes())
            {
                p.quality = null;
                p.creatorID = null;
                for (Artifact a : p.getArtifacts())
                {
                    a.contentChecksum = null;
                }
            }
            testObservation(observation, false, "caom2", XmlConstants.CAOM2_0_NAMESPACE, true); // compat mode
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCompleteSimpleSetAlgorithm()
    {
        try
        {
            log.debug("testCompleteSimpleSetAlgorithm");
            for (int i = 1; i <= 1; i++)
            {
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                SimpleObservation observation = getCompleteSimpleSetAlgorithm(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);

                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getCompleteSimpleSetAlgorithm(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testMinimalComposite()
    {
        try
        {
            log.debug("testMinimalComposite");
            for (int i = 1; i < 6; i++)
            {
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                CompositeObservation observation = getMinimalComposite(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
                
                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getMinimalComposite(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCompleteComposite()
    {
        try
        {
            log.debug("testCompleteComposite");
            for (int i = 1; i < 6; i++)
            {
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                CompositeObservation observation = getCompleteComposite(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
                
                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getCompleteComposite(i, boundsIsCircle);
                
                // Write empty elements.
                testObservation(observation, true);
                
                // Do not write empty elements.
                testObservation(observation, false);
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    //@Test
    public void testComputedSimple()
    {
        try
        {
            log.debug("testComputedSimple");
            int i = 5; // need chunks for compute
            SimpleObservation observation = getCompleteSimple(i, false); // CoordBounds2D as CoordPolygon2D

            // Write empty elements.
            testObservation(observation, true);

            // Do not write empty elements.
            testObservation(observation, false);
            
            log.debug("computing transient metadata for planes...");
                    
            for (Plane p : observation.getPlanes())
            {
                // TODO: create dummy values for all computed plane metadata
                
                Assert.assertNotNull("Plane.position", p.position);
                Assert.assertNotNull("Plane.position.bounds", p.position.bounds);

                Assert.assertNotNull("Plane.energy", p.energy);
                Assert.assertNotNull("Plane.energy.bounds", p.energy.bounds);

                Assert.assertNotNull("Plane.time", p.time);
                Assert.assertNotNull("Plane.time.bounds", p.time.bounds);

                Assert.assertNotNull("Plane.polarization", p.polarization);
                Assert.assertNotNull("Plane.polarization.states", p.polarization.states);
                Assert.assertTrue("Plane.polarization.states non-empty", !p.polarization.states.isEmpty());
            }

            // CAOM-2.3 is now the default
            testObservation(observation, true);
            
            testObservation(observation, false);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
    protected SimpleObservation getMinimalSimple(int depth, boolean boundsIsCircle)
        throws Exception
    {        
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(false);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getSimpleObservation();
    }
    
    protected SimpleObservation getCompleteSimple(int depth, boolean boundsIsCircle)
        throws Exception
    {        
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(true);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getSimpleObservation();
    }

    protected SimpleObservation getCompleteSimpleSetAlgorithm(int depth, boolean boundsIsCircle)
            throws Exception
    {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(true);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getSimpleObservationSetAlgorithm();
    }
    
    protected CompositeObservation getMinimalComposite(int depth, boolean boundsIsCircle)
        throws Exception
    {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(false);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getCompositeObservation();
    }
    
    protected CompositeObservation getCompleteComposite(int depth, boolean boundsIsCircle)
        throws Exception
    {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(true);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getCompositeObservation();
    }

    protected void testObservation(Observation observation, boolean writeEmptyCollections)
        throws Exception
    {
        testObservation(observation, writeEmptyCollections, null, null, true);
    }

    protected void testObservation(Observation observation, boolean writeEmptyCollections, String nsPrefix, String forceNS, boolean schemaVal)
        throws Exception
    {
        StringBuilder sb = new StringBuilder();
        ObservationWriter writer = null;
        if (forceNS != null)
            writer = new ObservationWriter(nsPrefix, forceNS, writeEmptyCollections);
        else
            writer = new ObservationWriter();
        
        writer.setWriteEmptyCollections(writeEmptyCollections);
        writer.write(observation, sb);
        log.debug(sb.toString());

        // well-formed XML
        ObservationReader reader = new ObservationReader(false);
        Observation returned = reader.read(sb.toString());

        compareObservations(observation, returned);
        
        if (!schemaVal)
            return;
        
        // valid XML
        reader = new ObservationReader(true);
        returned = reader.read(sb.toString());

        compareObservations(observation, returned);
    }
    
    protected void compareEntity(CaomEntity expected, CaomEntity actual)
    {
        assertEquals("type", expected.getClass().getName(), actual.getClass().getName());
        String t = expected.getClass().getSimpleName();
        assertEquals(expected.getID(), actual.getID());
        if (expected.getLastModified() != null)
        {
            assertNotNull(t+".lastModified", actual.getLastModified());
            assertEquals(t+".lastModified", expected.getLastModified().getTime(), actual.getLastModified().getTime());
        }
        if (expected.getMaxLastModified() != null)
        {
            assertNotNull(t+".maxLastModified", actual.getMaxLastModified());
            assertEquals(t+".maxLastModified", expected.getMaxLastModified().getTime(), actual.getMaxLastModified().getTime());
        }
        if (expected.getMetaChecksum() != null)
        {
            assertNotNull(t+".metaChecksum", actual.getMetaChecksum());
            assertEquals(t+".metaChecksum", expected.getMetaChecksum(), actual.getMetaChecksum());
        }
        if (expected.getAccMetaChecksum()!= null)
        {
            assertNotNull(t+".accMetaChecksum", actual.getAccMetaChecksum());
            assertEquals(t+".accMetaChecksum", expected.getAccMetaChecksum(), actual.getAccMetaChecksum());
        }
    
    }
    
    protected void compareObservations(Observation expected, Observation actual)
    {
        compareEntity(expected, actual);
        
        assertNotNull(expected.getURI().getCollection());
        assertNotNull(actual.getURI().getCollection());
        assertEquals(expected.getURI().getCollection(), actual.getURI().getCollection());
        
        assertNotNull(expected.getURI().getObservationID());
        assertNotNull(actual.getURI().getObservationID());
        assertEquals(expected.getURI().getObservationID(), actual.getURI().getObservationID());
        
        assertNotNull(expected.getAlgorithm());
        assertNotNull(actual.getAlgorithm());
        assertEquals(expected.getAlgorithm().getName(), actual.getAlgorithm().getName());
        
        assertEquals(expected.metaRelease, actual.metaRelease);
        compareProposal(expected.proposal, actual.proposal);
        compareTarget(expected.target, actual.target);
        compareTargetPosition(expected.targetPosition, actual.targetPosition);
        compareRequirements(expected.requirements, actual.requirements);
        compareTelescope(expected.telescope, actual.telescope);
        compareInstrument(expected.instrument, actual.instrument);
        compareEnvironment(expected.environment, actual.environment);
        
        comparePlanes(expected.getPlanes(), actual.getPlanes());
        
        if (expected instanceof CompositeObservation && actual instanceof CompositeObservation)
        {
            compareMembers(((CompositeObservation) expected).getMembers(), ((CompositeObservation) actual).getMembers());
        }
    }
    
    protected void compareProposal(Proposal expected, Proposal actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getID(), actual.getID());
        assertEquals(expected.pi, actual.pi);
        assertEquals(expected.project, actual.project);
        assertEquals(expected.title, actual.title);
        assertEquals(expected.getKeywords(), actual.getKeywords());
    }
    
    protected void compareTarget(Target expected, Target actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.type, actual.type);
        assertEquals(expected.redshift, actual.redshift);
        assertEquals(expected.getKeywords(), actual.getKeywords());
    }
    
    protected void compareTargetPosition(TargetPosition expected, TargetPosition actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getCoordsys(), actual.getCoordsys());
        comparePoint(expected.getCoordinates(), actual.getCoordinates());
        assertEquals(expected.equinox, actual.equinox);
    }
    
    protected void comparePoint(Point expected, Point actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.cval1, actual.cval1, 0.0);
        assertEquals(expected.cval2, actual.cval2, 0.0);
    }
    
    protected void compareTelescope(Telescope expected, Telescope actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.geoLocationX, actual.geoLocationX);
        assertEquals(expected.geoLocationY, actual.geoLocationY);
        assertEquals(expected.geoLocationZ, actual.geoLocationZ);
        assertEquals(expected.getKeywords(), actual.getKeywords());
    }
    
    protected void compareInstrument(Instrument expected, Instrument actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getKeywords(), actual.getKeywords());
    }

    protected void compareEnvironment(Environment expected, Environment actual)
    {
        if (expected == null)
        {
            assertNull(actual);
            return;
        }

        assertNotNull(actual);
        assertEquals(expected.seeing, actual.seeing, 0.0);
        assertEquals(expected.humidity, actual.humidity, 0.0);
        assertEquals(expected.elevation, actual.elevation, 0.0);
        assertEquals(expected.tau, actual.tau, 0.0);
        assertEquals(expected.wavelengthTau, actual.wavelengthTau, 0.0);
        assertEquals(expected.ambientTemp, actual.ambientTemp, 0.0);
        assertEquals(expected.photometric, actual.photometric);

    }
    
    protected void compareMembers(Set<ObservationURI> expected, Set<ObservationURI> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            ObservationURI expectedUri = (ObservationURI) expectedIter.next();
            ObservationURI actualUri = (ObservationURI) actualIter.next();
            
            assertNotNull(expectedUri);
            assertNotNull(actualUri);
            assertEquals(expectedUri, actualUri);
        }
    }
    
    protected void comparePlanes(Set<Plane> expected, Set<Plane> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator expectedIter = expected.iterator();
        Iterator actualIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            Plane expectedPlane = (Plane) expectedIter.next();
            Plane actualPlane = (Plane) actualIter.next();
            
            assertNotNull(expectedPlane);
            assertNotNull(actualPlane);

            compareEntity(expectedPlane, actualPlane);

            assertEquals(expectedPlane.getProductID(), actualPlane.getProductID());
            assertEquals(expectedPlane.creatorID, actualPlane.creatorID);
            assertEquals(expectedPlane.metaRelease, actualPlane.metaRelease);
            assertEquals(expectedPlane.dataRelease, actualPlane.dataRelease);
            assertEquals(expectedPlane.dataProductType, actualPlane.dataProductType);
            assertEquals(expectedPlane.calibrationLevel, actualPlane.calibrationLevel);
            
            compareComputed(expectedPlane, actualPlane);
            
            compareDataQuality(expectedPlane.quality, actualPlane.quality);
            compareProvenance(expectedPlane.provenance, actualPlane.provenance);
            compareArtifacts(expectedPlane.getArtifacts(), actualPlane.getArtifacts());
        }
    }
    
    protected void compareRequirements(Requirements expected, Requirements actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.getFlag(), actual.getFlag());
    }
    
    protected void compareComputed(Plane expected, Plane actual)
    {
        compare(expected.position, actual.position);
        compare(expected.energy, actual.energy);
        compare(expected.time, actual.time);
        compare(expected.polarization, actual.polarization);
    }
    protected void compare(Position expected, Position actual)
    {
        if (expected == null)
        {
            Assert.assertNull(actual);
            return;
        }
        if (expected.bounds == null)
            Assert.assertNull(actual.bounds);
        //else
        //    throw new UnsupportedOperationException("compare: non-null Position.bounds");
        
        if (expected.dimension == null)
            Assert.assertNull(actual.dimension);
        else
        {
            Assert.assertEquals(expected.dimension.naxis1, actual.dimension.naxis1);
            Assert.assertEquals(expected.dimension.naxis2, actual.dimension.naxis2);
        }
        if (expected.resolution == null)
            Assert.assertNull(actual.resolution);
        else
            Assert.assertEquals(expected.resolution, actual.resolution);
        if (expected.sampleSize == null)
            Assert.assertNull(actual.sampleSize);
        else
            Assert.assertEquals(expected.sampleSize, actual.sampleSize);
        if (expected.timeDependent == null)
            Assert.assertNull(actual.timeDependent);
        else
            Assert.assertEquals(expected.timeDependent, actual.timeDependent);
    }
    protected void compare(Energy expected, Energy actual)
    {
        if (expected == null)
        {
            Assert.assertNull(actual);
            return;
        }
        if (expected.bounds == null)
            Assert.assertNull(actual.bounds);
        else
        {
            Assert.assertEquals(expected.bounds.getLower(), actual.bounds.getLower(), 0.0);
            Assert.assertEquals(expected.bounds.getUpper(), actual.bounds.getUpper(), 0.0);
            // TODO: compare samples when read/write includes them
        }
        
        Assert.assertEquals(expected.dimension, actual.dimension);
        Assert.assertEquals(expected.resolvingPower, actual.resolvingPower);
        Assert.assertEquals(expected.sampleSize, actual.sampleSize);
        Assert.assertEquals(expected.bandpassName, actual.bandpassName);
        Assert.assertEquals(expected.emBand, actual.emBand);
        Assert.assertEquals(expected.restwav, actual.restwav);
        if (expected.transition == null)
            Assert.assertNull(actual.transition);
        else
        {
            Assert.assertEquals(expected.transition.getSpecies(), actual.transition.getSpecies());
            Assert.assertEquals(expected.transition.getTransition(), actual.transition.getTransition());
        }
    }
    protected void compare(Time expected, Time actual)
    {
        if (expected == null)
        {
            Assert.assertNull(actual);
            return;
        }
        if (expected.bounds == null)
            Assert.assertNull(actual.bounds);
        else
        {
            Assert.assertEquals(expected.bounds.getLower(), actual.bounds.getLower(), 0.0);
            Assert.assertEquals(expected.bounds.getUpper(), actual.bounds.getUpper(), 0.0);
            // TODO: compare samples when read/write includes them
        }
        
        Assert.assertEquals(expected.dimension, actual.dimension);
        Assert.assertEquals(expected.resolution, actual.resolution);
        Assert.assertEquals(expected.sampleSize, actual.sampleSize);
        Assert.assertEquals(expected.exposure, actual.exposure);
    }
    
    protected void compare(Polarization expected, Polarization actual)
    {
        if (expected == null)
        {
            Assert.assertNull(actual);
            return;
        }
        if (expected.states == null)
            Assert.assertNull(actual.states);
        else
        {
            Assert.assertEquals(expected.dimension, actual.dimension);
            Assert.assertEquals(expected.states.size(), actual.states.size());
            // states is in canonical order already
            Iterator<PolarizationState> ei = expected.states.iterator();
            Iterator<PolarizationState> ai = actual.states.iterator();
            while ( ei.hasNext() )
            {
                Assert.assertEquals(ei.next(), ai.next());
            }
        }
    }
    
    protected void compareDataQuality(DataQuality expected, DataQuality actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.getFlag(), actual.getFlag());
    }
    
    protected void compareProvenance(Provenance expected, Provenance actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.version, actual.version);
        assertEquals(expected.project, actual.project);
        assertEquals(expected.producer, actual.producer);
        assertEquals(expected.runID, actual.runID);
        assertEquals(expected.reference, actual.reference);
        assertEquals(expected.lastExecuted, actual.lastExecuted);
        compareInputs(expected.getInputs(), actual.getInputs());
    }
    
    protected void compareInputs(Set<PlaneURI> expected, Set<PlaneURI> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            PlaneURI expectedPlaneUri = (PlaneURI) expectedIter.next();
            PlaneURI actualPlaneUri = (PlaneURI) actualIter.next();
            
            assertNotNull(expectedPlaneUri);
            assertNotNull(actualPlaneUri);
            assertEquals(expectedPlaneUri, actualPlaneUri);
        }
    }
    
    protected void compareArtifacts(Set<Artifact> expected, Set<Artifact> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            Artifact expectedArtifact = (Artifact) expectedIter.next();
            Artifact actualArtifact = (Artifact) actualIter.next();
            
            assertNotNull(expectedArtifact);
            assertNotNull(actualArtifact);

            compareEntity(expectedArtifact, actualArtifact);
            
            assertEquals(expectedArtifact.getURI(), actualArtifact.getURI());
            assertEquals(expectedArtifact.getProductType(), actualArtifact.getProductType());
            assertEquals(expectedArtifact.getReleaseType(), actualArtifact.getReleaseType());
            
            assertEquals(expectedArtifact.contentType, actualArtifact.contentType);
            assertEquals(expectedArtifact.contentLength, actualArtifact.contentLength);
            if (expectedArtifact.contentChecksum != null && actualArtifact.contentChecksum != null)
            {
                assertEquals(expectedArtifact.contentChecksum, actualArtifact.contentChecksum);
            }

            compareParts(expectedArtifact.getParts(), expectedArtifact.getParts());
        }
    }
    
    protected void compareParts(Set<Part> expected, Set<Part> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            Part expectedPart = (Part) expectedIter.next();
            Part actualPart = (Part) actualIter.next();
            
            assertNotNull(expectedPart);
            assertNotNull(actualPart);

            compareEntity(expectedPart, actualPart);
            
            assertEquals(expectedPart.getName(), actualPart.getName());
            assertEquals(expectedPart.productType, actualPart.productType);
            
            compareChunks(expectedPart.getChunks(), actualPart.getChunks());
        }
    }
    
    protected void compareChunks(Set<Chunk> expected, Set<Chunk> actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());
        
        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext())
        {
            Chunk expectedChunk = (Chunk) expectedIter.next();
            Chunk actualChunk = (Chunk) actualIter.next();
            
            assertNotNull(expectedChunk);
            assertNotNull(actualChunk);

            compareEntity(expectedChunk, actualChunk);
            
            assertEquals(expectedChunk.productType, actualChunk.productType);
            assertEquals(expectedChunk.naxis, actualChunk.naxis);
            assertEquals(expectedChunk.observableAxis, actualChunk.observableAxis);
            assertEquals(expectedChunk.positionAxis1, actualChunk.positionAxis1);
            assertEquals(expectedChunk.positionAxis2, actualChunk.positionAxis2);
            assertEquals(expectedChunk.energyAxis, actualChunk.energyAxis);
            assertEquals(expectedChunk.timeAxis, actualChunk.timeAxis);
            assertEquals(expectedChunk.polarizationAxis, actualChunk.polarizationAxis);
            
            compareObservableAxis(expectedChunk.observable, actualChunk.observable);
            compareSpatialWCS(expectedChunk.position, actualChunk.position);
            compareSpectralWCS(expectedChunk.energy, actualChunk.energy);
            compareTemporalWCS(expectedChunk.time, actualChunk.time);
            comparePolarizationWCS(expectedChunk.polarization, actualChunk.polarization);
        }
    }
    
    protected void compareObservableAxis(ObservableAxis expected, ObservableAxis actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareSlice(expected.getDependent(), actual.getDependent());
        compareSlice(expected.independent, actual.independent);
    }
    
    protected void compareSpatialWCS(SpatialWCS expected, SpatialWCS actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareCoordAxis2D(expected.getAxis(), actual.getAxis());
        assertEquals(expected.coordsys, actual.coordsys);
        assertEquals(expected.equinox, actual.equinox);
        assertEquals(expected.resolution, actual.resolution);
    }
    
    protected void compareSpectralWCS(SpectralWCS expected, SpectralWCS actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
        assertEquals(expected.bandpassName, actual.bandpassName);
        assertEquals(expected.resolvingPower, actual.resolvingPower);
        assertEquals(expected.restfrq, actual.restfrq);
        assertEquals(expected.restwav, actual.restwav);
        assertEquals(expected.getSpecsys(), actual.getSpecsys());
        assertEquals(expected.ssysobs, actual.ssysobs);
        assertEquals(expected.ssyssrc, actual.ssyssrc);
        assertEquals(expected.velang, actual.velang);
        assertEquals(expected.velosys, actual.velosys);
        assertEquals(expected.zsource, actual.zsource);
    }
    
    protected void compareTemporalWCS(TemporalWCS expected, TemporalWCS actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
        assertEquals(expected.exposure, actual.exposure);
        assertEquals(expected.resolution, actual.resolution);
    }
    
    protected void comparePolarizationWCS(PolarizationWCS expected, PolarizationWCS actual)
    {
        if (expected == null && actual == null)
            return;
        
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
    }
    
    protected void compareAxis(Axis expected, Axis actual)
    {        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.getCtype());
        assertNotNull(actual.getCunit());
        
        assertEquals(expected.getCtype(), actual.getCtype());
        assertEquals(expected.getCunit(), actual.getCunit());
    }
    
    protected void compareValueCoord2(ValueCoord2D expected, ValueCoord2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.coord1, actual.coord1, 0.0);
        assertEquals(expected.coord2, actual.coord2, 0.0);
    }
    
    protected void compareCoord2D(Coord2D expected, Coord2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareRefCoord(expected.getCoord1(), actual.getCoord1());
        compareRefCoord(expected.getCoord2(), actual.getCoord2());
    }
    
    protected void compareCoordAxis1D(CoordAxis1D expected, CoordAxis1D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareCoordError(expected.error, actual.error);
        compareCoordRange1D(expected.range, actual.range);
        compareCoordBounds1D(expected.bounds, actual.bounds);
        compareCoordFunction1D(expected.function, actual.function);
    }
    
    protected void compareCoordAxis2D(CoordAxis2D expected, CoordAxis2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);

        assertNotNull(actual.getAxis1());
        assertNotNull(actual.getAxis2());
        
        compareAxis(expected.getAxis1(), actual.getAxis1());
        compareAxis(expected.getAxis2(), actual.getAxis2());
        compareCoordError(expected.error1, actual.error1);
        compareCoordError(expected.error2, actual.error2);
        compareCoordRange2D(expected.range, actual.range);
        compareCoordBounds2D(expected.bounds, actual.bounds);
        compareCoordFunction2D(expected.function, actual.function);
    }
    
    protected void compareCoordBounds1D(CoordBounds1D expected, CoordBounds1D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(expected.getSamples());
        assertNotNull(actual.getSamples());
        assertEquals(expected.getSamples().size(), actual.getSamples().size());
        
        Iterator actualIter = expected.getSamples().iterator();
        Iterator expectedIter = actual.getSamples().iterator();
        while (expectedIter.hasNext())
        {
            CoordRange1D expectedRange = (CoordRange1D) expectedIter.next();
            CoordRange1D actualRange = (CoordRange1D) actualIter.next();
            compareCoordRange1D(expectedRange, actualRange);
        }
    }
    
    protected void compareCoordBounds2D(CoordBounds2D expected, CoordBounds2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        if (expected instanceof CoordCircle2D && actual instanceof CoordCircle2D)
            compareCoordCircle2D((CoordCircle2D) expected, (CoordCircle2D) actual);
        else if (expected instanceof CoordPolygon2D && actual instanceof CoordPolygon2D)
            compareCoordPolygon2D((CoordPolygon2D) expected, (CoordPolygon2D) actual);
        else
            fail("CoordBounds2D expected and actual are different types.");
                
    }
    
    protected void compareCoordCircle2D(CoordCircle2D expected, CoordCircle2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.getCenter());
        assertNotNull(actual.getRadius());
        compareValueCoord2(expected.getCenter(), actual.getCenter());
        assertEquals(expected.getRadius(), actual.getRadius());
    }
    
    protected void compareCoordError(CoordError expected, CoordError actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.syser);
        assertNotNull(actual.rnder);
        
        assertEquals(expected.syser, actual.syser);
        assertEquals(expected.rnder, actual.rnder);
    }
    
    protected void compareCoordFunction1D(CoordFunction1D expected, CoordFunction1D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.getNaxis(), actual.getNaxis());
        assertEquals(expected.getDelta(), actual.getDelta());
        compareRefCoord(expected.getRefCoord(), actual.getRefCoord());
    }
    
    protected void compareCoordFunction2D(CoordFunction2D expected, CoordFunction2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.getDimension());
        assertNotNull(actual.getRefCoord());
        assertNotNull(actual.getCd11());
        assertNotNull(actual.getCd12());
        assertNotNull(actual.getCd21());
        assertNotNull(actual.getCd22());
        
        compareDimension2D(expected.getDimension(), actual.getDimension());
        compareCoord2D(expected.getRefCoord(), actual.getRefCoord());
        assertEquals(expected.getCd11(), actual.getCd11(), 0.0);
        assertEquals(expected.getCd12(), actual.getCd12(), 0.0);
        assertEquals(expected.getCd21(), actual.getCd21(), 0.0);
        assertEquals(expected.getCd22(), actual.getCd22(), 0.0);
    }
    
    protected void compareCoordPolygon2D(CoordPolygon2D expected, CoordPolygon2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(expected.getVertices());
        assertNotNull(actual.getVertices());
        assertEquals(expected.getVertices().size(), actual.getVertices().size());
        
        Iterator<ValueCoord2D> actualIter = expected.getVertices().iterator();
        Iterator<ValueCoord2D> expectedIter = actual.getVertices().iterator();
        while (expectedIter.hasNext())
        {
            ValueCoord2D expectedCoord2D = expectedIter.next();
            ValueCoord2D actualCoord2D = actualIter.next();
            compareValueCoord2(expectedCoord2D, actualCoord2D);
        }
    }
    
    protected void compareCoordRange1D(CoordRange1D expected, CoordRange1D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        compareRefCoord(expected.getStart(), actual.getStart());
        compareRefCoord(expected.getEnd(), actual.getEnd());
    }
    
    protected void compareCoordRange2D(CoordRange2D expected, CoordRange2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.getStart());
        assertNotNull(actual.getEnd());
        
        compareCoord2D(expected.getStart(), actual.getStart());
        compareCoord2D(expected.getEnd(), actual.getEnd());
    }
    
    protected void compareDimension2D(Dimension2D expected, Dimension2D actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);

        assertEquals(expected.naxis1, actual.naxis1);
        assertEquals(expected.naxis2, actual.naxis2);
    }
    
    protected void compareRefCoord(RefCoord expected, RefCoord actual)
    {
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertEquals(expected.pix, actual.pix, 0.0);
        assertEquals(expected.val, actual.val, 0.0);
    }
    
    protected void compareSlice(Slice expected, Slice actual)
    {        
        assertNotNull(expected);
        assertNotNull(actual);
        
        assertNotNull(actual.getBin());
        assertNotNull(actual.getAxis());
        
        assertEquals(expected.getBin(), actual.getBin());
        compareAxis(expected.getAxis(), actual.getAxis());
    }

}
