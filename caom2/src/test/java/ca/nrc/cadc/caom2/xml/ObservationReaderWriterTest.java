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
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.caom2.xml;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.CustomAxis;
import ca.nrc.cadc.caom2.DataQuality;
import ca.nrc.cadc.caom2.DerivedObservation;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.EnergyBand;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Metrics;
import ca.nrc.cadc.caom2.Observable;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
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
import ca.nrc.cadc.caom2.Visibility;
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
import ca.nrc.cadc.caom2.wcs.CustomWCS;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
import ca.nrc.cadc.caom2.wcs.PolarizationWCS;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.SpatialWCS;
import ca.nrc.cadc.caom2.wcs.SpectralWCS;
import ca.nrc.cadc.caom2.wcs.TemporalWCS;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class ObservationReaderWriterTest {

    private static Logger log = Logger.getLogger(ObservationReaderWriterTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.xml", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.xml", Level.INFO);
        Log4jInit.setLevel("org.opencadc.persist", Level.INFO);
    }

    public ObservationReaderWriterTest() {
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
    public void testLenientTimestampParser() {
        try {
            Caom2TestInstances ti = new Caom2TestInstances();
            ti.setComplete(false);
            ti.setDepth(1);
            Observation o = ti.getSimpleObservation();

            String ivoaDateStr = "2017-08-15T12:34:56.000";
            String truncDateStr = "2017-08-15T12:34:56";

            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            Date ts = df.parse(ivoaDateStr);

            CaomUtil.assignLastModified(o, ts, "lastModified");
            CaomUtil.assignLastModified(o, ts, "maxLastModified");

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObservationWriter w = new ObservationWriter();
            w.write(o, bos);
            String xml = bos.toString();
            log.debug("testLenientTimestampParser[before]:\n" + xml);
            String xml2 = xml.replaceAll(ivoaDateStr, truncDateStr);
            log.debug("testLenientTimestampParser[after]:\n" + xml2);

            ObservationReader r = new ObservationReader();
            Observation o2 = r.read(xml2);
            compareObservations(o, o2);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // this "test" writes out a pretty complete document to use in comparison with python round-trip
    // and python meta checksum computations
    @Test
    public void doWriteComplete() {
        try {
            Caom2TestInstances ti = new Caom2TestInstances();
            ti.setComplete(true);
            ti.setDepth(5);
            ti.setChildCount(2);
            Observation o = ti.getDerivedObservation();

            long t1 = new Date().getTime();
            long t2 = t1 + 2000l;

            MessageDigest digest = MessageDigest.getInstance("MD5");
            CaomUtil.assignLastModified(o, new Date(t1), "lastModified");
            CaomUtil.assignLastModified(o, new Date(t2), "maxLastModified");
            URI ocs = o.computeMetaChecksum(digest);
            URI oacs = o.computeAccMetaChecksum(digest);
            CaomUtil.assignMetaChecksum(o, ocs, "metaChecksum");
            CaomUtil.assignMetaChecksum(o, oacs, "accMetaChecksum");
            for (Plane pl : o.getPlanes()) {
                CaomUtil.assignLastModified(pl, new Date(t1), "lastModified");
                CaomUtil.assignLastModified(pl, new Date(t2), "maxLastModified");
                CaomUtil.assignMetaChecksum(pl, pl.computeMetaChecksum(digest), "metaChecksum");
                CaomUtil.assignMetaChecksum(pl, pl.computeAccMetaChecksum(digest), "accMetaChecksum");
                for (Artifact ar : pl.getArtifacts()) {
                    CaomUtil.assignLastModified(ar, new Date(t1), "lastModified");
                    CaomUtil.assignLastModified(ar, new Date(t2), "maxLastModified");
                    CaomUtil.assignMetaChecksum(ar, ar.computeMetaChecksum(digest), "metaChecksum");
                    CaomUtil.assignMetaChecksum(ar, ar.computeAccMetaChecksum(digest), "accMetaChecksum");
                    for (Part pa : ar.getParts()) {
                        CaomUtil.assignLastModified(pa, new Date(t1), "lastModified");
                        CaomUtil.assignLastModified(pa, new Date(t2), "maxLastModified");
                        CaomUtil.assignMetaChecksum(pa, pa.computeMetaChecksum(digest), "metaChecksum");
                        CaomUtil.assignMetaChecksum(pa, pa.computeAccMetaChecksum(digest), "accMetaChecksum");
                        for (Chunk ch : pa.getChunks()) {
                            CaomUtil.assignLastModified(ch, new Date(t1), "lastModified");
                            CaomUtil.assignLastModified(ch, new Date(t2), "maxLastModified");
                            CaomUtil.assignMetaChecksum(ch, ch.computeMetaChecksum(digest), "metaChecksum");
                            CaomUtil.assignMetaChecksum(ch, ch.computeAccMetaChecksum(digest), "accMetaChecksum");
                        }
                    }
                }
            }

            File f = new File("sample-derived-caom25.xml");
            FileOutputStream fos = new FileOutputStream(f);
            ObservationWriter w = new ObservationWriter();
            w.write(o, fos);
            fos.close();
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSupportAllVersions() {
        try {
            Observation obs = new SimpleObservation("FOO", "bar");

            ObservationReader validatingReader = new ObservationReader();
            ObservationReader nonvalidatingReader = new ObservationReader(false);

            ByteArrayOutputStream bos;

            ObservationWriter w24 = new ObservationWriter("caom2", XmlConstants.CAOM2_4_NAMESPACE, false);
            bos = new ByteArrayOutputStream();
            w24.write(obs, bos);
            String caom24 = bos.toString();
            log.info("caom-2.4 XML:\n" + caom24);
            Assert.assertTrue(caom24.contains(XmlConstants.CAOM2_4_NAMESPACE));
            Observation obs24 = nonvalidatingReader.read(caom24);
            compareObservations(obs, obs24);

            // add maxLastModified and meta checksums
            CaomUtil.assignLastModified(obs, new Date(), "maxLastModified");
            MessageDigest digest = MessageDigest.getInstance("MD5");
            URI ocs = obs.computeMetaChecksum(digest);
            URI oacs = obs.computeAccMetaChecksum(digest);
            CaomUtil.assignMetaChecksum(obs, ocs, "metaChecksum");
            CaomUtil.assignMetaChecksum(obs, oacs, "accMetaChecksum");
            bos = new ByteArrayOutputStream();
            w24.write(obs, bos);
            caom24 = bos.toString();
            log.info("caom-2.4 XML:\n" + caom24);
            Assert.assertTrue(caom24.contains(XmlConstants.CAOM2_4_NAMESPACE));
            obs24 = nonvalidatingReader.read(caom24);
            compareObservations(obs, obs24);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCleanWhitespace() {
        try {
            ObservationReader r = new ObservationReader();

            String actual;

            actual = r.cleanWhitespace(null);
            Assert.assertNull(actual);

            actual = r.cleanWhitespace("");
            Assert.assertNull(actual);

            actual = r.cleanWhitespace("foo");
            Assert.assertEquals("foo", actual);

            actual = r.cleanWhitespace("  trimmed  ");
            Assert.assertEquals("trimmed", actual);

            actual = r.cleanWhitespace("  trim outside only  ");
            Assert.assertEquals("trim outside only", actual);

            actual = r.cleanWhitespace("  trim  multiple \t inside  ");
            Assert.assertEquals("trim  multiple \t inside", actual); // leave inside alone

            actual = r.cleanWhitespace("  trim\njunk\rinside\tphrase  ");
            Assert.assertEquals("trim\njunk\rinside\tphrase", actual); // leave inside alone
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testReadNull() {
        try {
            ObservationReader r = new ObservationReader();

            try {
                r.read((String) null);
            } catch (IllegalArgumentException expected) {
            }

            try {
                r.read((InputStream) null);
            } catch (IllegalArgumentException expected) {
            }

            try {
                r.read((Reader) null);
            } catch (IllegalArgumentException expected) {
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCleanWhitespaceRoundTrip() {
        try {
            ObservationReader r = new ObservationReader();
            SimpleObservation observation = new SimpleObservation("FOO", URI.create("caom:FOO/bar"), SimpleObservation.EXPOSURE);
            observation.telescope = new Telescope("bar\tbaz\n1.0");
            StringBuilder sb = new StringBuilder();
            ObservationWriter writer = new ObservationWriter();
            writer.setWriteEmptyCollections(false);
            writer.write(observation, sb);
            log.debug(sb.toString());

            // do not validate the XML.
            ObservationReader reader = new ObservationReader(false);
            Observation returned = reader.read(sb.toString());

            Assert.assertEquals(observation.getCollection(), returned.getCollection());
            Assert.assertEquals(observation.getURI(), returned.getURI());
            Assert.assertNotNull("has telescope", returned.telescope);
            // no longer sanitising
            Assert.assertEquals("bar\tbaz\n1.0", returned.telescope.getName());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMinimalSimple() {
        try {

            for (int i = 1; i <= 5; i++) {
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
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testRountTrip25() {
        try {
            final ObservationWriter w = new ObservationWriter();
            final ObservationReader r = new ObservationReader(true);
            
            Caom2TestInstances ti = new Caom2TestInstances();
            ti.setComplete(true);
            ti.setDepth(2); // changes in Observation and Plane only
            ti.setChildCount(2);
            Observation obs = ti.getDerivedObservation();
            
            StringBuilder sb = new StringBuilder();
            w.write(obs, sb);
            String xml = sb.toString();
            PrintWriter pw = new PrintWriter(new File("orig.xml"));
            pw.print(xml);
            pw.close();
            log.info("simple: \n" + xml);
            Observation rt = r.read(xml);
            log.info("read: " + rt.getClass().getSimpleName() + " " + rt.getURI());

            sb = new StringBuilder();
            w.write(rt, sb);
            xml = sb.toString();
            pw = new PrintWriter(new File("roundtrip.xml"));
            pw.print(xml);
            pw.close();
            
            compareObservations(obs, rt);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCompleteSimple() {
        try {
            for (int i = 1; i <= 5; i++) {
                log.info("testCompleteSimple: depth = " + i);
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
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMinimalComposite() {
        try {
            for (int i = 1; i < 6; i++) {
                log.info("testMinimalComposite: depth = " + i);
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                DerivedObservation observation = getMinimalDerived(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);

                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getMinimalDerived(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCompleteComposite() {
        try {
            for (int i = 1; i < 6; i++) {
                log.info("testCompleteComposite: depth = " + i);
                // CoordBounds2D as CoordCircle2D
                boolean boundsIsCircle = true;
                DerivedObservation observation = getCompleteDerived(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);

                // CoordBounds2D as CoordPolygon2D
                boundsIsCircle = false;
                observation = getCompleteDerived(i, boundsIsCircle);

                // Write empty elements.
                testObservation(observation, true);

                // Do not write empty elements.
                testObservation(observation, false);
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    protected SimpleObservation getMinimalSimple(int depth, boolean boundsIsCircle)
            throws Exception {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(false);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getSimpleObservation();
    }

    protected SimpleObservation getCompleteSimple(int depth, boolean boundsIsCircle)
            throws Exception {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(true);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getSimpleObservation();
    }

    protected DerivedObservation getMinimalDerived(int depth, boolean boundsIsCircle)
            throws Exception {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(false);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getDerivedObservation();
    }

    protected DerivedObservation getCompleteDerived(int depth, boolean boundsIsCircle)
            throws Exception {
        Caom2TestInstances instances = new Caom2TestInstances();
        instances.setComplete(true);
        instances.setDepth(depth);
        instances.setBoundsIsCircle(boundsIsCircle);
        return instances.getDerivedObservation();
    }

    protected void testObservation(Observation observation, boolean writeEmptyCollections)
            throws Exception {
        testObservation(observation, writeEmptyCollections, null, null, true);
    }

    protected void testObservation(Observation observation, boolean writeEmptyCollections, String nsPrefix, String forceNS, boolean schemaVal)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        ObservationWriter writer = null;
        if (forceNS != null) {
            writer = new ObservationWriter(nsPrefix, forceNS, writeEmptyCollections);
        } else {
            writer = new ObservationWriter();
        }

        writer.setWriteEmptyCollections(writeEmptyCollections);
        writer.write(observation, sb);
        log.info(sb.toString());

        // well-formed XML
        ObservationReader reader = new ObservationReader(false);
        Observation returned = reader.read(sb.toString());

        compareObservations(observation, returned);

        if (!schemaVal) {
            return;
        }

        // valid XML
        reader = new ObservationReader(true);
        returned = reader.read(sb.toString());

        compareObservations(observation, returned);
    }

    protected void compareEntity(CaomEntity expected, CaomEntity actual) throws NoSuchAlgorithmException {
        Assert.assertEquals("type", expected.getClass().getName(), actual.getClass().getName());
        String t = expected.getClass().getSimpleName();
        Assert.assertEquals(expected.getID(), actual.getID());
        if (expected.getLastModified() != null) {
            Assert.assertNotNull(t + ".lastModified", actual.getLastModified());
            Assert.assertEquals(t + ".lastModified", expected.getLastModified().getTime(), actual.getLastModified().getTime());
        }
        if (expected.getMaxLastModified() != null) {
            Assert.assertNotNull(t + ".maxLastModified", actual.getMaxLastModified());
            Assert.assertEquals(t + ".maxLastModified", expected.getMaxLastModified().getTime(), actual.getMaxLastModified().getTime());
        }
        if (expected.getMetaChecksum() != null) {
            Assert.assertNotNull(t + ".metaChecksum", actual.getMetaChecksum());
            Assert.assertEquals(t + ".metaChecksum", expected.getMetaChecksum(), actual.getMetaChecksum());
        }
        if (expected.getAccMetaChecksum() != null) {
            Assert.assertNotNull(t + ".accMetaChecksum", actual.getAccMetaChecksum());
            Assert.assertEquals(t + ".accMetaChecksum", expected.getAccMetaChecksum(), actual.getAccMetaChecksum());
        }
        if (expected.metaProducer == null) {
            Assert.assertNull(t + ".metaProducer", actual.metaProducer);
        } else {
            Assert.assertEquals(t + ".metaProducer", expected.metaProducer, actual.metaProducer);
        }

        // verify checksums
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        URI expectedCS = expected.computeMetaChecksum(md5);
        URI actualCS = actual.computeMetaChecksum(md5);
        Assert.assertEquals(expected.getClass().getSimpleName() + ".metaChecksum recomp", expectedCS, actualCS);

        URI expectedAcc = actual.computeAccMetaChecksum(md5);
        URI actualAcc = actual.computeAccMetaChecksum(md5);
        Assert.assertEquals(actual.getClass().getSimpleName() + ".accMetaChecksum recomp", expectedAcc, actualAcc);
    }

    protected void compareObservations(Observation expected, Observation actual) throws NoSuchAlgorithmException {
        Assert.assertEquals(expected.getCollection(), actual.getCollection());
        Assert.assertEquals(expected.getURI(), actual.getURI());
        Assert.assertEquals(expected.getAlgorithm().getName(), actual.getAlgorithm().getName());

        Assert.assertEquals(expected.metaRelease, actual.metaRelease);
        compareSets("observation.metaReadGroups", expected.getMetaReadGroups(), actual.getMetaReadGroups());
        Assert.assertEquals(expected.getMetaReadGroups().size(), actual.getMetaReadGroups().size());

        compareProposal(expected.proposal, actual.proposal);
        compareTarget(expected.target, actual.target);
        compareTargetPosition(expected.targetPosition, actual.targetPosition);
        compare(expected.requirements, actual.requirements);
        compareTelescope(expected.telescope, actual.telescope);
        compareInstrument(expected.instrument, actual.instrument);
        compareEnvironment(expected.environment, actual.environment);

        comparePlanes(expected.getPlanes(), actual.getPlanes());

        if (expected instanceof DerivedObservation && actual instanceof DerivedObservation) {
            compareSets("observation.members", ((DerivedObservation) expected).getMembers(), ((DerivedObservation) actual).getMembers());
        }

        compareEntity(expected, actual);
    }

    protected void compareProposal(Proposal expected, Proposal actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getID(), actual.getID());
        Assert.assertEquals(expected.pi, actual.pi);
        Assert.assertEquals(expected.project, actual.project);
        Assert.assertEquals(expected.title, actual.title);
        Assert.assertEquals(expected.reference, actual.reference);
        Assert.assertEquals(expected.getKeywords(), actual.getKeywords());
    }

    protected void compareTarget(Target expected, Target actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.targetID, actual.targetID);
        Assert.assertEquals(expected.type, actual.type);
        Assert.assertEquals(expected.redshift, actual.redshift);
        Assert.assertEquals(expected.getKeywords(), actual.getKeywords());
    }

    protected void compareTargetPosition(TargetPosition expected, TargetPosition actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getCoordsys(), actual.getCoordsys());
        comparePoint(expected.getCoordinates(), actual.getCoordinates());
        Assert.assertEquals(expected.equinox, actual.equinox);
    }

    protected void comparePoint(Point expected, Point actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.getLongitude(), actual.getLongitude(), 0.0);
        Assert.assertEquals(expected.getLatitude(), actual.getLatitude(), 0.0);
    }

    protected void compareTelescope(Telescope expected, Telescope actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.geoLocationX, actual.geoLocationX);
        Assert.assertEquals(expected.geoLocationY, actual.geoLocationY);
        Assert.assertEquals(expected.geoLocationZ, actual.geoLocationZ);
        Assert.assertEquals(expected.trackingMode, actual.trackingMode);
        Assert.assertEquals(expected.getKeywords(), actual.getKeywords());
    }

    protected void compareInstrument(Instrument expected, Instrument actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getKeywords(), actual.getKeywords());
    }

    protected void compareEnvironment(Environment expected, Environment actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }

        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.seeing, actual.seeing, 0.0);
        Assert.assertEquals(expected.humidity, actual.humidity, 0.0);
        Assert.assertEquals(expected.elevation, actual.elevation, 0.0);
        Assert.assertEquals(expected.tau, actual.tau, 0.0);
        Assert.assertEquals(expected.wavelengthTau, actual.wavelengthTau, 0.0);
        Assert.assertEquals(expected.ambientTemp, actual.ambientTemp, 0.0);
        Assert.assertEquals(expected.photometric, actual.photometric);

    }

    protected void compareSets(String label, Set<URI> expected, Set<URI> actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(label, expected);
        Assert.assertNotNull(label, actual);
        Assert.assertEquals(label + ".size", expected.size(), actual.size());

        Iterator<URI> actualIter = expected.iterator();
        Iterator<URI> expectedIter = actual.iterator();
        while (expectedIter.hasNext()) {
            URI expectedUri = expectedIter.next();
            URI actualUri = actualIter.next();
            Assert.assertEquals(label + ".uri", expectedUri, actualUri);
        }
    }

    protected void compareSets(Set<URI> expected, Set<URI> actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());

        Iterator<URI> actualIter = expected.iterator();
        Iterator<URI> expectedIter = actual.iterator();
        while (expectedIter.hasNext()) {
            URI expectedUri = expectedIter.next();
            URI actualUri = actualIter.next();

            Assert.assertNotNull(expectedUri);
            Assert.assertNotNull(actualUri);
            Assert.assertEquals(expectedUri, actualUri);
        }
    }

    protected void comparePlanes(Set<Plane> expected, Set<Plane> actual) throws NoSuchAlgorithmException {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());

        Iterator expectedIter = expected.iterator();
        Iterator actualIter = actual.iterator();
        while (expectedIter.hasNext()) {
            Plane expectedPlane = (Plane) expectedIter.next();
            Plane actualPlane = (Plane) actualIter.next();

            Assert.assertNotNull(expectedPlane);
            Assert.assertNotNull(actualPlane);

            Assert.assertEquals(expectedPlane.getURI(), actualPlane.getURI());
            Assert.assertEquals(expectedPlane.metaRelease, actualPlane.metaRelease);
            compareSets("plane.metaReadGroups", expectedPlane.getMetaReadGroups(), actualPlane.getMetaReadGroups());
            Assert.assertEquals(expectedPlane.dataRelease, actualPlane.dataRelease);
            compareSets("plane.dataReadGroups", expectedPlane.getDataReadGroups(), actualPlane.getDataReadGroups());
            Assert.assertEquals(expectedPlane.dataProductType, actualPlane.dataProductType);
            Assert.assertEquals(expectedPlane.calibrationLevel, actualPlane.calibrationLevel);

            compare(expectedPlane.metrics, actualPlane.metrics);
            
            compare(expectedPlane.observable, actualPlane.observable);
            compare(expectedPlane.position, actualPlane.position);
            compare(expectedPlane.energy, actualPlane.energy);
            compare(expectedPlane.time, actualPlane.time);
            compare(expectedPlane.polarization, actualPlane.polarization);
            compare(expectedPlane.custom, actualPlane.custom);
            compare(expectedPlane.visibility, actualPlane.visibility);

            compareDataQuality(expectedPlane.quality, actualPlane.quality);
            compareProvenance(expectedPlane.provenance, actualPlane.provenance);

            compareArtifacts(expectedPlane.getArtifacts(), actualPlane.getArtifacts());

            compareEntity(expectedPlane, actualPlane);
        }
    }

    protected void compare(Requirements expected, Requirements actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.getFlag(), actual.getFlag());
    }

    protected void compare(Metrics expected, Metrics actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compare(expected.background, actual.background);
        compare(expected.backgroundStddev, actual.backgroundStddev);
        compare(expected.fluxDensityLimit, actual.fluxDensityLimit);
        compare(expected.magLimit, actual.magLimit);
        compare(expected.sourceNumberDensity, actual.sourceNumberDensity);
        compare(expected.sampleSNR, actual.sampleSNR);
    }

    protected void compare(Double e, Double a) {
        if (e == null) {
            Assert.assertNull(a);
        } else {
            Assert.assertEquals(e, a, 0.0);
        }
    }

    protected void compare(Observable expected, Observable actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.getUCD(), actual.getUCD());
        Assert.assertEquals(expected.calibration, actual.calibration);
    }

    protected void compare(Position expected, Position actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        compare(expected.getBounds(), actual.getBounds());
        compare(expected.getSamples(), actual.getSamples());
        
        if (expected.minBounds == null) {
            Assert.assertNull(actual.minBounds);
        } else {
            compare(expected.minBounds, actual.minBounds);
        }
        if (expected.dimension == null) {
            Assert.assertNull(actual.dimension);
        } else {
            Assert.assertEquals(expected.dimension.naxis1, actual.dimension.naxis1);
            Assert.assertEquals(expected.dimension.naxis2, actual.dimension.naxis2);
        }
        compare(expected.maxAngularScale, actual.maxAngularScale);
        compare(expected.resolution, actual.resolution);
        compare(expected.resolutionBounds, actual.resolutionBounds);
        compare(expected.sampleSize, actual.sampleSize);
        Assert.assertEquals(expected.calibration, actual.calibration);
    }

    protected void compare(Energy expected, Energy actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertEquals(expected.getBounds(), actual.getBounds());
        Assert.assertEquals(expected.getSamples(), actual.getSamples());
        
        Assert.assertEquals("energyBands", expected.getEnergyBands().size(), actual.getEnergyBands().size());
        Iterator<EnergyBand> ebs = expected.getEnergyBands().iterator();
        Iterator<EnergyBand> abs = actual.getEnergyBands().iterator();
        while (ebs.hasNext()) {
            Assert.assertEquals(ebs.next(), abs.next());
        }
        Assert.assertEquals(expected.dimension, actual.dimension);
        Assert.assertEquals(expected.resolvingPower, actual.resolvingPower);
        compare(expected.resolvingPowerBounds, actual.resolvingPowerBounds);
        Assert.assertEquals(expected.resolution, actual.resolution);
        compare(expected.resolutionBounds, actual.resolutionBounds);
        Assert.assertEquals(expected.sampleSize, actual.sampleSize);
        Assert.assertEquals(expected.bandpassName, actual.bandpassName);
        Assert.assertEquals(expected.rest, actual.rest);
        if (expected.transition == null) {
            Assert.assertNull(actual.transition);
        } else {
            Assert.assertEquals(expected.transition.getSpecies(), actual.transition.getSpecies());
            Assert.assertEquals(expected.transition.getTransition(), actual.transition.getTransition());
        }
        Assert.assertEquals(expected.rest, actual.rest);
        Assert.assertEquals(expected.calibration, actual.calibration);
    }

    protected void compare(Time expected, Time actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertEquals(expected.getBounds(), actual.getBounds());
        Assert.assertEquals(expected.getSamples(), actual.getSamples());

        Assert.assertEquals(expected.dimension, actual.dimension);
        Assert.assertEquals(expected.resolution, actual.resolution);
        compare(expected.resolutionBounds, actual.resolutionBounds);
        Assert.assertEquals(expected.exposure, actual.exposure);
        compare(expected.exposureBounds, actual.exposureBounds);
        Assert.assertEquals(expected.sampleSize, actual.sampleSize);
        Assert.assertEquals(expected.calibration, actual.calibration);
    }

    protected void compare(Polarization expected, Polarization actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertEquals(expected.dimension, actual.dimension);
        Assert.assertEquals(expected.getStates().size(), actual.getStates().size());
        // states is in canonical order already
        Iterator<PolarizationState> ei = expected.getStates().iterator();
        Iterator<PolarizationState> ai = actual.getStates().iterator();
        while (ei.hasNext()) {
            Assert.assertEquals(ei.next(), ai.next());
        }
    }

    protected void compare(CustomAxis expected, CustomAxis actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertNotNull("plane.custom", actual);
        Assert.assertEquals("CustomAxis.ctype", expected.getCtype(), actual.getCtype());

        Assert.assertEquals(expected.getBounds().getLower(), actual.getBounds().getLower(), 0.0);
        Assert.assertEquals(expected.getBounds().getUpper(), actual.getBounds().getUpper(), 0.0);
        // TODO: samples
        
        Assert.assertEquals(expected.dimension, actual.dimension);
    }

    protected void compare(Visibility expected, Visibility actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertNotNull("plane.visibility", actual);
        compare(expected.getDistance(), actual.getDistance());
        compare(expected.getDistributionEccentricity(), actual.getDistributionEccentricity());
        compare(expected.getDistributionFill(), actual.getDistributionFill());
    }

    protected void compare(MultiShape expected, MultiShape actual) {
        Assert.assertEquals(expected.getShapes().size(), actual.getShapes().size());
        Iterator<Shape> ei = expected.getShapes().iterator();
        Iterator<Shape> ai = actual.getShapes().iterator();
        while (ei.hasNext()) {
            compare(ei.next(), ai.next());
        }
    }

    protected void compare(Shape expected, Shape actual) {
        Assert.assertEquals(expected.getClass().getName(), actual.getClass().getName());
        if (expected instanceof Circle) {
            Circle ec = (Circle) expected;
            Circle ac = (Circle) actual;
            Assert.assertEquals(ec.getCenter().getLongitude(), ac.getCenter().getLongitude(), 0.0);
            Assert.assertEquals(ec.getCenter().getLatitude(), ac.getCenter().getLatitude(), 0.0);
            Assert.assertEquals(ec.getRadius(), ac.getRadius(), 0.0);
        } else if (expected instanceof Polygon) {
            Polygon ep = (Polygon) expected;
            Polygon ap = (Polygon) actual;
            Assert.assertEquals(ep.getVertices().size(), ap.getVertices().size());
            for (int i = 0; i < ep.getVertices().size(); i++) {
                Point ept = ep.getVertices().get(i);
                Point apt = ap.getVertices().get(i);
                Assert.assertEquals(ept.getLongitude(), apt.getLongitude(), 0.0);
                Assert.assertEquals(ept.getLatitude(), apt.getLatitude(), 0.0);
            }
        } else {
            throw new RuntimeException("TEST BUG: unexpected shape type: " + expected.getClass().getName());
        }
    }

    protected void compare(List<DoubleInterval> expected, List<DoubleInterval> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        Iterator<DoubleInterval> ei = expected.iterator();
        Iterator<DoubleInterval> ai = actual.iterator();
        while (ei.hasNext()) {
            compare(ei.next(), ai.next());
        }
    }

    protected void compare(DoubleInterval expected, DoubleInterval actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        Assert.assertEquals(expected.getLower(), actual.getLower(), 0.0);
        Assert.assertEquals(expected.getUpper(), actual.getUpper(), 0.0);
    }

    protected void compareDataQuality(DataQuality expected, DataQuality actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.getFlag(), actual.getFlag());
    }

    protected void compareProvenance(Provenance expected, Provenance actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.version, actual.version);
        Assert.assertEquals(expected.project, actual.project);
        Assert.assertEquals(expected.producer, actual.producer);
        Assert.assertEquals(expected.runID, actual.runID);
        Assert.assertEquals(expected.reference, actual.reference);
        Assert.assertEquals(expected.lastExecuted, actual.lastExecuted);
        compareSets("provenance.inputs", expected.getInputs(), actual.getInputs());
    }

    protected void compareArtifacts(Set<Artifact> expected, Set<Artifact> actual) throws NoSuchAlgorithmException {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());

        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext()) {
            Artifact expectedArtifact = (Artifact) expectedIter.next();
            Artifact actualArtifact = (Artifact) actualIter.next();

            Assert.assertNotNull(expectedArtifact);
            Assert.assertNotNull(actualArtifact);

            Assert.assertEquals(expectedArtifact.getURI(), actualArtifact.getURI());
            Assert.assertEquals(expectedArtifact.getProductType(), actualArtifact.getProductType());
            Assert.assertEquals(expectedArtifact.getReleaseType(), actualArtifact.getReleaseType());

            Assert.assertEquals(expectedArtifact.contentType, actualArtifact.contentType);
            Assert.assertEquals(expectedArtifact.contentLength, actualArtifact.contentLength);
            Assert.assertEquals(expectedArtifact.contentChecksum, actualArtifact.contentChecksum);
            Assert.assertEquals(expectedArtifact.contentRelease, actualArtifact.contentRelease);
            compareSets("Artifact.contentReadGroups", expectedArtifact.getContentReadGroups(), actualArtifact.getContentReadGroups());

            compareParts(expectedArtifact.getParts(), expectedArtifact.getParts());

            compareEntity(expectedArtifact, actualArtifact);
        }
    }

    protected void compareParts(Set<Part> expected, Set<Part> actual) throws NoSuchAlgorithmException {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());

        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext()) {
            Part expectedPart = (Part) expectedIter.next();
            Part actualPart = (Part) actualIter.next();

            Assert.assertNotNull(expectedPart);
            Assert.assertNotNull(actualPart);

            Assert.assertEquals(expectedPart.getName(), actualPart.getName());
            Assert.assertEquals(expectedPart.productType, actualPart.productType);

            compareChunks(expectedPart.getChunks(), actualPart.getChunks());

            compareEntity(expectedPart, actualPart);
        }
    }

    protected void compareChunks(Set<Chunk> expected, Set<Chunk> actual) throws NoSuchAlgorithmException {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());

        Iterator actualIter = expected.iterator();
        Iterator expectedIter = actual.iterator();
        while (expectedIter.hasNext()) {
            Chunk expectedChunk = (Chunk) expectedIter.next();
            Chunk actualChunk = (Chunk) actualIter.next();

            Assert.assertNotNull(expectedChunk);
            Assert.assertNotNull(actualChunk);

            Assert.assertEquals(expectedChunk.productType, actualChunk.productType);
            Assert.assertEquals(expectedChunk.naxis, actualChunk.naxis);
            Assert.assertEquals(expectedChunk.observableAxis, actualChunk.observableAxis);
            Assert.assertEquals(expectedChunk.positionAxis1, actualChunk.positionAxis1);
            Assert.assertEquals(expectedChunk.positionAxis2, actualChunk.positionAxis2);
            Assert.assertEquals(expectedChunk.energyAxis, actualChunk.energyAxis);
            Assert.assertEquals(expectedChunk.timeAxis, actualChunk.timeAxis);
            Assert.assertEquals(expectedChunk.polarizationAxis, actualChunk.polarizationAxis);
            Assert.assertEquals(expectedChunk.customAxis, actualChunk.customAxis);

            compareObservableAxis(expectedChunk.observable, actualChunk.observable);
            compareSpatialWCS(expectedChunk.position, actualChunk.position);
            compareSpectralWCS(expectedChunk.energy, actualChunk.energy);
            compareTemporalWCS(expectedChunk.time, actualChunk.time);
            comparePolarizationWCS(expectedChunk.polarization, actualChunk.polarization);
            compare(expectedChunk.custom, actualChunk.custom);

            compareEntity(expectedChunk, actualChunk);
        }
    }

    protected void compareObservableAxis(ObservableAxis expected, ObservableAxis actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareSlice(expected.getDependent(), actual.getDependent());
        compareSlice(expected.independent, actual.independent);
    }

    protected void compareSpatialWCS(SpatialWCS expected, SpatialWCS actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordAxis2D(expected.getAxis(), actual.getAxis());
        Assert.assertEquals(expected.coordsys, actual.coordsys);
        Assert.assertEquals(expected.equinox, actual.equinox);
        Assert.assertEquals(expected.resolution, actual.resolution);
    }

    protected void compareSpectralWCS(SpectralWCS expected, SpectralWCS actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
        Assert.assertEquals(expected.bandpassName, actual.bandpassName);
        Assert.assertEquals(expected.resolvingPower, actual.resolvingPower);
        Assert.assertEquals(expected.restfrq, actual.restfrq);
        Assert.assertEquals(expected.restwav, actual.restwav);
        Assert.assertEquals(expected.getSpecsys(), actual.getSpecsys());
        Assert.assertEquals(expected.ssysobs, actual.ssysobs);
        Assert.assertEquals(expected.ssyssrc, actual.ssyssrc);
        Assert.assertEquals(expected.velang, actual.velang);
        Assert.assertEquals(expected.velosys, actual.velosys);
        Assert.assertEquals(expected.zsource, actual.zsource);
    }

    protected void compareTemporalWCS(TemporalWCS expected, TemporalWCS actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
        Assert.assertEquals(expected.exposure, actual.exposure);
        Assert.assertEquals(expected.resolution, actual.resolution);
    }

    protected void comparePolarizationWCS(PolarizationWCS expected, PolarizationWCS actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
    }

    protected void compare(CustomWCS expected, CustomWCS actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordAxis1D(expected.getAxis(), actual.getAxis());
    }

    protected void compareAxis(Axis expected, Axis actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getCtype());
        Assert.assertNotNull(actual.getCunit());

        Assert.assertEquals(expected.getCtype(), actual.getCtype());
        Assert.assertEquals(expected.getCunit(), actual.getCunit());
    }

    protected void compareValueCoord2(ValueCoord2D expected, ValueCoord2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.coord1, actual.coord1, 0.0);
        Assert.assertEquals(expected.coord2, actual.coord2, 0.0);
    }

    protected void compareCoord2D(Coord2D expected, Coord2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareRefCoord(expected.getCoord1(), actual.getCoord1());
        compareRefCoord(expected.getCoord2(), actual.getCoord2());
    }

    protected void compareCoordAxis1D(CoordAxis1D expected, CoordAxis1D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareCoordError(expected.error, actual.error);
        compareCoordRange1D(expected.range, actual.range);
        compareCoordBounds1D(expected.bounds, actual.bounds);
        compareCoordFunction1D(expected.function, actual.function);
    }

    protected void compareCoordAxis2D(CoordAxis2D expected, CoordAxis2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getAxis1());
        Assert.assertNotNull(actual.getAxis2());

        compareAxis(expected.getAxis1(), actual.getAxis1());
        compareAxis(expected.getAxis2(), actual.getAxis2());
        compareCoordError(expected.error1, actual.error1);
        compareCoordError(expected.error2, actual.error2);
        compareCoordRange2D(expected.range, actual.range);
        compareCoordBounds2D(expected.bounds, actual.bounds);
        compareCoordFunction2D(expected.function, actual.function);
    }

    protected void compareCoordBounds1D(CoordBounds1D expected, CoordBounds1D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(expected.getSamples());
        Assert.assertNotNull(actual.getSamples());
        Assert.assertEquals(expected.getSamples().size(), actual.getSamples().size());

        Iterator actualIter = expected.getSamples().iterator();
        Iterator expectedIter = actual.getSamples().iterator();
        while (expectedIter.hasNext()) {
            CoordRange1D expectedRange = (CoordRange1D) expectedIter.next();
            CoordRange1D actualRange = (CoordRange1D) actualIter.next();
            compareCoordRange1D(expectedRange, actualRange);
        }
    }

    protected void compareCoordBounds2D(CoordBounds2D expected, CoordBounds2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        if (expected instanceof CoordCircle2D && actual instanceof CoordCircle2D) {
            compareCoordCircle2D((CoordCircle2D) expected, (CoordCircle2D) actual);
        } else if (expected instanceof CoordPolygon2D && actual instanceof CoordPolygon2D) {
            compareCoordPolygon2D((CoordPolygon2D) expected, (CoordPolygon2D) actual);
        } else {
            Assert.fail("CoordBounds2D expected and actual are different types.");
        }

    }

    protected void compareCoordCircle2D(CoordCircle2D expected, CoordCircle2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getCenter());
        Assert.assertNotNull(actual.getRadius());
        compareValueCoord2(expected.getCenter(), actual.getCenter());
        Assert.assertEquals(expected.getRadius(), actual.getRadius());
    }

    protected void compareCoordError(CoordError expected, CoordError actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.syser);
        Assert.assertNotNull(actual.rnder);

        Assert.assertEquals(expected.syser, actual.syser);
        Assert.assertEquals(expected.rnder, actual.rnder);
    }

    protected void compareCoordFunction1D(CoordFunction1D expected, CoordFunction1D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.getNaxis(), actual.getNaxis());
        Assert.assertEquals(expected.getDelta(), actual.getDelta());
        compareRefCoord(expected.getRefCoord(), actual.getRefCoord());
    }

    protected void compareCoordFunction2D(CoordFunction2D expected, CoordFunction2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getDimension());
        Assert.assertNotNull(actual.getRefCoord());
        Assert.assertNotNull(actual.getCd11());
        Assert.assertNotNull(actual.getCd12());
        Assert.assertNotNull(actual.getCd21());
        Assert.assertNotNull(actual.getCd22());

        compareDimension2D(expected.getDimension(), actual.getDimension());
        compareCoord2D(expected.getRefCoord(), actual.getRefCoord());
        Assert.assertEquals(expected.getCd11(), actual.getCd11(), 0.0);
        Assert.assertEquals(expected.getCd12(), actual.getCd12(), 0.0);
        Assert.assertEquals(expected.getCd21(), actual.getCd21(), 0.0);
        Assert.assertEquals(expected.getCd22(), actual.getCd22(), 0.0);
    }

    protected void compareCoordPolygon2D(CoordPolygon2D expected, CoordPolygon2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(expected.getVertices());
        Assert.assertNotNull(actual.getVertices());
        Assert.assertEquals(expected.getVertices().size(), actual.getVertices().size());

        Iterator<ValueCoord2D> actualIter = expected.getVertices().iterator();
        Iterator<ValueCoord2D> expectedIter = actual.getVertices().iterator();
        while (expectedIter.hasNext()) {
            ValueCoord2D expectedCoord2D = expectedIter.next();
            ValueCoord2D actualCoord2D = actualIter.next();
            compareValueCoord2(expectedCoord2D, actualCoord2D);
        }
    }

    protected void compareCoordRange1D(CoordRange1D expected, CoordRange1D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        compareRefCoord(expected.getStart(), actual.getStart());
        compareRefCoord(expected.getEnd(), actual.getEnd());
    }

    protected void compareCoordRange2D(CoordRange2D expected, CoordRange2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getStart());
        Assert.assertNotNull(actual.getEnd());

        compareCoord2D(expected.getStart(), actual.getStart());
        compareCoord2D(expected.getEnd(), actual.getEnd());
    }

    protected void compareDimension2D(Dimension2D expected, Dimension2D actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.naxis1, actual.naxis1);
        Assert.assertEquals(expected.naxis2, actual.naxis2);
    }

    protected void compareRefCoord(RefCoord expected, RefCoord actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected.pix, actual.pix, 0.0);
        Assert.assertEquals(expected.val, actual.val, 0.0);
    }

    protected void compareSlice(Slice expected, Slice actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);

        Assert.assertNotNull(actual.getBin());
        Assert.assertNotNull(actual.getAxis());

        Assert.assertEquals(expected.getBin(), actual.getBin());
        compareAxis(expected.getAxis(), actual.getAxis());
    }

}
