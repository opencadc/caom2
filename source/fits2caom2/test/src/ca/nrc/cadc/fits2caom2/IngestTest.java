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
package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.FitsValuesMap;
import ca.nrc.cadc.caom2.xml.ObservationReader;
import ca.nrc.cadc.util.Log4jInit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class IngestTest
{
    private static Logger log = Logger.getLogger(IngestTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }

    
    String collection = "TEST";
    String observationID = "XMLIngestTest";
    String productID = "productIDFromFITS";
    
    public IngestTest() { }
    
    @Test
    public void testIngest() throws Exception
    {
        URI[] uris = new URI[] { new URI("ad", "BLAST/BLASTvulpecula2005-06-12_250_reduced_2006-10-03", null) };
        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);

        Ingest ingest = new Ingest(collection, observationID, productID,uris,config);
        ingest.setMapping(Util.getFitsMapping(config, null, null));
        ingest.setInFile(new File("test/files/simple.xml"));
        ingest.setOutFile(new File("build/test/xmltestout.xml"));
        ingest.run();
        
        File out = new File("build/test/xmltestout.xml");
        Assert.assertNotNull(out);
        
        ObservationReader reader = new ObservationReader();
        Observation observation = reader.read(new BufferedReader(new FileReader(out)));
        
        Assert.assertNotNull(observation);
        
        Set<Plane> planes = observation.getPlanes();
        Assert.assertEquals(2, planes.size());
        
        out.delete();
    }
    
    @Test
    public void testIngestArtifactTypes22() throws Exception
    {
        URI uri = URI.create("ad:BLAST/BLASTvulpecula2005-06-12_250_reduced_2006-10-03");
        URI[] uris = new URI[] { uri };
        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);

        Ingest ingest = new Ingest(collection, observationID, productID, uris, config);
        ingest.setMapping(Util.getFitsMapping(config, "test/config/fits2caom2/artifact-ingest.defaults", null));
        ingest.setInFile(new File("test/files/simple.xml"));
        ingest.setOutFile(new File("build/test/xmltestout22.xml"));
        ingest.run();
        
        File out = new File("build/test/xmltestout22.xml");
        Assert.assertNotNull(out);
        
        ObservationReader reader = new ObservationReader();
        Observation observation = reader.read(new BufferedReader(new FileReader(out)));
        
        Assert.assertNotNull(observation);
        
        Set<Plane> planes = observation.getPlanes();
        Assert.assertEquals(2, planes.size());
        
        boolean found = false;
        for (Plane p : planes)
        {
            if (p.getProductID().equals(productID))
            {
                for (Artifact a : p.getArtifacts())
                {
                    if (a.getURI().equals(uri))
                    {
                        found = true;
                        Assert.assertEquals("Artifact.productType", ProductType.THUMBNAIL, a.getProductType());
                        Assert.assertEquals("Artifact.productType", ReleaseType.META, a.getReleaseType());
                    }
                }
            }
        }
        Assert.assertTrue("found plane/artifact " + uri, found);
        
        out.delete();
    }
    
    //@Test
    public void testPopulateChunk() throws Exception
    {
        FitsValuesMap defaults = new FitsValuesMap(null, null);        
        defaults.setKeywordValue("NAXIS", "2");
        defaults.setKeywordValue("WCSAXES", "3");
        defaults.setKeywordValue("CUNIT1", "deg");
        defaults.setKeywordValue("CTYPE1", "RA");
        defaults.setKeywordValue("CUNIT2", "deg");
        defaults.setKeywordValue("CTYPE2", "DEC");
        defaults.setKeywordValue("position.range.start.coord1.pix", "1.0");
        defaults.setKeywordValue("position.range.start.coord1.val", "2.0");
        defaults.setKeywordValue("position.range.start.coord2.pix", "3.0");
        defaults.setKeywordValue("position.range.start.coord2.val", "4.0");
        defaults.setKeywordValue("position.range.end.coord1.pix", "5.0");
        defaults.setKeywordValue("position.range.end.coord1.val", "6.0");
        defaults.setKeywordValue("position.range.end.coord2.pix", "7.0");
        defaults.setKeywordValue("position.range.end.coord2.val", "8.0");
        defaults.setKeywordValue("RADECSYS", "coordsys");
        defaults.setKeywordValue("EQUINOX", "1.0");
        defaults.setKeywordValue("position.resolution", "2.0");
        defaults.setKeywordValue("CTYPE3", "ctype3");
        defaults.setKeywordValue("CUNIT3", "cunit3");
        defaults.setKeywordValue("CRPIX3", "3");
        
        String userConfig = null;
        Map<String,String> config = Util.loadConfig(userConfig);
        FitsMapping mapping = new FitsMapping(config, defaults, null);
        
        URI[] uris = new URI[] { new URI("ad", "CFHT/700000o", null) };
        mapping.uri = "ad:CFHT/700000o";
        mapping.extension = new Integer(1);
        
        Ingest ingest = new Ingest(collection, observationID, productID, uris, config);
        
        Chunk chunk = new Chunk();
        ingest.populateChunk(chunk, mapping);
        
        Assert.assertNotNull(chunk);
        Assert.assertNotNull(chunk.positionAxis1);
        Assert.assertNotNull(chunk.positionAxis2);
        Assert.assertNotNull(chunk.observableAxis);
        Assert.assertNull(chunk.energyAxis);
        Assert.assertNull(chunk.timeAxis);
        Assert.assertNull(chunk.polarizationAxis);
        
        Assert.assertNotNull(chunk.position);
        Assert.assertNotNull(chunk.observable);
        Assert.assertNull(chunk.energy);
        Assert.assertNull(chunk.time);
        Assert.assertNull(chunk.polarization);
        
    }
}
