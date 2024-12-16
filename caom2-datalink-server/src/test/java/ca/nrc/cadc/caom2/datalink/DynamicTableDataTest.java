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

package ca.nrc.cadc.caom2.datalink;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CaomTapQuery;
import ca.nrc.cadc.caom2ops.ServiceConfig;
import ca.nrc.cadc.caom2ops.UsageFault;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.net.URI;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.datalink.DataLink;

/**
 * @author pdowler
 */
public class DynamicTableDataTest 
{
    private static final Logger log = Logger.getLogger(DynamicTableDataTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.datalink", Level.INFO);
    }
    
    static String RUNID = "abc123";

    static URI SODA_ID = URI.create("ivo://cadc.nrc.ca/caom2ops");
    
    ServiceConfig conf = new ServiceConfig();
    
    public DynamicTableDataTest() { }

    //@Test
    public void testTemplate()
    {
        try
        {

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNoInputURI()
    {
        log.debug("testNoInputURI - START");
        try
        {
            Job job = new Job();
            
            ArtifactProcessor ap = new ArtifactProcessor();
            CaomTapQuery query = new TestCaomTapQuery("123456", new URI("ivo://cadc.nrc.ca/unused"), 0);
            DynamicTableData dtd = new DynamicTableData(job, query, ap);
            dtd.setDownloadOnly(false);
            dtd.setMaxrec(10);
            Iterator<DataLink> iter = dtd.links();
            
            Assert.assertFalse( iter.hasNext() );
        }
        catch(UsageFault expected)
        {
            log.debug("caught expected exception: " + expected);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testNoResults()
    {
        log.debug("testNoResults - START");
        try
        {
            Job job = new Job();
            job.getParameterList().add(new Parameter("id", "caom:FOO/bar/baz1"));
            job.getParameterList().add(new Parameter("id", "caom:FOO/bar/baz2"));
            ArtifactProcessor ap = new ArtifactProcessor();
            CaomTapQuery query = new TestCaomTapQuery("123456", new URI("ivo://cadc.nrc.ca/unused"), 0);
            DynamicTableData dtd = new DynamicTableData(job, query, ap);
            dtd.setDownloadOnly(false);
            dtd.setMaxrec(10);
            Iterator<DataLink> iter = dtd.links();

            Assert.assertTrue(iter.hasNext());
            DataLink row1 = iter.next();
            Assert.assertNotNull(row1.errorMessage); // see DataLinkerror message 
            Assert.assertTrue(iter.hasNext());
            DataLink row2 = iter.next();
            Assert.assertNotNull(row2.errorMessage); // see DataLinkerror message 
            
            Assert.assertFalse( iter.hasNext() );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSingleInputURI()
    {
        log.debug("testSingleInputURI - START");
        try
        {
            Job job = new Job();
            job.getParameterList().add(new Parameter("id", "ivo://cadc.nrc.ca/IRIS?bar/baz1"));
            ArtifactProcessor ap = new ArtifactProcessor();
            CaomTapQuery query = new TestCaomTapQuery("123456", new URI("ivo://cadc.nrc.ca/unused"), 1);
            DynamicTableData dtd = new DynamicTableData(job, query, ap);
            dtd.setDownloadOnly(false);
            dtd.setMaxrec(10);
            Iterator<DataLink> iter = dtd.links();

            // 1 results
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());

            Assert.assertFalse( iter.hasNext() );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMultipleInputURI()
    {
        log.debug("testMultipleInputURI - START");
        try
        {
            Job job = new Job();
            job.getParameterList().add(new Parameter("id", "ivo://cadc.nrc.ca/IRIS?bar/baz1"));
            job.getParameterList().add(new Parameter("id", "ivo://cadc.nrc.ca/IRIS?bar/baz2"));
            ArtifactProcessor ap = new ArtifactProcessor();
            CaomTapQuery query = new TestCaomTapQuery("123456", new URI("ivo://cadc.nrc.ca/unused"), 1);
            DynamicTableData dtd = new DynamicTableData(job, query, ap);
            dtd.setDownloadOnly(false);
            dtd.setMaxrec(10);
            Iterator<DataLink> iter = dtd.links();

            // 2x2 results
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());

            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());

            Assert.assertFalse( iter.hasNext() );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testNestedIteration()
    {
        log.debug("testNestedIteration - START");
        try
        {
            Job job = new Job();
            job.getParameterList().add(new Parameter("id", "ivo://cadc.nrc.ca/IRIS?bar/baz1"));
            job.getParameterList().add(new Parameter("id", "ivo://cadc.nrc.ca/IRIS?bar/baz2"));
            ArtifactProcessor ap = new ArtifactProcessor();
            CaomTapQuery query = new TestCaomTapQuery("123456", new URI("ivo://cadc.nrc.ca/unused"), 2);
            DynamicTableData dtd = new DynamicTableData(job, query, ap);
            dtd.setDownloadOnly(false);
            dtd.setMaxrec(10);
            Iterator<DataLink> iter = dtd.links();

            // 2 ID x2 artifacts + 1 pkg each = 6
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());

            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());

            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());
            
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());
            
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());
            
            Assert.assertTrue( iter.hasNext() );
            Assert.assertNotNull(iter.next());
            
            Assert.assertFalse( iter.hasNext() );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    class TestCaomTapQuery extends CaomTapQuery
    {
        int num;
        TestCaomTapQuery(String jobID, URI tapURI, int num)
        {
            super(tapURI, jobID);
            this.num = num;
        }

        @Override
        public ArtifactQueryResult performQuery(PublisherID id, boolean artifactOnly)
        {
            ArtifactQueryResult ret = new ArtifactQueryResult(id);
            try
            {
                for (int i=0; i<num; i++)
                {
                    Artifact a = new Artifact(
                        URI.create("ad:IRIS/bar_baz_" + i),
                        ProductType.SCIENCE, ReleaseType.DATA);
                    a.contentLength = 123L;
                    a.contentType = "text/plain";
                    ret.getArtifacts().add(a);
                }
            }
            catch(Exception ex)
            {
                throw new RuntimeException("test setup failed", ex);
            }
            log.debug("TestCaomTapQuery.getArtifacts: " + ret.getArtifacts().size());
            return  ret;
        }
    }
}
