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

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.dali.tables.ListTableData;
import ca.nrc.cadc.dali.tables.TableData;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class CaomTapQueryTest 
{
    private static final Logger log = Logger.getLogger(CaomTapQueryTest.class);
    private static final int ARTIFACT_SIZE = 2;
    private static final int PART_SIZE = 2;
    private static final String ARTIFACT_ID = "-635063708241084595";
    private static final String ARTIFACT_URI = "ad:IRIS/I212B2H0";
    private static final String ARTIFACT_PRODUCT_TYPE = "science";
    private static final String ARTIFACT_CONTENT_TYPE = "application/fits";
    private static final Long ARTIFACT_CONTENT_LENGTH = new Long("1008000");
    private static final Integer ARTIFACT_ALTERNATIVE = new Integer(0);
    private static final String ARTIFACT_LASTMODIFIED = "2012-07-12T23:16:14.750";
    private static final String ARTIFACT_MAXLASTMODIFIED = "2012-07-12T23:16:14.750";
    private static final String PLANE_ID = "-6350637082410845957";
    private static final String PART_ID = "-5";
    private static final String PART_NAME = "partName";
    private static final String PART_PRODUCT_TYPE = null;
    private static final String PART_LASTMODIFIED = "2012-07-12T23:16:14.750";
    private static final String PART_MAXLASTMODIFIED = "2012-07-12T23:16:14.750";
    private static final String TAP_URI = "ivo://cadc.nrc.ca/tap";
    private DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2ops", Level.INFO);
    }

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
    public void testConstructor()
    {
        try
        {
            CaomTapQuery query = getLinkQuery();
            String jobID = query.getRunID();

            try
            {
                query = new CaomTapQuery(null, "foo");
                Assert.fail("expected IllegalArgumentException for tapURL=null");
            }
            catch(IllegalArgumentException expected) { log.debug("expected: " + expected); }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testBuildArtifacts()
    {
        doBuildTest(false);
        
        doBuildTest(true);
    }
    
    private void doBuildTest(boolean artifactsOnly)
    {
    	try
    	{
            boolean alternative = false;
            CaomTapQuery query = getLinkQuery();
            VOTableDocument votable = buildVOTable(artifactsOnly);

            Method buildArtifacts = query.getClass().getDeclaredMethod("buildArtifacts", VOTableDocument.class);
            buildArtifacts.setAccessible(true);
            
            int i = 1;
            List<Artifact> artifacts = (List<Artifact>) buildArtifacts.invoke(query, votable);
            Assert.assertEquals(ARTIFACT_SIZE, artifacts.size() );
            for (Artifact artifact : artifacts)
            {
            	Assert.assertEquals(Long.parseLong(ARTIFACT_ID) + i, artifact.getID().getLeastSignificantBits());
            	Assert.assertEquals(ARTIFACT_URI, artifact.getURI().toString());
            	Assert.assertEquals(ARTIFACT_CONTENT_TYPE, artifact.contentType);
            	Assert.assertEquals(ARTIFACT_CONTENT_LENGTH.longValue(), artifact.contentLength.longValue());
            	Assert.assertEquals(ARTIFACT_PRODUCT_TYPE, artifact.productType.getValue());
            	Assert.assertTrue(alternative == artifact.alternative);
            	String expectedLM = dateFormat.parse(ARTIFACT_LASTMODIFIED).toString();
            	String actualLM = artifact.getLastModified().toString();
            	Assert.assertEquals(expectedLM, actualLM);
            	String expectedMLM = dateFormat.parse(ARTIFACT_MAXLASTMODIFIED).toString();
            	String actualMLM = artifact.getMaxLastModified().toString();
            	Assert.assertEquals(expectedMLM, actualMLM);
            	
            	int j = 1;
            	Set<Part> parts = artifact.getParts();
                if (artifactsOnly)
                    Assert.assertTrue( parts.isEmpty());
                else
                {
                    Assert.assertEquals(PART_SIZE, parts.size());
                    for (Part part : parts)
                    {
                        Assert.assertEquals(Long.parseLong(PART_ID) + j, part.getID().getLeastSignificantBits());
                        Assert.assertEquals(PART_NAME + j, part.getName());
                        Assert.assertEquals(null, part.productType);
                        expectedLM = dateFormat.parse(PART_LASTMODIFIED).toString();
                        actualLM = part.getLastModified().toString();
                        Assert.assertEquals(expectedLM, actualLM);
                        expectedMLM = dateFormat.parse(PART_MAXLASTMODIFIED).toString();
                        actualMLM = part.getMaxLastModified().toString();
                        Assert.assertEquals(expectedMLM, actualMLM);
                        j++;
                    }
                }
            	i++;
            }
    	}
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private VOTableDocument buildVOTable(boolean artifactsOnly)
    {
    	VOTableDocument votable = new VOTableDocument();
        VOTableResource vr = new VOTableResource("results");
        votable.getResources().add(vr);
    	VOTableTable vtab = new VOTableTable();
        vr.setTable(vtab);
    	vtab.getFields().addAll(buildVOTableFields(artifactsOnly));
    	vtab.setTableData(buildTableData(artifactsOnly));    		
    	
    	return votable;
    }
    
    private List<VOTableField> buildArtifactFields()
    {
    	List<VOTableField> fields = new ArrayList<VOTableField>();

    	try
        {
            VOTableField planeIdField = new VOTableField("planeID", "long");
            //planeIdField.utype = "caom2:Plane.id";  // FK has no utype
            planeIdField.xtype = "adql:BIGINT";
            fields.add(planeIdField);

            VOTableField idField = new VOTableField("artifactID", "long");
            idField.utype = "caom2:Artifact.id";
            idField.xtype = "adql:BIGINT";
            fields.add(idField);
            
            VOTableField  uriField = new VOTableField("uri", "char");
            uriField.setArraysize(128);
            uriField.setVariableSize(true);
            uriField.utype = "caom2:Artifact.uri";
            uriField.xtype = "adql:VARCHAR";
            fields.add(uriField);
            
            VOTableField ptField = new VOTableField("productType", "char");
            ptField.setArraysize(32);
            ptField.setVariableSize(true);
            ptField.utype = "caom2:Artifact.productType";
            ptField.xtype = "adql:VARCHAR";
            fields.add(ptField);
            
            VOTableField ctField = new VOTableField("contentType", "char");
            ctField.setArraysize(128);
            ctField.setVariableSize(true);
            ctField.utype = "caom2:Artifact.contentType";
            ctField.xtype = "adql:VARCHAR";
            fields.add(ctField);
            
            VOTableField clField = new VOTableField("contentLength", "long");
            clField.utype = "caom2:Artifact.contentLength";
            clField.xtype = "adql:BIGINT";
            fields.add(clField);
            
            VOTableField aField = new VOTableField("alternative", "int");
            aField.utype = "caom2:Artifact.alternative";
            aField.xtype = "adql:INTEGER";
            fields.add(aField);
            
            VOTableField lmField = new VOTableField("lastModified", "char");
            lmField.setArraysize(64);
            lmField.setVariableSize(true);
            lmField.utype = "caom2:Artifact.lastModified";
            lmField.xtype = "adql:TIMESTAMP";
            fields.add(lmField);
            
            VOTableField mlmField = new VOTableField("maxLastModified", "char");
            mlmField.setArraysize(64);
            mlmField.setVariableSize(true);
            mlmField.utype = "caom2:Artifact.maxLastModified";
            mlmField.xtype = "adql:TIMESTAMP";
            fields.add(mlmField);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    	
    	return fields;
    }
    
    private List<VOTableField> buildPartFields()
    {
    	List<VOTableField> fields = new ArrayList<VOTableField>();

    	try
        {
            VOTableField artifactIdField = new VOTableField("artifactID", "long");
            //artifactIdField.utype = "caom2:Artifact.id"; // FK has no utype
            artifactIdField.xtype = "adql:BIGINT";
            fields.add(artifactIdField);
            
            VOTableField idField = new VOTableField("partID", "long");
            idField.utype = "caom2:Part.id";
            idField.xtype = "adql:BIGINT";
            fields.add(idField);
            
            VOTableField nameField = new VOTableField("name", "char");
            nameField.setArraysize(128);
            nameField.setVariableSize(true);
            nameField.utype = "caom2:Part.name";
            nameField.xtype = "adql:VARCHAR";
            fields.add(nameField);
            
            VOTableField ptField = new VOTableField("pProductType", "char");
            ptField.setArraysize(32);
            ptField.setVariableSize(true);
            ptField.utype = "caom2:Part.productType";
            ptField.xtype = "adql:VARCHAR";
            fields.add(ptField);
            
            VOTableField lmField = new VOTableField("lastModified", "char");
            lmField.setArraysize(64);
            lmField.setVariableSize(true);
            lmField.utype = "caom2:Part.lastModified";
            lmField.xtype = "adql:TIMESTAMP";
            fields.add(lmField);
            
            VOTableField mlmField = new VOTableField("maxLastModified", "char");
            mlmField.setArraysize(64);
            mlmField.setVariableSize(true);
            mlmField.utype = "caom2:Part.maxLastModified";
            mlmField.xtype = "adql:TIMESTAMP";
            fields.add(mlmField);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    	
    	return fields;
    }
    
    private List<VOTableField> buildVOTableFields(boolean artifactsOnly)
    {
    	List<VOTableField> fields = new ArrayList<VOTableField>();
    	
        try
        {
            fields.addAll(buildArtifactFields());
            if (!artifactsOnly)
            {
                fields.addAll(buildPartFields());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }    	
        
        return fields;
    }
    
    private void buildPartData(List<Object> row, final int i, final int j)
    {
        try
        {
            row.add(new Long(ARTIFACT_ID) + i);            
            row.add(new Long(PART_ID) + j); 
            row.add(PART_NAME + j);
            row.add(PART_PRODUCT_TYPE);
            row.add(dateFormat.parse(PART_LASTMODIFIED));
            row.add(dateFormat.parse(PART_MAXLASTMODIFIED));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private void buildArtifactData(List<Object> row, final int i)
    {
        try
        {
            row.add(new Long(PLANE_ID));            
            row.add(new Long(ARTIFACT_ID) + i);            
            row.add(ARTIFACT_URI);
            row.add(ARTIFACT_PRODUCT_TYPE);
            row.add(ARTIFACT_CONTENT_TYPE);
            row.add(ARTIFACT_CONTENT_LENGTH);
            row.add(ARTIFACT_ALTERNATIVE);
            row.add(dateFormat.parse(ARTIFACT_LASTMODIFIED));
            row.add(dateFormat.parse(ARTIFACT_MAXLASTMODIFIED));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    private TableData buildTableData(boolean artifactsOnly)
    {
    	ListTableData data = new ListTableData();

        try
        {
            for (int i=1; i < ARTIFACT_SIZE + 1; i++)
            {
                for (int j=1; j < PART_SIZE + 1; j++)
                {
                	List<Object> row = new ArrayList<Object>();  
                	buildArtifactData(row, i);
                    if (!artifactsOnly)
                    {
                        buildPartData(row, i, j);
                    }
                    data.getArrayList().add(row);
                }                
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }   
        
        return data;
    }
    
    private CaomTapQuery getLinkQuery()
    {
    	CaomTapQuery query = null;
    	
    	try
    	{
	    	String runID = "testJobID";
	    	String tapProto = "http";
	    	RegistryClient reg = new RegistryClient();
	    	URL tapURL = reg.getServiceURL(new URI(TAP_URI), Standards.TAP_SYNC_11_URI, AuthMethod.ANON);
	    	
	        query = new CaomTapQuery(tapURL, runID);    	
	        Assert.assertEquals(runID, query.getRunID());
	        Assert.assertEquals(tapURL, query.getTapURL());
    	}
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        return query;
    }
}
