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
package ca.nrc.cadc.caom2.fits;

import ca.nrc.cadc.fits2caom2.Util;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import nom.tam.fits.Header;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class FitsMappingTest
{
    private static Logger log = Logger.getLogger(FitsMappingTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc", Level.INFO);
    }
    
    public FitsMappingTest() { }

    /**
     * Test of getMapping method, of class FitsMapping.
     */
    @Test
    public void testGetMapping() throws Exception
    {        
        File df = new File("src/test/resources/FitsMappingTest.default");
        File of = new File("src/test/resources/FitsMappingTest.override");
        
        Map<String,String> config = Util.loadConfig("src/test/resources/FitsMappingTest.config");
        FitsValuesMap defaults = new FitsValuesMap(new FileReader(df), "default");
        FitsValuesMap override = new FitsValuesMap(new FileReader(of), "override");

        FitsMapping mapping = new FitsMapping(config, defaults, override);        
        
        Assert.assertEquals("true", mapping.getMapping("aBoolean"));
        Assert.assertEquals("1.0", mapping.getMapping("aDouble"));
        Assert.assertEquals("1.5", mapping.getMapping("aFloat"));
        Assert.assertEquals("2", mapping.getMapping("aLong"));
        Assert.assertEquals("2012-3-13 12:12:12.000GMT", mapping.getMapping("aDate"));
        Assert.assertEquals("a,list", mapping.getMapping("aList"));
        Assert.assertEquals("caom:collection/observationID", mapping.getMapping("aSet_members"));
        Assert.assertEquals("1,2", mapping.getMapping("aIntArray"));
    }
    
    @Test
    public void testGetKeyword() throws Exception
    {
        File cf = new File("config/fits2caom2.config");
        Map<String,String> config = Util.loadConfig("config/fits2caom2.config");
        
        FitsMapping mapping = new FitsMapping(config, null, null);
        mapping.positionAxis1 = 1;
        mapping.positionAxis2 = 2;
        mapping.energyAxis = 3;
        mapping.timeAxis = 4;
        mapping.polarizationAxis = 5;
        
        String keyword = mapping.getKeyword("CTYPE{positionAxis1}");
        Assert.assertNotNull(keyword);
        Assert.assertEquals("CTYPE1", keyword);
        
        keyword = mapping.getKeyword("CTYPE{positionAxis2}");
        Assert.assertNotNull(keyword);
        Assert.assertEquals("CTYPE2", keyword);
        
        keyword = mapping.getKeyword("CTYPE{energyAxis}");
        Assert.assertNotNull(keyword);
        Assert.assertEquals("CTYPE3", keyword);
        
        keyword = mapping.getKeyword("CTYPE{timeAxis}");
        Assert.assertNotNull(keyword);
        Assert.assertEquals("CTYPE4", keyword);
        
        keyword = mapping.getKeyword("CTYPE{polarizationAxis}");
        Assert.assertNotNull(keyword);
        Assert.assertEquals("CTYPE5", keyword);
        
        try
        {
            keyword = mapping.getKeyword("CTYPE{unknownAxis}");
            Assert.fail("Uknown axis should have thrown IllegalArgumentException");
        }
        catch (IllegalArgumentException ignore) { }
    }
    
    @Test
    public void testGetExtensionOnlyKeyword() throws Exception
    {
        Map<String,String> config = Util.loadConfig("config/fits2caom2.config");   
        FitsMapping mapping = new FitsMapping(config, null, null);
        
        mapping.extension = new Integer(0);

        mapping.primary = new Header();
        mapping.primary.addValue("NAXIS", "0", "");
        mapping.primary.addValue("NAXIS1", "3", "");
        mapping.primary.addValue("NAXIS2", "1", "");
        mapping.primary.addValue("BITPIX", "8", "");
        mapping.primary.addValue("CRPIX1", "1.0", "");
        
        mapping.header = new Header();
        mapping.header.addValue("NAXIS", "0", "");
        mapping.header.addValue("NAXIS1", "3", "");
        mapping.header.addValue("BITPIX", "8", "");
        mapping.header.addValue("CRPIX1", "1.0", "");
        
        String value = mapping.getKeywordValue("NAXIS");
        Assert.assertEquals("NAXIS", "0", value);
        
        value = mapping.getKeywordValue("NAXIS1");
        Assert.assertEquals("NAXIS1", "3", value);
        
        value = mapping.getKeywordValue("NAXIS2");
        Assert.assertEquals("NAXIS2", "1", value);
        
        value = mapping.getKeywordValue("BITPIX");
        Assert.assertEquals("BITPIX", "8", value);
        
        value = mapping.getKeywordValue("CRPIX1");
        Assert.assertEquals("CRPIX1", "1.0", value);
        
        mapping.extension = new Integer(1);
        
        mapping.header = new Header();
        mapping.header.addValue("NAXIS", "1", "");
        mapping.header.addValue("NAXIS1", "4", "");
        mapping.header.addValue("BITPIX", "2", "");
        mapping.header.addValue("CRPIX1", "2.0", "");
        
        value = mapping.getKeywordValue("NAXIS");
        Assert.assertEquals("NAXIS", "1", value);
        
        value = mapping.getKeywordValue("NAXIS1");
        Assert.assertEquals("NAXIS1", "4", value);
        
        value = mapping.getKeywordValue("NAXIS2");
        Assert.assertNull("NAXIS2", value);
        
        value = mapping.getKeywordValue("BITPIX");
        Assert.assertEquals("BITPIX", "2", value);
        
        value = mapping.getKeywordValue("CRPIX1");
        Assert.assertEquals("CRPIX1", "2.0", value);
        
        mapping.primary = new Header();
        mapping.primary.addValue("NAXIS", "0", "");
        mapping.primary.addValue("NAXIS1", "3", "");
        mapping.primary.addValue("BITPIX", "8", "");
        
        mapping.header = new Header();
        mapping.header.addValue("NAXIS", "2", "");
        mapping.header.addValue("NAXIS2", "4", "");
        mapping.header.addValue("CRPIX1", "1.0", "");
        
        value = mapping.getKeywordValue("NAXIS");
        Assert.assertEquals("NAXIS", "2", value);
        
        value = mapping.getKeywordValue("NAXIS1");
        Assert.assertNull("NAXIS1" + value, value);
        
        value = mapping.getKeywordValue("NAXIS2");
        Assert.assertEquals("NAXIS2", "4", value);
        
        value = mapping.getKeywordValue("BITPIX");
        Assert.assertNull("BITPIX", value);
        
        value = mapping.getKeywordValue("CRPIX1");
        Assert.assertEquals("CRPIX1", "1.0", value);
        
    }
    
}
