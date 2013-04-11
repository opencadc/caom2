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

import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileReader;
import org.apache.log4j.Level;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

public class ConfigMapTest
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.fits", Level.INFO);
    }
    
    @Test
    public void testConfigFileKeysValues() throws Exception
    {
        
        File configFile = new File("test/config/fits2caom2/configMap.config");
        FitsValuesMap defaults = new FitsValuesMap(new FileReader(configFile), "defaults");
        
        // GLOBAL Scope elements
        assertEquals("valueA, valueB", defaults.getValue("key0"));

        assertEquals("value1", defaults.getValue("key1"));
        
        assertEquals("", defaults.getValue("key2"));
        
        assertEquals("'", defaults.getValue("key3"));
        
        assertEquals("", defaults.getValue("key4"));
        
        assertEquals("'value5", defaults.getValue("key5"));
        
        assertEquals("value6'", defaults.getValue("key6"));
        
        assertEquals("value7", defaults.getValue("key7"));
        
        assertEquals(null, defaults.getValue("key8"));
        
        assertNull(defaults.getValue("key9"));
        
        // FILE Scope Elements
        String uri = "http://host/path";
        String extension = "0";
        
        assertEquals("value10", defaults.getValue("key10", uri));
        
        assertEquals("value11", defaults.getValue("key11", uri));
        
        assertEquals("", defaults.getValue("key12", uri));
        
        assertEquals(null, defaults.getValue("key13", uri));
        
        // EXTENSION Scope Elements
        assertEquals("value20", defaults.getValue("key20", uri, extension));
        
        assertEquals("", defaults.getValue("key21", uri, extension));
        
        assertEquals("value22", defaults.getValue("key22", uri, extension));
        
        assertEquals(null, defaults.getValue("key23", uri, extension));
        
        // Comments after a key value
        assertEquals("value24", defaults.getValue("key24"));
        
        // Comments after a key
        assertNull(defaults.getValue("key25"));
        
        // Toggle to return the value and the comment.
        defaults.setReturnComments(true);
        assertEquals("value1", defaults.getValue("key1"));
        assertEquals("value10", defaults.getValue("key10", uri));
        assertEquals("value24 #comment", defaults.getValue("key24"));
    }

}
