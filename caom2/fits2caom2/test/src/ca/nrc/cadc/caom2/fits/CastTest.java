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

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.fits.exceptions.CastException;
import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jburke
 */
public class CastTest
{

    private boolean defaultBoolean;
    private int defaultInt;
    private long defaultLong;
    private float defaultFloat;
    private double defaultDouble;

    public CastTest() { }
    
    @Test
    public void testCastNullValue() throws Exception
    {
        try
        {
            Object actual = Cast.cast(null, String.class, null);
            Assert.assertNull(actual);

            actual = Cast.cast(null, boolean.class, null);
            Assert.assertEquals(defaultBoolean, actual);

            actual = Cast.cast(null, int.class, null);
            Assert.assertEquals(defaultInt, actual);

            actual = Cast.cast(null, long.class, null);
            Assert.assertEquals(defaultLong, actual);

            actual = Cast.cast(null, float.class, null);
            Assert.assertEquals(defaultFloat, actual);

            actual = Cast.cast(null, double.class, null);
            Assert.assertEquals(defaultDouble, actual);
        }
        catch (CastException ignore) { }
    }
    
    @Test
    public void testCastNullClass() throws Exception
    {
        try
        {
            Cast.cast("value", null, null);
            Assert.fail("CastException should be thrown for null Class");
        }
        catch (CastException ignore) { }
    }
    
    @Test
    public void testCastNullUtype() throws Exception
    {
        try
        {
            Cast.cast("value", TreeSet.class, null);
            Assert.fail("CastException should be thrown for null utype for a Set Class");
        }
        catch (CastException ignore) { }
        
        try
        {
            Cast.cast("value", String.class, null);
        }
        catch (CastException e)
        {
            Assert.fail("CastException should not be thrown for null utype for a String Class");
        }
    }
    
    @Test
    public void testCastException() throws Exception
    {
        try
        {
            Cast.cast("1.5", Integer.class, null);
            Assert.fail("Cast of 1.5 to an Integer should throw a CastException");
        }
        catch (CastException e) { }
    }
    
    @Test
    public void testCastBoolean() throws Exception
    {
        String value = "true";
        Boolean expected = true;
        Object actual = Cast.cast(value, Boolean.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
        
        actual = Cast.cast(value, boolean.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.booleanValue(), actual);
    }
    
    @Test
    public void testCastDouble() throws Exception
    {
        String value = "1.5";
        Double expected = 1.5;
        Object actual = Cast.cast(value, Double.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
        
        actual = Cast.cast(value, double.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.doubleValue(), actual);
    }
    
    @Test
    public void testCastFloat() throws Exception
    {
        String value = "1.5";
        Float expected = 1.5F;
        Object actual = Cast.cast(value, Float.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
        
        actual = Cast.cast(value, float.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.floatValue(), actual);
    }
    
    @Test
    public void testCastInteger() throws Exception
    {
        String value = "9";
        Integer expected = 9;
        Object actual = Cast.cast(value, Integer.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
        
        actual = Cast.cast(value, int.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.intValue(), actual);
    }
    
    @Test
    public void testCastLong() throws Exception
    {
        String value = "7";
        Long expected = 7L;
        Object actual = Cast.cast(value, Long.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
        
        actual = Cast.cast(value, long.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.longValue(), actual);
    }
    
    @Test
    public void testCastDate() throws Exception
    {
        String value = "2012-3-15 11:25:17.000";
        Calendar cal = Calendar.getInstance(DateUtil.UTC);
        cal.set(2012, 2, 15, 11, 25, 17);
        Date expected = cal.getTime();
        
        Object actual = Cast.cast(value, Date.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testCastIntArray() throws Exception
    {
        String value = "1,2, 3";
        int[] expected = new int[] {1,2,3};
        
        Object actual = Cast.cast(value, int[].class, null);
        Assert.assertNotNull(actual);
        
        int[] ia = (int[]) expected;
        Assert.assertEquals(1, ia[0]);
        Assert.assertEquals(2, ia[1]);
        Assert.assertEquals(3, ia[2]);
    }
    
    @Test
    public void testCastList() throws Exception
    {
        // comma delimited list.
        String value = "a,b,c";
        List<String> expected = new ArrayList<String>();
        expected.add("a");
        expected.add("b");
        expected.add("c");
        
        Object actual = Cast.cast(value, List.class, null);
        Assert.assertNotNull(actual);
        
        List list = (List) actual;
        Assert.assertEquals("a", list.get(0));
        Assert.assertEquals("b", list.get(1));
        Assert.assertEquals("c", list.get(2));
        
        // space delimited list.
        // a utype ending with .keywords results in a space delimited list.
        value = "a b c";
        actual = Cast.cast(value, List.class, "utype.keywords");
        Assert.assertNotNull(actual);
        
        list = (List) actual;
        Assert.assertEquals("a", list.get(0));
        Assert.assertEquals("b", list.get(1));
        Assert.assertEquals("c", list.get(2));
    }
    
    @Test
    public void testCastSet() throws Exception
    {
        ObservationURI a = new ObservationURI("foo", "bar");
        ObservationURI b = new ObservationURI("oof", "rab");
        
        String value = a.getURI().toString() + "," + b.getURI().toString();
        Set<ObservationURI> expected = new TreeSet<ObservationURI>();
        expected.add(a);
        expected.add(b);
        
        Object actual = Cast.cast(value, Set.class, "utype.members");
        Assert.assertNotNull(actual);
        
        Set set = (Set) actual;
        Iterator it = set.iterator();
        Assert.assertEquals(a, it.next());
        Assert.assertEquals(b, it.next());
        
        PlaneURI c = new PlaneURI(a, "bar");
        PlaneURI d = new PlaneURI(b, "rab");
        
        value = c.getURI().toString() + "," + d.getURI().toString();
        Set<PlaneURI> expected2 = new TreeSet<PlaneURI>();
        expected2.add(c);
        expected2.add(d);
        
        actual = Cast.cast(value, Set.class, "utype.inputs");
        Assert.assertNotNull(actual);
        
        set = (Set) actual;
        Iterator it2 = set.iterator();
        Assert.assertEquals(c, it2.next());
        Assert.assertEquals(d, it2.next());
    }
    
    @Test
    public void testCastURI() throws Exception
    {
        String value = "ad:archive/fileID";
        URI expected = new URI(value);
        
        Object actual = Cast.cast(value, URI.class, null);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
    }
    
}
