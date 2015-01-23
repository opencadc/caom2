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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class SerializableTest 
{
    private static final Logger log = Logger.getLogger(SerializableTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
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
    public void testCaomPackage()
    {
        try
        {
            ClassLoader loader = Observation.class.getClassLoader();
            String cname = Observation.class.getName();
            String pname = Observation.class.getPackage().getName();
            String path = cname.replace('.', '/') + ".class";
            log.debug("pkg: " + pname + " class: " + cname + " -> path: " + path);
            URL url = loader.getResource(path);
            File dir = new File(url.getPath()).getParentFile();
            doPackage(pname, dir);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void doPackage(String pname, File dir)
        throws Exception
    {
        log.debug("location: " + dir);
        int num = 0;
        int skip = 0;
        for (File f : dir.listFiles())
        {
            if (f.isFile() && f.getName().endsWith(".class"))
            {
                String cn = pname + "." + f.getName().substring(0,f.getName().length()-6);
                log.debug("loading: " + cn);
                Class c = Class.forName(cn);
                if ( shouldBeSerializable(c) )
                {
                    assertSerializable(c);
                    log.debug("serializable: " + c.getName());
                    num++;
                }
                else
                {
                    log.debug("skip: " + cn);
                    skip++;
                }
            }
            else if ( f.isDirectory() )
            {
                String spname = pname + "." + f.getName();
                doPackage(spname, f);
            }
        }
        log.info("package: " + pname + " serializable: " + num + " skip: " + skip);
    }

    private boolean shouldBeSerializable(Class c)
    {
        if ( c.isInterface() )
            return false;
        
        if ( c.isEnum() )
            return false;

        // is final, has only static methods and static fields
        if ( Modifier.isFinal(c.getModifiers()) && c.getSuperclass().equals(Object.class) )
        {
            boolean as = true;
            Method[] methods = c.getDeclaredMethods();
            for (Method m : methods)
            {
                boolean sm = Modifier.isStatic(m.getModifiers());
                log.debug("static method: " + m.getName() + " " + as);
                as = as && sm;
            }
            log.debug("static methods only: " + as);
            Field[] fields = c.getDeclaredFields();
            for (Field f : fields)
            {
                boolean sf = Modifier.isStatic(f.getModifiers());
                log.debug("static field: " + f.getName() + " " + as);
                as = as && sf;
            }
            log.debug("and static fields only: " + as);
            if (as)
                return false;
        }
        return true;
    }

    private void assertSerializable(Class c)
        throws NoSuchFieldException
    {
        String cn = c.getName();
        Assert.assertTrue(cn + " is Serializable", Serializable.class.isAssignableFrom(c));
        try
        {
            Field sv = c.getDeclaredField("serialVersionUID");
            int m = sv.getModifiers();
            Class fc = sv.getType();
            Assert.assertTrue(cn + ".serialVersionUID is private", Modifier.isPrivate(m));
            Assert.assertTrue(cn + ".serialVersionUID is static", Modifier.isStatic(m));
            Assert.assertTrue(cn + ".serialVersionUID is final", Modifier.isFinal(m));
            Assert.assertEquals(cn + ".serialVersionUID is long", "long", fc.getName());
        }
        catch(NoSuchFieldException ex)
        {
            log.error("Serializable class missing serialVersionUID: " + cn, ex);
            Assert.fail("Serializable class " + cn + " without serialVersionUID: " + ex);
        }
    }
}
