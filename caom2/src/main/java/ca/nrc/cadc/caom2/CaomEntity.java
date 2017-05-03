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

import ca.nrc.cadc.caom2.util.FieldComparator;
import ca.nrc.cadc.util.HashUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import ca.nrc.cadc.util.HexUtil;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public abstract class CaomEntity implements Serializable
{
    private static final long serialVersionUID = 201704181300L;
    private static final Logger log = Logger.getLogger(CaomEntity.class);
    private static final String CAOM2 = CaomEntity.class.getPackage().getName();
    private static final boolean SC_DEBUG = false;
    
    // state
    private UUID id;
    private Date lastModified;
    private Date maxLastModified;
    private URI metaChecksum;
    private URI accumulatedMetaChecksum;
    
    protected CaomEntity()
    {
        this(false); // default: 64-bit consistent with CAOM-2.0 use of Long
    }
    protected CaomEntity(boolean fullUUID)
    {
        if (fullUUID)
            this.id = UUID.randomUUID();
        else
            this.id = new UUID(0L, CaomIDGenerator.getInstance().generateID());

        // metaChecksum and accMetaChecksum will be calculated as
        // part of this constructor.
    }

    /**
     * Get the unique persistent numeric identifier for this object.
     * @return
     */
    public final UUID getID()
    {
        return id;
    }

    /**
     * Get the timestamp of the last modification of the state of this object.
     * The last modified date includes all local state but not the state
     * of child objects contained in collections.
     * 
     * @return
     */
    public final Date getLastModified()
    {
        return lastModified;
    }

    /**
     * Get the maximum timestamp of the last modification of the state of this
     * object and any child entities..
     *
     * @return
     */
    public final Date getMaxLastModified()
    {
        return maxLastModified;
    }

    /**
     * Get the checksum for the state of this entity. This checksum does not 
     * include child entities.
     * 
     * @return checksum URI in the form algorithm:value
     */
    public URI getMetaChecksum()
    {
        return metaChecksum;
    }

    /**
     * Get the accumulated checksum for the state of this entity. The accumulated 
     * checksum includes the state of this entity and all child entities.
     * 
     * @return checksum URI in the form algorithm:value
     */
    public URI getAccumulatedMetaChecksum()
    {
        return accumulatedMetaChecksum;
    }
    
    /**
     * The base implementation compares numeric IDs.
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (o instanceof CaomEntity)
        {
            CaomEntity a = (CaomEntity) o;
            return this.id.equals(a.id);
        }
        return false;
    }

    /**
     * The base implementation uses the hash code of the numeric ID.
     * @return hashCode
     */
    @Override
    public int hashCode()
    {
        return this.id.hashCode();
    }

    /**
     * Compute and return a hash code of the non-transient state of the entity.
     *
     * @return 32-bit checksum of all non-null fields, 0 for an empty entity
     * @deprecated
     */
    public int getStateCode()
    {
        return getStateCode(false);
    }
    
    /**
     * Compute and return a hash code of the entire state of the entity.
     *
     * @param includeTransient fields in checksum calculation
     * @return 32-bit checksum of all non-null fields, 0 for an empty entity
     * @deprecated
     */
    public int getStateCode(boolean includeTransient)
    {
        return checksum(this.getClass(), this, includeTransient);
    }

    /**
     * Compute new metadata checksum for this entity. The computed value is based on
     * the current state and is returned without changing the stored value that is
     * accessed via the getMetaChecksum() method.
     * 
     * @param includeTransient
     * @param digest
     * @return 
     */
    public URI computeMetaChecksum(boolean includeTransient, MessageDigest digest) 
    {
        try 
        {
            byte[] metaChecksumBytes = calcMetaChecksum(this.getClass(), this, includeTransient, digest);
            String hexMetaChecksum = HexUtil.toHex(metaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, hexMetaChecksum, null);
        } 
        catch (URISyntaxException e) 
        {
            throw new RuntimeException("Unable to create metadata checksum URI for " + this.getClass().getName(), e);
        }
        finally { }
    }

    private byte[] calcMetaChecksum(Class c, Object o, boolean includeTransient, MessageDigest msgDigest)
    {
        try
        {
            SortedSet<Field> fields = getStateFields(c, includeTransient);
            for (Field f : fields) 
            {
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) 
                {
                    if (SC_DEBUG) log.debug("checksum: value type is " + fo.getClass().getName());
                    Class ac = fo.getClass(); // actual class
                    if (fo instanceof CaomEnum) 
                    {
                        // use ce.getValue
                        CaomEnum ce = (CaomEnum) fo;
                        return ce.getBytes();
                    } 
                    else if (isLocalClass(ac)) 
                    {
                        //only merge the checksum if there is state (cs != 0)
                        msgDigest.update(calcMetaChecksum(ac, fo, includeTransient, msgDigest));

                    } 
                    else if (fo instanceof Collection) 
                    {
                        Collection stuff = (Collection) fo;
                        Iterator i = stuff.iterator();
                        while (i.hasNext()) 
                        {
                            Object co = i.next();
                            Class cc = co.getClass();
                            if (isLocalClass(cc)) 
                            {
                                //only merge the checksum if there is state (cs != 0)
                                msgDigest.update(calcMetaChecksum(cc, co, includeTransient, msgDigest));
                            } 
                            else // non-caom2 class ~primtive value
                            {
                                msgDigest.update(primtiveValueToBytes(co));
                            }
                        }
                    } 
                    else if (fo instanceof Date) 
                    {
                        // only compare down to seconds
                        Date date = (Date) fo;
                        long sec = (date.getTime() / 1000L);
                        msgDigest.update(HexUtil.toBytes(sec));
                    } 
                    else // non-caom2 class ~primtive value
                    {
                        msgDigest.update(primtiveValueToBytes(fo));
                    }
                }
                else if (SC_DEBUG) log.debug("skip: " + c.getName() + "." + f.getName());
            }

        }
        catch(IllegalAccessException bug)
        {
            throw new RuntimeException("Unable to calculate metaChecksum for class " + c.getName(), bug);
        }

        return msgDigest.digest();

    }
    
    private byte[] primtiveValueToBytes(Object o)
    {
        if (o instanceof Byte)
            return HexUtil.toBytes((Byte) o); // auto-unbox
        if (o instanceof Short)
            return HexUtil.toBytes((Short) o); // auto-unbox
        if (o instanceof Integer)
            return HexUtil.toBytes((Integer) o); // auto-unbox
        if (o instanceof Long)
            return HexUtil.toBytes((Long) o); // auto-unbox
        
        if (o instanceof Float)
            return HexUtil.toBytes(Float.floatToIntBits((Float) o)); // auto-unbox, IEEE754 single
        if (o instanceof Double)
            return HexUtil.toBytes(Double.doubleToLongBits((Double) o)); // auto-unbox, IEEE754 double
        
        if (o instanceof String)
        {
            try { return ((String) o).getBytes("UTF-8"); }
            catch(UnsupportedEncodingException ex)
            {
                throw new RuntimeException("BUG: failed to encode String in UTF-8", ex);
            }
        }
        if (o instanceof URI)
        {
            try { return ((URI) o).toASCIIString().getBytes("UTF-8"); }
            catch(UnsupportedEncodingException ex)
            {
                throw new RuntimeException("BUG: failed to encode String in UTF-8", ex);
            }
        }
        
        throw new UnsupportedOperationException("unexpected primitive/value type: " + o.getClass().getName());
    }

    /**
     * Compute new accumulated metadata checksum for this entity. The computed value 
     * is based on the current state of this entity and all child entities. The value
     * is returned without changing the stored value that is accessed via the 
     * getAccumulatedMetaChecksum() method.
     * 
     * @param includeTransient
     * @param digest
     * @return 
     */
    public URI computeAccumulatedMetaChecksum(boolean includeTransient, MessageDigest digest) 
    {
        try 
        {
            byte[] accMetaChecksumBytes = calcAccMetaChecksum(this.getClass(), this, includeTransient, digest);
            String accHexMetaChecksum = HexUtil.toHex(accMetaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, accHexMetaChecksum, null);
        } 
        catch (URISyntaxException e) 
        {
            throw new RuntimeException("Unable to create metadata checksum URI for " + this.getClass().getName(), e);
        }
    }

    private byte[] calcAccMetaChecksum(Class c, Object o, boolean includeTransient, MessageDigest msgDigest) 
    {
        // add this object's metaChecksum value to the msgDigest
        CaomEntity ce = (CaomEntity) o;
        byte[] cs = calcMetaChecksum(c, o, includeTransient, msgDigest);
        msgDigest.update(cs);
            
        // add acccumulated checksum of child entities in id order
        try
        {
            SortedSet<Field> fields = getChildFields(c);
            if (SC_DEBUG) log.debug("calcAccMetaChecksum: " + c.getClass().getName() + " has " + fields.size() + " child fields");
            
            for (Field f : fields) 
            {
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) 
                {
                    if (SC_DEBUG) log.debug("calcAccMetaChecksum: value type is " + fo.getClass().getName());
                    if (fo instanceof Collection) 
                    {
                        Collection stuff = (Collection) fo;
                        Iterator i = stuff.iterator();
                        while (i.hasNext()) 
                        {
                            Object co = i.next();
                            Class cc = co.getClass();
                            msgDigest.update(calcAccMetaChecksum(cc, co, includeTransient, msgDigest));
                        }
                    }
                }
                // child sets are never null
                //else if (SC_DEBUG) log.debug("skip: " + c.getName() + "." + f.getName());
            }
        }
        catch(IllegalAccessException bug)
        {
            throw new RuntimeException("BUG: Unable to calculate metaChecksum for class " + c.getName(), bug);
        }

        return msgDigest.digest();
    }

    // recursive compute checksum
    int checksum(Class c, Object o, boolean includeTransient)
    {
        int ret = 0;
        try
        {
            SortedSet<Field> fields = getStateFields(c, includeTransient);
            for (Field f : fields)
            {
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null)
                {
                    if (SC_DEBUG) log.debug("checksum: value type is " + fo.getClass().getName());
                    Class ac = fo.getClass(); // actual class
                    if (fo instanceof CaomEnum)
                    {
                        CaomEnum ce = (CaomEnum) fo;
                        ret = HashUtil.hash(ret, ce.checksum());
                    }
                    else if(isLocalClass(ac))
                    {
                        //only merge the checksum if there is state (cs != 0)
                        int cs = checksum(ac, fo, includeTransient);
                        if (cs != 0)
                        {
                            ret = HashUtil.hash(ret, cs);
                            if (SC_DEBUG) log.debug("checksum: " + c.getName() + "." + f.getName() + " = " + cs + " -> " + ret);
                        }
                        else
                            if (SC_DEBUG) log.debug("skip: " + c.getName() + "." + f.getName());
                    }
                    else if (fo instanceof Collection)
                    {
                        Collection stuff = (Collection) fo;
                        Iterator i = stuff.iterator();
                        while ( i.hasNext() )
                        {
                            Object co = i.next();
                            Class cc = co.getClass();
                            if (isLocalClass(cc))
                            {
                                //only merge the checksum if there is state (cs != 0)
                                int cs = checksum(cc, co, includeTransient);
                                if (cs != 0)
                                {
                                    ret = HashUtil.hash(ret, cs);
                                    if (SC_DEBUG) log.debug("checksum: " + cc.getName() + " = " + cs + " -> " + ret);
                                }
                                else
                                    if (SC_DEBUG) log.debug("skip: " + cc.getName());
                            }
                            else // non-caom2 class
                            {
                                int hc = fo.hashCode();
                                ret = HashUtil.hash(ret, hc);
                                if (SC_DEBUG) log.debug("checksum: " + c.getName() + "." + f.getName() + " = " + hc + " -> " + ret);
                            }
                        }
                    }
                    else if (fo instanceof Date)
                    {
                        // only compare down to seconds
                        Date date = (Date) fo;
                        long sec = (date.getTime() / 1000L);
                        ret = HashUtil.hash(ret, sec);
                        if (SC_DEBUG) log.debug("checksum: " + c.getName() + "." + f.getName() + " = " + sec + " -> " + ret);
                    }
                    else // non-caom2 class
                    {
                        int hc = fo.hashCode();
                        if (hc != 0)
                        {
                            ret = HashUtil.hash(ret, hc);
                            if (SC_DEBUG) log.debug("checksum: " + c.getName() + "." + f.getName() + " = " + hc +  " -> " + ret);
                        }
                    }
                }
                else
                    if (SC_DEBUG) log.debug("skip: " + c.getName() + "." + f.getName());
            }
        }
        catch(IllegalAccessException bug)
        {
            throw new RuntimeException("BUG accessing field via reflection", bug);
        }
        return ret;
    }

    private boolean isLocalClass(Class c)
    {
        if ( c.isPrimitive() || c.isArray() )
            return false;
        String pname = c.getPackage().getName();
        return pname.startsWith(CAOM2);
    }

    // child is a CaomEntity
    static boolean isChildEntity(Field f)
        throws IllegalAccessException
    {
        return ( CaomEntity.class.isAssignableFrom(f.getType()));
    }
    
    // child collection is a non-empty Set<CaomEntity>
    static boolean isChildCollection(Field f)
        throws IllegalAccessException
    {
        if ( Set.class.isAssignableFrom(f.getType()) )
        {
            if (f.getGenericType() instanceof ParameterizedType)
            {
                ParameterizedType pt = (ParameterizedType) f.getGenericType();
                Type[] ptypes = pt.getActualTypeArguments();
                Class genType = (Class) ptypes[0];
                if ( CaomEntity.class.isAssignableFrom(genType))
                    return true;
            }
        }
        return false;
    }
    
    static SortedSet<Field> getStateFields(Class c, boolean includeTransient)
        throws IllegalAccessException
    {
        SortedSet<Field> ret = new TreeSet<>(new FieldComparator());
        Field[] fields = c.getDeclaredFields();
        for (Field f : fields)
        {
            int m = f.getModifiers();
            boolean inc = true;
            inc = inc && (includeTransient || !Modifier.isTransient(m));
            inc = inc && !Modifier.isStatic(m);
            inc = inc && !isChildCollection(f); // 0..* relations to other CaomEntity
            inc = inc && !isChildEntity(f); // 0..1 relation to other CaomEntity
            if (inc)
                ret.add(f);
        }
        Class sc = c.getSuperclass();
        while (sc != null && !CaomEntity.class.equals(sc))
        {
            ret.addAll(getStateFields(sc, includeTransient));
            sc = sc.getSuperclass();
        }
        return ret;
    }

    static SortedSet<Field> getChildFields(Class c)
        throws IllegalAccessException
    {
        SortedSet<Field> ret = new TreeSet<>(new FieldComparator());
        Field[] fields = c.getDeclaredFields();
        for (Field f : fields)
        {
            int m = f.getModifiers();
            if ( !Modifier.isTransient(m) && !Modifier.isStatic(m)
                    && (isChildCollection(f) || isChildEntity(f)) )
                ret.add(f);
        }
        Class sc = c.getSuperclass();
        while (sc != null && !CaomEntity.class.equals(sc))
        {
            ret.addAll(getChildFields(sc));
            sc = sc.getSuperclass();
        }
        return ret;
    }

    static Date max(Date d1, Date d2)
    {
        if (d1.compareTo(d2) >= 0)
            return d1;
        return d2;
    }
}
