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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.dali.Circle;
import ca.nrc.cadc.dali.DoubleInterval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.dali.Polygon;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.UUIDComparator;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.persist.Entity;

/**
 * Base class for CAOM entity classes. The base class contains the UUID, modification
 * timestamps, and metadata checksums.
 * 
 * @author pdowler
 */
public abstract class CaomEntity extends Entity {
    private static final Logger log = Logger.getLogger(CaomEntity.class);
    
    // Entity metaChecksum algorithm setup: DO NOT CHANGE
    static final boolean ENTITY_TRUNCATE_DATES = false; // was true but trying false for 2.5
    static final boolean ENTITY_DIGEST_FIELD_NAMES = true;
    static final boolean ENTITY_DIGEST_FIELD_NAMES_LOWER = true;
    
    private static final String CAOM2 = CaomEntity.class.getPackage().getName();
    static boolean MCS_DEBUG = false; // way to much debug when true
    static boolean OVERRRIDE_CORRECT_UUID_SORT = false; 

    // state but not part of meta checksum
    private transient Date maxLastModified;
    private transient URI accMetaChecksum;
    
    protected CaomEntity() {
        super(ENTITY_TRUNCATE_DATES, ENTITY_DIGEST_FIELD_NAMES, ENTITY_DIGEST_FIELD_NAMES_LOWER);
    }

    protected CaomEntity(UUID id) {
        super(id, ENTITY_TRUNCATE_DATES, ENTITY_DIGEST_FIELD_NAMES, ENTITY_DIGEST_FIELD_NAMES_LOWER);
    }

    @Override
    protected boolean isDataModelClass(Class c) {
        // imported data model components
        if (DoubleInterval.class.equals(c)) {
            return true;
        }
        if (Point.class.equals(c)) {
            return true;
        }
        if (Circle.class.equals(c)) {
            return true;
        }
        if (Polygon.class.equals(c)) {
            return true;
        }
        if (Shape.class.equals(c)) {
            return true;
        }
        if (MultiShape.class.equals(c)) {
            return true;
        }
        return super.isDataModelClass(c);
    }

    /**
     * Get the maximum timestamp of the last modification of the state of this
     * object and any child entities.
     *
     * @return
     */
    public final Date getMaxLastModified() {
        return maxLastModified;
    }

    /**
     * Get the accumulated checksum for the state of this entity. The
     * accumulated checksum includes the state of this entity and all child
     * entities.
     * 
     * @return checksum URI in the form algorithm:value
     */
    public URI getAccMetaChecksum() {
        return accMetaChecksum;
    }

    /**
     * Compute new metadata checksum for this entity. The computed value is
     * based on the current state and is returned without changing the stored
     * value that is accessed via the getMetaChecksum() method.
     * 
     * @param digest
     * @return
     */
    public URI computeMetaChecksumV1(MessageDigest digest) {
        try {
            calcMetaChecksumV1(this.getClass(), this, digest);
            byte[] metaChecksumBytes = digest.digest();
            String hexMetaChecksum = HexUtil.toHex(metaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, hexMetaChecksum, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Unable to create metadata checksum URI for "
                            + this.getClass().getName(),
                    e);
        }
    }

    private void calcMetaChecksumV1(Class c, Object o, MessageDigest digest) {
        // calculation order:
        // 1. CaomEntity.id for entities
        // 2. CaomEntity.metaProducer
        // 3. state fields in alphabetic order; depth-first recursion so
        // foo.abc.x comes before foo.def
        // value handling:
        // Date: truncate time to whole number of seconds and treat as a long
        // String: UTF-8 encoded bytes
        // URI: UTF-8 encoded bytes of string representation
        // float: IEEE754 single (4 bytes)
        // double: IEEE754 double (8 bytes)
        // boolean: convert to single byte, false=0, true=1 (1 bytes)
        // byte: as-is (1 byte)
        // short: (2 bytes, network byte order == big endian))
        // integer: (4 bytes, network byte order == big endian)
        // long: (8 bytes, network byte order == big endian)
        try {
            if (o instanceof CaomEntity) {
                CaomEntity ce = (CaomEntity) o;
                digest.update(primitiveValueToBytesV1(ce.getID(), "CaomEntity.id", digest.getAlgorithm()));
                if (MCS_DEBUG) {
                    log.debug("metaChecksum: " + ce.getClass().getSimpleName() + ".id " + ce.getID());
                }
                if (ce.metaProducer != null) {
                    digest.update(primitiveValueToBytesV1(ce.metaProducer, "CaomEntity.metaProducer", digest.getAlgorithm()));
                    if (MCS_DEBUG) {
                        log.debug("metaChecksum: " + ce.getClass().getSimpleName() + ".metaProducer " + ce.metaProducer);
                    }
                }
            }

            SortedSet<Field> fields = getStateFields(c);
            for (Field f : fields) {
                String cf = c.getSimpleName() + "." + f.getName();
                // if (SC_DEBUG) log.debug("check: " + cf);
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) {
                    Class ac = fo.getClass(); // actual class
                    if (fo instanceof CaomEnum) {
                        // use ce.getValue
                        CaomEnum ce = (CaomEnum) fo;
                        digest.update(primitiveValueToBytesV1(ce.getValue(), cf, digest.getAlgorithm()));
                    } else if (isLocalClass(ac)) {
                        calcMetaChecksumV1(ac, fo, digest);
                    } else if (fo instanceof Collection) {
                        Collection stuff = (Collection) fo;
                        Iterator i = stuff.iterator();
                        while (i.hasNext()) {
                            Object co = i.next();
                            Class cc = co.getClass();
                            if (co instanceof CaomEnum) {
                                // use ce.getValue
                                CaomEnum ce = (CaomEnum) co;
                                digest.update(
                                        primitiveValueToBytesV1(ce.getValue(), cf, digest.getAlgorithm()));
                            } else if (isLocalClass(cc)) {
                                calcMetaChecksumV1(cc, co, digest);
                            } else { // non-caom2 class ~primtive value
                                digest.update(primitiveValueToBytesV1(co, cf, digest.getAlgorithm()));
                            }
                        }
                    } else { // non-caom2 class ~primtive value
                        digest.update(primitiveValueToBytesV1(fo, cf, digest.getAlgorithm()));
                    }
                } else if (MCS_DEBUG) {
                    log.debug("skip: " + cf);
                }
            }

        } catch (IllegalAccessException bug) {
            throw new RuntimeException(
                    "Unable to calculate metaChecksum for class " + c.getName(),
                    bug);
        }
    }

    private byte[] primitiveValueToBytesV1(Object o, String name, String digestAlg) {
        byte[] ret = null;
        int len = 0;
        if (o instanceof Byte) {
            ret = HexUtil.toBytes((Byte) o); // auto-unbox
        } else if (o instanceof Short) {
            ret = HexUtil.toBytes((Short) o); // auto-unbox
        } else if (o instanceof Integer) {
            ret = HexUtil.toBytes((Integer) o); // auto-unbox
        } else if (o instanceof Long) {
            ret = HexUtil.toBytes((Long) o); // auto-unbox
        } else if (o instanceof Boolean) {
            Boolean b = (Boolean) o;
            if (b) {
                ret = HexUtil.toBytes((byte) 1);
            } else {
                ret = HexUtil.toBytes((byte) 0);
            }
        } else if (o instanceof Date) {
            // only compare down to seconds
            Date date = (Date) o;
            long sec = (date.getTime() / 1000L);
            ret = HexUtil.toBytes(sec);
        } else if (o instanceof Float) {
            ret = HexUtil.toBytes(Float.floatToIntBits((Float) o)); /* auto-unbox,
                                                                     IEEE754
                                                                     single */
        } else if (o instanceof Double) {
            ret = HexUtil.toBytes(Double.doubleToLongBits((Double) o)); /* auto-unbox,
                                                                         IEEE754
                                                                         double */
        } else if (o instanceof String) {
            try {
                String s = (String) o;
                len = s.length();
                ret = ((String) o).trim().getBytes("UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(
                        "BUG: failed to encode String in UTF-8", ex);
            }
        } else if (o instanceof URI) {
            try {
                String s = ((URI) o).toASCIIString();
                len = s.length();
                ret = s.trim().getBytes("UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(
                        "BUG: failed to encode String in UTF-8", ex);
            }
        } else if (o instanceof UUID) {
            UUID uuid = (UUID) o;
            byte[] msb = HexUtil.toBytes(uuid.getMostSignificantBits());
            byte[] lsb = HexUtil.toBytes(uuid.getLeastSignificantBits());
            ret = new byte[16];
            System.arraycopy(msb, 0, ret, 0, 8);
            System.arraycopy(lsb, 0, ret, 8, 8);
        }

        if (ret != null) {
            if (MCS_DEBUG) {
                try {
                    MessageDigest md  = MessageDigest.getInstance(digestAlg);
                    byte[] dig = md.digest(ret);
                    String cn = o.getClass().getSimpleName();
                    if (len > 0) {
                        cn += "[" + len + "]";
                    }
                    log.debug(cn + " " + name + " = " + o.toString()
                        + " -- " + HexUtil.toHex(dig));
                } catch (Exception ignore) {
                    log.debug("OOPS", ignore);
                }
            }
            return ret;
        }

        throw new UnsupportedOperationException(
                "unexpected primitive/value type: " + o.getClass().getName());
    }

    /**
     * Compute accumulated metadata checksum for this entity. The computed
     * value is based on the current state of this entity and all child
     * entities. The value is returned without changing the stored value that is
     * accessed via the getAccumulatedMetaChecksum() method.
     * 
     * @param digest
     * @return
     */
    public URI computeAccMetaChecksum(MessageDigest digest) {
        try {
            calcAccMetaChecksum(this.getClass(), this, digest);
            byte[] accMetaChecksumBytes = digest.digest();
            String accHexMetaChecksum = HexUtil.toHex(accMetaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, accHexMetaChecksum, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Unable to create metadata checksum URI for "
                            + this.getClass().getName(),
                    e);
        }
    }

    private void calcAccMetaChecksum(Class c, Object o, MessageDigest digest) {

        // calculation order:
        // 1. bytes of the metaChecksum of the entity
        // 2. bytes of the accMetaChecksum of child entities accumulated in
        // CaomEntity.id order
        try {
            // clone for use with child entities
            MessageDigest md = MessageDigest.getInstance(digest.getAlgorithm());

            // add this object's complete metadata
            calcMetaChecksum(c, o, new Entity.MessageDigestWrapper(md));
            digest.update(md.digest());
            // call to digest also resets

            SortedSet<Field> fields = getChildFields(c);
            if (MCS_DEBUG) {
                log.debug("calcAccMetaChecksum: " + c.getName() + " has "
                        + fields.size() + " child fields");
            }

            for (Field f : fields) {
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) {
                    if (MCS_DEBUG) {
                        log.debug("calcAccMetaChecksum: value type is "
                                + fo.getClass().getName());
                    }
                    if (fo instanceof Collection) {
                        Set<CaomEntity> children = (Set<CaomEntity>) fo;
                        SortedMap<UUID, byte[]> sorted = new TreeMap<>(new UUIDComparator());
                        if (OVERRRIDE_CORRECT_UUID_SORT) {
                            // this reverts to default java ordering of UUID which is wrong but
                            // useful for diagnosing accMetaChecksum issues
                            sorted = new TreeMap<>();
                        }
                        Iterator<CaomEntity> i = children.iterator();
                        while (i.hasNext()) {
                            CaomEntity ce = i.next();
                            Class cc = ce.getClass();
                            if (MCS_DEBUG) {
                                log.debug("calculate: " + ce.getID());
                            }
                            calcAccMetaChecksum(cc, ce, md);
                            byte[] bb = md.digest();
                            // call to digest also resets
                            sorted.put(ce.getID(), bb);
                        }
                        Iterator<Map.Entry<UUID, byte[]>> si = sorted.entrySet().iterator();
                        while (si.hasNext()) {
                            Map.Entry<UUID, byte[]> me = si.next();
                            if (MCS_DEBUG) {
                                log.debug("accumulate: " + me.getKey());
                            }
                            digest.update(me.getValue());
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "found single child field " + f.getName() + " in " + c.getName());
                    }
                }
                // child sets are never null
                // else if (SC_DEBUG) log.debug("skip: " + c.getName() + "." +
                // f.getName());
            }
        } catch (IllegalAccessException bug) {
            throw new RuntimeException(
                    "BUG: Unable to calculate metaChecksum for class "
                            + c.getName(),
                    bug);
        } catch (NoSuchAlgorithmException oops) {
            throw new RuntimeException("BUG: Unable to clone MessageDigest "
                    + digest.getAlgorithm(), oops);
        }
    }

    public URI computeAccMetaChecksumV1(MessageDigest digest) {
        try {
            calcAccMetaChecksumV1(this.getClass(), this, digest);
            byte[] accMetaChecksumBytes = digest.digest();
            String accHexMetaChecksum = HexUtil.toHex(accMetaChecksumBytes);
            String alg = digest.getAlgorithm().toLowerCase();
            return new URI(alg, accHexMetaChecksum, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    "Unable to create metadata checksum URI for "
                            + this.getClass().getName(),
                    e);
        }
    }

    private void calcAccMetaChecksumV1(Class c, Object o, MessageDigest digest) {

        // calculation order:
        // 1. bytes of the metaChecksum of the entity
        // 2. bytes of the accMetaChecksum of child entities accumulated in
        // CaomEntity.id order
        try {
            // clone for use with child entities
            MessageDigest md = MessageDigest.getInstance(digest.getAlgorithm());

            // add this object's complete metadata
            calcMetaChecksumV1(c, o, md);
            digest.update(md.digest());
            // call to digest also resets
            // md.reset();

            SortedSet<Field> fields = getChildFields(c);
            if (MCS_DEBUG) {
                log.debug("calcAccMetaChecksum: " + c.getName() + " has "
                        + fields.size() + " child fields");
            }

            for (Field f : fields) {
                f.setAccessible(true);
                Object fo = f.get(o);
                if (fo != null) {
                    if (MCS_DEBUG) {
                        log.debug("calcAccMetaChecksum: value type is "
                                + fo.getClass().getName());
                    }
                    if (fo instanceof Collection) {
                        Set<CaomEntity> children = (Set<CaomEntity>) fo;
                        SortedMap<UUID, byte[]> sorted = new TreeMap<>(new UUIDComparator());
                        if (OVERRRIDE_CORRECT_UUID_SORT) {
                            // this reverts to default java ordering of UUID which is wrong but
                            // uweful for diagnosing accMetaChecksum issues
                            sorted = new TreeMap<>();
                        }
                        Iterator<CaomEntity> i = children.iterator();
                        while (i.hasNext()) {
                            CaomEntity ce = i.next();
                            Class cc = ce.getClass();
                            if (MCS_DEBUG) {
                                log.debug("calculate: " + ce.getID());
                            }
                            calcAccMetaChecksumV1(cc, ce, md);
                            byte[] bb = md.digest();
                            // call to digest also resets
                            // md.reset();
                            sorted.put(ce.getID(), bb);
                        }
                        Iterator<Map.Entry<UUID, byte[]>> si = sorted.entrySet().iterator();
                        while (si.hasNext()) {
                            Map.Entry<UUID, byte[]> me = si.next();
                            if (MCS_DEBUG) {
                                log.debug("accumulate: " + me.getKey());
                            }
                            digest.update(me.getValue());
                        }
                    } else {
                        throw new UnsupportedOperationException(
                                "found single child field " + f.getName() + " in " + c.getName());
                    }
                }
                // child sets are never null
                // else if (SC_DEBUG) log.debug("skip: " + c.getName() + "." +
                // f.getName());
            }
        } catch (IllegalAccessException bug) {
            throw new RuntimeException(
                    "BUG: Unable to calculate metaChecksum for class "
                            + c.getName(),
                    bug);
        } catch (NoSuchAlgorithmException oops) {
            throw new RuntimeException("BUG: Unable to clone MessageDigest "
                    + digest.getAlgorithm(), oops);
        }
    }

    private boolean isLocalClass(Class c) {
        if (c.isPrimitive() || c.isArray()) {
            return false;
        }
        String pname = c.getPackage().getName();
        return pname.startsWith(CAOM2);
    }

    // child is a CaomEntity
    /*
    static boolean isChildEntity(Field f) throws IllegalAccessException {
        return (CaomEntity.class.isAssignableFrom(f.getType()));
    }
    */
    // child collection is a non-empty Set<CaomEntity>
    /*
    static boolean isChildCollection(Field f) throws IllegalAccessException {
        if (Set.class.isAssignableFrom(f.getType())) {
            if (f.getGenericType() instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) f.getGenericType();
                Type[] ptypes = pt.getActualTypeArguments();
                Class genType = (Class) ptypes[0];
                if (CaomEntity.class.isAssignableFrom(genType)) {
                    return true;
                }
            }
        }
        return false;
    }
    */
    /*
    static SortedSet<Field> getStateFields(Class c, boolean includeTransient)
            throws IllegalAccessException {
        SortedSet<Field> ret = new TreeSet<>(new FieldComparator());
        Field[] fields = c.getDeclaredFields();
        for (Field f : fields) {
            int m = f.getModifiers();
            boolean inc = true;
            inc = inc && (includeTransient || !Modifier.isTransient(m));
            inc = inc && !Modifier.isStatic(m);
            inc = inc && !isChildCollection(f); // 0..* relations to other CaomEntity
            inc = inc && !isChildEntity(f); // 0..1 relation to other CaomEntity
            if (inc) {
                ret.add(f);
            }
        }
        Class sc = c.getSuperclass();
        while (sc != null && !CaomEntity.class.equals(sc)) {
            ret.addAll(getStateFields(sc, includeTransient));
            sc = sc.getSuperclass();
        }
        return ret;
    }
    */
    /*
    static SortedSet<Field> getChildFields(Class c)
            throws IllegalAccessException {
        SortedSet<Field> ret = new TreeSet<>(new FieldComparator());
        Field[] fields = c.getDeclaredFields();
        for (Field f : fields) {
            int m = f.getModifiers();
            if (!Modifier.isTransient(m) && !Modifier.isStatic(m)
                    && (isChildCollection(f) || isChildEntity(f))) {
                ret.add(f);
            }
        }
        Class sc = c.getSuperclass();
        while (sc != null && !CaomEntity.class.equals(sc)) {
            ret.addAll(getChildFields(sc));
            sc = sc.getSuperclass();
        }
        return ret;
    }
    */

    static Date max(Date d1, Date d2) {
        if (d1.compareTo(d2) >= 0) {
            return d1;
        }
        return d2;
    }
}
