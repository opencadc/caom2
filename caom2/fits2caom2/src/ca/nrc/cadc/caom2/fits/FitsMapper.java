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

import ca.nrc.cadc.caom2.CalibrationLevel;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.DataProductType;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.TargetType;
import ca.nrc.cadc.caom2.fits.exceptions.MapperException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Class to populate an instance using the Mapping class to provide
 * the data member values.
 * 
 * @author jburke
 */
public class FitsMapper
{
    private static Logger log = Logger.getLogger(FitsMapper.class);
    
    // Array containing Constructor parameter names.
    private static final String CTOR_UTYPES = "CTOR_UTYPES";
    
    private FitsMapping mapping;
    
    public FitsMapper(FitsMapping mapping)
    {
        this.mapping = mapping;
    }
    
    /**
     * Populate an instance with values from the FITS mapping.
     * 
     * @param instanceClass The Class of the instance.
     * @param instance The instance to populate.
     * @param utype utype of the instance.
     * @return A populated instance.
     * @throws MapperException 
     */
    public Object populate(Class instanceClass, Object instance, String utype)
        throws MapperException
    {
        String className = instanceClass.getName();
        log.debug("populate " + className + "[" + utype + "]");
        
        // Can't populate an enum.
        if (instanceClass.isEnum())
            return instance;
        
        // Instance is null, primitive type or Number.
        if (instance == null)
        {               
            // Check if there is a value in the mapping for this utype.
            String value = mapping.getMapping(utype);
            
            // If no value found, returns the null instance.
            // Else create a new instance set to the value.
            if (value != null)
            {
                // Found a value, instantiate the class using the value.
                try
                {
                    instance = getNumberOrString(instanceClass, value);
                }
                catch (NumberFormatException e)
                {
                    throw new MapperException("Unable to map " + utype + " to " + instanceClass.getName(), e);
                }
            }
        }
        
        // If this is a CAOM2 instance.
        else if (className.startsWith("ca.nrc.cadc.caom2"))
        {      
            // Get the public class fields.
            List<Field> fields = getPublicFields(instanceClass);
            for (Field field : fields)
            {
                // field utype.
                Class fieldClass = field.getDeclaringClass();
                String fieldUtype;
                if (fieldClass != instanceClass)
                {
                    int index = utype.lastIndexOf(".");
                    if (index == -1)
                    {
                        fieldUtype = fieldClass.getSimpleName() + "." + field.getName();
                    }
                    else
                    {
                        fieldUtype = utype.substring(0, index + 1) + fieldClass.getSimpleName() + "." + field.getName();
                    }
                }
                else
                {
                    fieldUtype = utype + "." + field.getName();
                }
                
                // If it's a caom field
                if (field.getType().getName().startsWith("ca.nrc.cadc.caom2"))
                {
                    if (field.getType().isEnum())
                    {
                        String value = mapping.getMapping(fieldUtype);
                        if (value != null)
                        {
                            try
                            {
                                field.set(instance, getEnumValue(field.getType(), value));
                                log.debug("assign: field(enum)[" + field.getName() + "] = " + value);
                            }
                            catch (Exception e)
                            {
                                throw new MapperException("Unable to map " + fieldUtype + " to " + value, e);
                            }
                        }
                        else
                            log.debug("field[" + field.getName() + "] skipped because value is null");
                    }
                    else
                    {
                        Object caomInstance = createInstance(field.getType(), fieldUtype, mapping);
                        if (caomInstance != null)
                        {
                            try
                            {
                                field.set(instance, caomInstance);
                                log.debug("assign: " + instance.getClass().getSimpleName() + "." + field.getName() + " = " + caomInstance);
                            }
                            catch (Exception e)
                            {
                                throw new MapperException("Unable to map " + fieldUtype + " to " + field.getType().getName(), e);
                            }
                        }
                        else
                            log.debug("field[" + field.getName() + "] skipped because value is null");
                    }
                }
                else
                {
                    // Get the value for the field from the mapping.
                    String value = mapping.getMapping(fieldUtype);

                    // Don't update fields with null values.
                    //if (value != null)
                    //{
                        // Update the field value.
                        try
                        {
                            Object val = Cast.cast(value, field.getType(), fieldUtype);
                            field.set(instance, val);
                        }
                        catch (Exception e)
                        {
                            throw new MapperException("Unable to map " + fieldUtype + " to " + value, e);
                        }
                        log.debug("field[" + field.getName() + "] = " + value);
                    //}
                    //else
                    //    log.debug("field[" + field.getName() + "] skipped because value is null");
                }
            } 

            // Invoke public get methods that return collections of non-CaomEntity's.
            invokePublicGetCollectionMethods(instanceClass, instance, utype, mapping);
            
            
            // Invoke public Set methods.
            //invokePublicSetMethods(instanceClass, instance, utype, mapping);
        }
        
        // A non-CAOM2 instance.
        else
        {
            String value = mapping.getMapping(utype);
            if (value != null)
            {
                try
                {   
                    instance = Cast.cast(value, instanceClass, utype);
                }
                catch (Exception e)
                {
                    throw new MapperException("Unable to map " + utype + " to " + value, e);
                }
            }
            log.debug("field(non-CAOM)[" + utype + "] = " + value);
        }
        return instance;
    }
    
    /**
     * Return a List of Field of public fields for the given Class.
     * Returned fields are public, but not static, final, or transient.
     * 
     * @param c Class to explore.
     * @return List of Field.
     */
    protected List<Field> getPublicFields(Class c)
    {
        // List of public fields for this class.
        List<Field> ret = new ArrayList<Field>();
        
        // Get the public class fields.
        Field[] fields = c.getFields();
        for (int i = 0; i < fields.length; i++)
        {
            if ( !c.equals(fields[i].getDeclaringClass()) )
                break;
            
            Field field = fields[i];

            // Only want public, non-static or final fields.
            if (!Modifier.isStatic(field.getModifiers()) &&
                !Modifier.isFinal(field.getModifiers()) &&
                !Modifier.isTransient(field.getModifiers()))
                ret.add(field);
        }
        return ret;
    }
    
    /**
     * Return a List of Field of private fields that are accessed
     * through a public get method.
     * 
     * @param c Class to explore.
     * @return List of Field.
     */
    protected List<Field> getPrivateFieldsWithGetter(Class c)
    {
        // List of private fields with get methods for this class.
        List<Field> fieldList = new ArrayList<Field>();
        
        // Get the public methods.
        Method[] methods = c.getMethods();
        for (Method method : methods)
        {
            if ( !c.equals(method.getDeclaringClass()) )
                break;
            
            // Look for get methods.
            if (method.getName().startsWith("get") && method.getName().length() > 4)
            {
                String name = method.getName().substring(3, 4).toLowerCase() +
                              method.getName().substring(4);
                try
                {
                    Field field = c.getDeclaredField(name);
                
                    // Not interested in static, final, or transient fields.
                    if (Modifier.isStatic(field.getModifiers()) ||
                        Modifier.isFinal(field.getModifiers()) ||
                        Modifier.isTransient(field.getModifiers()))
                        continue;
                    fieldList.add(field);
                }
                catch (NoSuchFieldException ignore)
                {
                    continue;
                }
            }
        }
        return fieldList;
    }
    
    /**
     * Returns a Map of public get method names(without the get at the front) 
     * and methods that return a Collection of non-CaomEntity's.
     * 
     * @param c Class to explore.
     * @return Map of names and methods.
     */
    protected Map<String, Method> getPublicGetCollectionMethods(Class c)
    {
        // List of get methods for this class.
        Map<String, Method> map = new HashMap<String, Method>();
        
        // Get the public methods.
        Method[] methods = c.getMethods();
        for (Method method : methods)
        {
            if ( !c.equals(method.getDeclaringClass()) )
                break;
            
            // Look for get methods.
            if ( !Modifier.isStatic(method.getModifiers()) && method.getName().startsWith("get") && method.getName().length() > 4)
            {
                String name = method.getName().substring(3, 4).toLowerCase() +
                              method.getName().substring(4);
                
                // Methods that return Collections.
                if (Collection.class.isAssignableFrom(method.getReturnType()))
                {
                    Type type = method.getGenericReturnType();
                    if (type instanceof ParameterizedType)
                    {                    
                        Class genericType = (Class)((ParameterizedType) type).getActualTypeArguments()[0];

                        // If the collection is not a CaomEntity add it to the list.
                        if (!CaomEntity.class.isAssignableFrom(genericType))
                        {
                            map.put(name, method);
                        }
                    }
                }
            }
        }
        return map;
    }
    
    /**
     * Returns a Map of public set method names(without the set at the front)
     * and methods.
     * 
     * @param c Class to explore.
     * @return Map of names and methods.
     */
    protected Map<String, Method> getPublicSetMethods(Class c)
    {
        // List of get methods for this class.
        Map<String, Method> map = new HashMap<String, Method>();
        
        // Get the public methods.
        Method[] methods = c.getMethods();
        for (Method method : methods)
        {
            if ( !c.equals(method.getDeclaringClass()) )
                break;
            
            // Look for set methods.
            if (method.getName().startsWith("set") && method.getName().length() > 4)
            {
                String name = method.getName().substring(3, 4).toLowerCase() +
                              method.getName().substring(4);
                
                map.put(name, method);
            }
        }
        return map;
    }
    
    /**
     * Create an instance of the given Class.
     * 
     * @param c Class to instantiate.
     * @param utype utype of the Class.
     * @param mapping FITS mapping.
     * @return an instance of the Class.
     * @throws MapperException 
     */
    protected Object createInstance(Class c, String utype, FitsMapping mapping)
        throws MapperException
    {
        // The new instance.
        Object instance;
            
        if (c.isEnum())
        {
            instance = createEnumInstance(c, utype, mapping);
        }
        else if (c.getName().startsWith("ca.nrc.cadc.caom2"))
        {
            instance = createCAOMInstance(c, utype, mapping);
        }
        else
        {
            instance = createDefaultInstance(c, utype, mapping);
        }
        
        // Populate the instance fields.
        instance = populate(c, instance, utype);
        
        // return the new populated instance.
        return instance;
    }
    
    /**
     * Create an instance of an enum for the given enum Class.
     * 
     * @param c Class of the enum.
     * @param utype utype of the enum.
     * @param mapping FITS Mapping.
     * @return an instance of an enum for the given Class.
     * @throws MapperException 
     */
    private Object createEnumInstance(Class c, String utype, FitsMapping mapping)
        throws MapperException
    {
        // The new instance.
        Object instance = null;
        
        // CAOM2 enum types have a private variable 'value' that defines the type of the enum.
        try
        {
            String value = mapping.getMapping(utype);
            if (value != null)
                instance = getEnumValue(c, value);
        }
        catch (SecurityException e)
        {
            throw new MapperException("Unable to access enum variable 'value' in " + c.getName(), e);
        }
        log.debug("createEnumInstance " + c.getName() + "[" + utype + "] = " + instance);
        return instance;
    }
    
    /**
     * Returns the name for the given enum value.
     * 
     * @param c Class of the enum.
     * @param value of the enum.
     * @return name of the given enum value.
     */
    protected static Object getEnumValue(Class c, String value)
    {
        if (c == DataProductType.class)
        {
            return DataProductType.toValue(value);
        }
        else if (c == ProductType.class)
        {
            return ProductType.toValue(value);
        }
        else if (c == TargetType.class)
        {
            return TargetType.toValue(value);
        }
        else if (c == ObservationIntentType.class)
        {
            return ObservationIntentType.toValue(value);
        }
        else if (c == CalibrationLevel.class)
        {
            return CalibrationLevel.toValue(Integer.parseInt(value));
        }
        throw new UnsupportedOperationException("unexpected enum class: " + c.getName());
    }
    
    /**
     * Creates an instance of the given CAOM2 Class.
     * 
     * @param c Class to instantiate.
     * @param utype utype of the Class
     * @param mapping FITS mapping.
     * @return an instance of the given Class.
     * @throws MapperException 
     */
    protected Object createCAOMInstance(Class c, String utype, FitsMapping mapping)
        throws MapperException
    {
        // The new instance.
        Object instance = null;
        
        // get all fields;
        Field[] fields = c.getDeclaredFields();

        // Get all public constructors.
        Constructor[] constructors = c.getConstructors();
        
        // No-arg constructor.
        Constructor noArgConstructor = null;

        // For each constructor.
        for (Constructor constructor : constructors)
        {
            log.debug("constructor " + constructor);
            
            // Constructor parameter types, could be zero length array
            // for no-arg constructor.
            Class[] types = constructor.getParameterTypes();
            
            // Save the no-arg constructor. For Part need to try constructors
            // with parameters first, then lastly the no-arg.
            if (types.length == 0)
            {
                noArgConstructor = constructor;
                continue;
            }
        
            // Array to hold constructor parameter Objects.
            Object[] parameters;

            // Constructor parameter types, could be zero length array for no-arg constructor.            
            if (isAmbiguousTypes(types))
            {
                parameters = getAmbiguousConstructorParameters(c, types, utype, mapping);
            }
            else
            {
                parameters = getConstructorParameters(types, fields, utype, mapping);
            }

            // Check if all parameters are null.
            boolean allNull = true;
            for (Object object : parameters)
            {
                if (object != null)
                    allNull = false;
            }
            
            // Don't try and construct an object will all null parameters.
            if (allNull)
            {
                log.debug("All constructor parameters found to be null for " + c.getName());
            }
            else
            {
                try
                {
                    // Instantiate the constructor for the Object.
                    instance = constructor.newInstance(parameters);
                    break;
                }
                catch (Exception ignore)
                {
                    log.debug("Unable to instantiate Object " + c.getName());
                }
            }
        }
        
        // Try the no-arg constructor.
        if (instance == null && noArgConstructor != null)
        {
            try
            {
                instance = instantiateNoArgConstructor(noArgConstructor, utype, mapping);
            }
            catch (Exception e)
            {
                log.debug("Unable to instantiate Object using no-arg constructor " + c.getName(), e);
            }
        }
        
        log.debug("createCAOMInstance " + c.getName() + "[" + utype + "] = " + instance);
        return instance;
    }
    
    /**
     * Creates an instance of the given Class.
     * 
     * @param c Class to instantiate.
     * @param utype utype of the Class.
     * @param mapping FITS mapping.
     * @return an instance of the given Class.
     * @throws MapperException 
     */
    protected Object createDefaultInstance(Class c, String utype, FitsMapping mapping)
        throws MapperException
    {
        // The new instance.
        Object instance;
        
        // Can't instantiate a primitive or a Number.
        if (c == double.class || c == Double.class ||
            c == int.class || c == Integer.class ||
            c == long.class || c == Long.class ||
            c == float.class || c == Float.class ||
            c == short.class || c == Short.class ||
            c == String.class)
        {
            instance = null;
        }
        else
        {    
            try
            {
                instance = c.newInstance();
            }
            catch (Exception e)
            {
                throw new MapperException("Unable to instantiate Object " + c.getName(), e);
            }
        }
        log.debug("createDefaultInstance " + c.getName() + "[" + utype + "]");
        return instance;
    }
    
    /**
     * Determine if the array of Classes is ambiguous, i.e. are there two
     * Classes of the same type in the array.
     * 
     * @param types Array of Classes.
     * @return true if the Classes are ambiguous, false otherwise.
     */
    protected boolean isAmbiguousTypes(Class[] types)
    {
        boolean ambiguous = false;
        for (int i = 0; i < types.length; i++)
        {
            for (int j = 0; j < types.length; j++)
            {
                if (i != j && types[i] == types[j])
                {
                    ambiguous = true;
                    break;
                }
            }
            if (ambiguous)
                break;
        }
        log.debug("isAmbiguousTypes" + Arrays.asList(types) + " = " + ambiguous);
        return ambiguous;
    }
    
    /**
     * Given an array containing Classes of the Constructor arguments, return
     * an array containing the instantiated class arguments.
     * 
     * @param types Class[] of argument Classes.
     * @param fields Fields of the declaring Class.
     * @param utype utype of the declaring Class.
     * @param mapping FITS Mapping.
     * @return
     * @throws MapperException 
     */
    protected Object[] getConstructorParameters(Class[] types, Field[] fields, String utype, FitsMapping mapping)
        throws MapperException
    {                
        // Array to hold constructor parameter Objects.
        Object[] parameters = new Object[types.length];
        
        // Try and get an instance of each parameter type.
        for (int i = 0; i < types.length; i++)
        {            
            // Find the matching field class for this parameter class,
            // which gives the name of the field needed to resolve the utype.
            for (Field field : fields)
            {
                // Only interested in non-static or non-final, public or private fields.
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers()) ||
                    !Modifier.isPublic(field.getModifiers()) && !Modifier.isPrivate(field.getModifiers()))
                        continue;
                
                if (field.getType() == types[i])
                {
                    parameters[i] = createInstance(types[i], utype + "." + field.getName(), mapping);
                    break;
                }
            }
        }
        log.debug("getConstructorParameters " + Arrays.asList(parameters));
        return parameters;
    }
    
    /**
     * If a Constructor has arguments that are ambiguous, i.e. two doubles, look
     * for a static array containing the Constructor argument names.
     * 
     * @param c Class declaring the Constructor.
     * @param types Class[] of argument Classes.
     * @param utype utype of the declaring Class.
     * @param apping FITS Mapping.
     * @return Array of Objects for the given constructor.
     * @throws MapperException 
     */
    protected Object[] getAmbiguousConstructorParameters(Class c, Class[] types, String utype, FitsMapping mapping)
        throws MapperException
    {
        // Array to hold constructor parameter Objects.
        Object[] parameters = new Object[types.length];
         
        // Get the CTOR_UTYPES array containing field names of constructor parameters.
        String[] constructorNames = null;
        try
        {
            Field field = c.getDeclaredField(CTOR_UTYPES);
            constructorNames = (String[]) field.get(null);
        }
        catch (NoSuchFieldException e)
        {
            throw new MapperException("CTOR_UTYPES not found in " + c.getName(), e);
        }
        catch (SecurityException e)
        {
            throw new MapperException("Not premitted to access CTOR_UTYPES in " + c.getName(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new MapperException("CTOR_UTYPES unaccessible in " + c.getName(), e);
        }

        // For each constructor argument.
        for (int i = 0; i < types.length; i++)
        {
            parameters[i] = createInstance(types[i], utype + "." + constructorNames[i], mapping);
        }

        log.debug("getAmbiguousConstructorParameters " + Arrays.asList(parameters));
        return parameters;
    }
    
    /**
     * Instantiate an instance using it's no-arg Constructor.
     * To avoid creating an empty instance, first get a list
     * of the instances public fields, and private fields accessed using a get
     * method, and check if any of the fields have a value in the mapping. If a
     * field does have a value, instantiate using the Constructor and return
     * the instance, else return null.
     * 
     * @param constructor No-arg Constructor.
     * @param utype utype of the declaring Class.
     * @param mapping FITS mapping.
     * @return An instantiated Object, else null.
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException 
     */
    protected Object instantiateNoArgConstructor(Constructor constructor, String utype, FitsMapping mapping)
        throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        // Has a value been found in the mapping for a Class member.
        boolean foundValue = false;
        
        // Get public fields.
        List<Field> fields = getPublicFields(constructor.getDeclaringClass());
        
        // Check for a value in the mapping for the public fields.
        for (Field field : fields)
        {
            String value = mapping.getMapping(utype + "." + field.getName());
            if (value == null)
                continue;
            foundValue = true;
            break;
        }
        
        // If a mapping value is found, instantiate via the Constructor and return.
        if (foundValue)
        {
            return constructor.newInstance(new Object[0]);
        }
        
        // Get private fields from their public get methods.
        fields = getPrivateFieldsWithGetter(constructor.getDeclaringClass());
        
        // Check for a value in the mapping for the private fields.
        for (Field field : fields)
        {
            String value = mapping.getMapping(utype + "." + field.getName());
            if (value == null)
                continue;
            foundValue = true;
            break;
        }
        
        // If a mapping value is found, instantiate via the Constructor and return.
        if (foundValue)
        {
            return constructor.newInstance(new Object[0]);
        }
        
        // No value found, return null.
        return null;
    }
    
    /**
     * 
     * @param c
     * @param value
     * @throws NumberFormatException if a Number can't be created from the value.
     * @return 
     */
    protected Object getNumberOrString(Class c, String value)
    {
        Object instance = null;
        if (c == double.class || c == Double.class)
        {
            return Double.valueOf(value);
        }
        else if (c == int.class || c == Integer.class)
        {
            return Integer.valueOf(value);
        }
        else if (c == long.class || c == Long.class)
        {
            return Long.valueOf(value);
        }
        else if (c == float.class || c == Float.class)
        {
            return Float.valueOf(value);
        }
        else if (c == short.class || c == Short.class)
        {
            return Short.valueOf(value);
        }
        else if (c == String.class)
        {
            return value;
        }
        return instance;
    }
    
    /**
     * Gets all public get methods that return a collection whose type isn't a
     * CaomEntity, and invokes the method.
     * 
     * @param c Class that contains the methods.
     * @param instance Object that contains the methods.
     * @param utype utype of the instance.
     * @param mapping FITS mapping.
     * @throws MapperException 
     */
    protected void invokePublicGetCollectionMethods(Class c, Object instance, String utype, FitsMapping mapping)
        throws MapperException
    {
        Map<String, Method> getters = getPublicGetCollectionMethods(c);
        Set<Map.Entry<String, Method>> getEntrySet = getters.entrySet();
        for (Map.Entry<String, Method> entry : getEntrySet)
        {
            // Check for a value for the field this method accesses.
            String fieldUtype = utype + "." + entry.getKey();
            String value = mapping.getMapping(fieldUtype);
            log.debug("invokePublicGetCollectionMethods: utype=" + utype + " fieldUType=" + fieldUtype + " value=" + value);
            if (value == null)
                continue;

            Method method = entry.getValue();

            // Lists are white space delimited.
            // assuming String type for List for keywords.
            if (List.class.isAssignableFrom(method.getReturnType()))
            {
                try
                {
                    // Get the list.
                    List<String> list = (List<String>) method.invoke(instance, new Object[] {});

                    // Add the values to the List.
                    String[] values = value.split("[\\s]+");
                    list.addAll(Arrays.asList(values));
                    log.debug(method.getName() + "(List)[" + utype + "] = " + list);
                }
                catch (Exception e)
                {   
                    String message = e.getMessage();
                    if (message == null && e.getCause() != null)
                        message = e.getCause().getMessage();
                    String error = "Unable to add to list " + entry.getKey() + 
                                   " in " + c.getSimpleName() + 
                                   " because " + message;
                    throw new MapperException(error, e);
                }
            }

            // Sets are comma delimited.
            else if (Set.class.isAssignableFrom(method.getReturnType()))
            {
                try
                {
                    // Find the type of the Set.
                    Type type = method.getGenericReturnType();
                    if (type instanceof ParameterizedType)
                    {                    
                        Class setClass = (Class)((ParameterizedType) type).getActualTypeArguments()[0];
                    
                        // TODO: turn this inside-out so if is outside to get rid of
                        // warnings in set.add(E)
                        Set set = (Set) method.invoke(instance, new Object[] {});

                        // Add the values to the List.
                        String[] values = value.split("[\\s]+");
                        for (String s : values)
                        {
                            if (s != null && !s.trim().isEmpty())
                            {
                                if (PlaneURI.class.isAssignableFrom(setClass))
                                    set.add(new PlaneURI(new URI(s.trim())));
                                else if (ObservationURI.class.isAssignableFrom(setClass))
                                    set.add(new ObservationURI(new URI(s.trim())));
                            }
                        }
                        log.debug(method.getName() + "(Set)[" + utype + "] = " + set);
                    }
                }
                catch (Exception e)
                {
                    String message = e.getMessage();
                    if (message == null && e.getCause() != null)
                        message = e.getCause().getMessage();
                    String error = "Unable to add to set " + entry.getKey() + 
                                    " in " + c.getSimpleName() +
                                    " because " + message;
                    throw new MapperException(error, e);
                }
            }

            // Unsupported collection type.
            else
            {
                String error = "Unsupported Collection type " + 
                                method.getReturnType().getName() +
                                " in " + c.getSimpleName();
                throw new MapperException(error);
            }
        }  
    }
    
    /**
     * Gets and invokes all public set methods.
     * 
     * @param c Class that contains the methods.
     * @param instance Object that contains the methods.
     * @param utype utype of the instance.
     * @param mapping FITS mapping.
     * @throws MapperException 
     */
    protected void invokePublicSetMethods(Class c, Object instance, String utype, FitsMapping mapping)
        throws MapperException
    {
        // Get the public set methods.
        Map <String, Method> setters = getPublicSetMethods(c);
        Set<Map.Entry<String, Method>> setEntrySet = setters.entrySet();
        for (Map.Entry<String, Method> entry : setEntrySet)
        {
            String fieldUtype = utype + "." + entry.getKey();
            Method method = entry.getValue();

            // Get the method parameters.
            Class[] classes = method.getParameterTypes();
            Object[] parameters = new Object[classes.length];
            for (int i = 0; i < classes.length; i++)
            {
                Class clazz = classes[i];
                parameters[i] = createInstance(clazz, fieldUtype, mapping);
                
                // A set method should not be setting a null value.
                if (parameters[i] == null)
                    return;
            }

            // Invoke the method.
            try
            {
                method.invoke(instance, parameters);
                log.debug(method.getName() + "[" + utype + "] = " + parameters);
            }
            catch (Exception e)
            {
                String message = e.getMessage();
                if (message == null && e.getCause() != null)
                    message = e.getCause().getMessage();
                String error = "Unable to invoke method " + method.getName() + 
                                " in " + c.getName() +
                                " because " + message;
                throw new MapperException(error, e);
            }
        }   
    }
    
}
