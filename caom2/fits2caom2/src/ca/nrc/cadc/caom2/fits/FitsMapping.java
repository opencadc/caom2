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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCard;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class FitsMapping
{    
    private static Logger log = Logger.getLogger(FitsMapping.class);
    
    public static final String IGNORE = "{ignore}";
    
    public static final String POSITION_AXIS_1 = "positionAxis1";
    public static final String POSITION_AXIS_2 = "positionAxis2";
    public static final String ENERGY_AXIS = "energyAxis";
    public static final String POLARIZATION_AXIS = "polarizationAxis";
    public static final String TIME_AXIS = "timeAxis";
    
    // Configuration files.
    private Map<String,String> config;
    private List<String> ignoreUTypes;
    private FitsValuesMap override;
    private FitsValuesMap defaults;
    private Properties arguments;

    // Primary and current FITS header.
    public Header primary;
    public Header header;
    
    // Current uri and extension number.
    public String uri;
    public Object extension;
    
    // WCS axes numbers.
    public Integer positionAxis1;
    public Integer positionAxis2;
    public Integer energyAxis;
    public Integer polarizationAxis;
    public Integer timeAxis;
    
    /**
     * Default constructor.
     * 
     * @param config Map of the config file.
     * @param override Map of the override file.
     * @param defaults Map of the defaults file.
     */
    public FitsMapping(Map<String,String> config, FitsValuesMap defaults, FitsValuesMap override)
    {
        // The config file must exist.
        if (config == null)
            throw new IllegalArgumentException("Config file must be specified.");
        
        this.config = config;
        this.defaults = defaults;
        this.override = override;
        this.arguments = new Properties();
        this.ignoreUTypes = new ArrayList<String>();
        for (Map.Entry<String,String> me : config.entrySet())
        {
            if ( IGNORE.equals(me.getValue()))
                ignoreUTypes.add(me.getKey());
        }
    }
    
    /**
     * Get the configuration mapping.
     * 
     * @return map of utype -> FITS keyword
     */
    public Map<String,String> getConfig()
    {
        return config;
    }

    /**
     * Add a command line property to the mapping.
     * @param key Property key.
     * @param value Property value.
     */
    public void setArgumentProperty(String key, String value)
    {
        arguments.setProperty(key, value);
    }
    
    /**
     * 
     * For the given utype, returns the corresponding value found in the 
     * content files or the FITS headers, else null if the utype doesn't exist
     * in either the content files or FITS headers.
     * 
     * @param utype of the field. 
     * @return String value of the utype field.
     */
    public String getMapping(String utype)
    {        
        // Check the arguments for the utype.
        String property = arguments.getProperty(utype);
        if (property != null)
        {
            //og.debug("arguments: " + utype + " = " + property);
        	return property;
        }

        // Check the config for the value for the utype.
        String keywords = config.get(utype);
        if (keywords == null)
        {
            //log.debug("utype:keywords mapping not found for " + utype);
            return null;
        }
        //log.debug(utype + " = " + keywords);
        for (String ignorable : ignoreUTypes)
        {
            if ( utype.startsWith(ignorable))
            {
                log.debug("ignore: " + utype + " matches: " + ignorable);
                return null;
            }
        }
        
        // Value for this utype could be multi-valued with values comma delimited.
        String[] values = keywords.split(",");
        for (int i = 0; i < values.length; i++)
        {
            String symbolicKW = values[i].trim();
            if (symbolicKW.isEmpty())
                continue;
            
            // If the value is a FITS header keyword, update the keyword
            // with the axes number.
            String keyword = getKeyword(symbolicKW);
            if (keyword == null)
            {
                log.debug("Unable to decode " + symbolicKW);
                continue;
            }
            //log.debug("using: " + utype + " = " + keyword);
            
            // Check the content files and FITS file for the keyword value;
            String value = getKeywordValue(keyword);
            log.debug(utype + " -> " + keyword + " = " + value);
            if (value != null)
                return value;
        }

        // Return null if keyword value not found.
        return null;
    }
    
    public String getKeywordValue(String keyword)
    {
        String caomValue;

        // Check the override for the keyword value.
        if (override != null)
        {
            // Get the value from the override.
            caomValue = override.getValue(keyword, uri, extension);
            if (caomValue != null)
            {
                //log.debug("override: " + keyword + " = " + caomValue);
                return caomValue;
            }
        }

        // Check the fits header for the keyword value.
        if (header != null) // && header.containsKey(keyword))
        {
            HeaderCard card = header.findCard(keyword);
            if (card != null)
            {
                caomValue = card.getValue();
                if (caomValue != null)
                {
                    //log.debug("header: " + keyword + " = "+ caomValue + " in " + uri + "[" + extension + "]");
                    return caomValue;
                }
            }
            //else
            //    log.debug("header: no card for " + keyword + " in " + uri + "[" + extension + "]");
            
        }
        //else
        //    log.debug("header: is null for " + uri + "[" + extension + "]");

        // Check the fits primary for the keyword value.
        if (primary != null) // && primary.containsKey(keyword))
        {
            HeaderCard card = primary.findCard(keyword);
            if (card != null)
            {
                caomValue = card.getValue();
                if (caomValue != null)
                {
                    //log.debug("primary: " + keyword + " = "+ caomValue + " in " + uri);
                    return caomValue;
                }
            }
            //else
            //    log.debug("primary: no card for " + keyword + " in " + uri);
        }
        //else
        //    log.debug("primary: is null for " + uri + "[" + extension + "]");

        // Check the defaults for the keyword value.
        if (defaults != null)
        {
            caomValue = defaults.getValue(keyword, uri, extension);
            if (caomValue != null)
            {
                //log.debug("defaults: " + keyword + " = "+ caomValue);
                return caomValue;
            }
        }
        //else
        //    log.debug("defaults: is null for " + uri + "[" + extension + "]");
        
        //log.debug("value not found for " + keyword);
        return null;
    }
    
    protected String getKeyword(String key)
    {        
        boolean inBrackets = false;
        StringBuilder result = new StringBuilder();
        StringBuilder axis = new StringBuilder();
        char[] chars = key.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            char c = chars[i];
            if (c == '{')
            {
                inBrackets = true;
            }
            else if (c == '}')
            {
                inBrackets = false;
                String type = axis.toString();
                if (type.equals(POSITION_AXIS_1))
                {
                    if (positionAxis1 != null)
                        result.append(positionAxis1);
                    else
                        return null;
                }
                else if (type.equals(POSITION_AXIS_2))
                {
                    if (positionAxis2 != null)
                        result.append(positionAxis2);
                    else
                        return null;
                }
                else if (type.equals(ENERGY_AXIS))
                {
                    if (energyAxis != null)
                        result.append(energyAxis);
                    else
                        return null;
                }
                else if (type.equals(TIME_AXIS))
                {
                    if (timeAxis != null)
                        result.append(timeAxis);
                    else
                        return null;
                }
                else if (type.equals(POLARIZATION_AXIS))
                {
                    if (polarizationAxis != null)
                        result.append(polarizationAxis);
                    else
                        return null;
                }
                else
                {
                    throw new IllegalArgumentException("Unknown WCS axis " + type + " in keyword " + key);
                }
                axis.setLength(0);
            }
            else
            {
                if (inBrackets)
                    axis.append(c);
                else
                    result.append(c);
            }
        }
        String ret = null;
        if (inBrackets)
        {
            log.warn("found non-terminated symbolic reference ({ without }) in: " + key);
            return null;
        }
        
        if (result.length() > 0)
            ret = result.toString();
        
        log.debug("getKeyword: key=" + key + " result="+ret);
        return ret;
    }

}
