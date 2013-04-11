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

import java.io.BufferedReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Reads a fits2caom2 configuration file into a Map. If the configuration
 * file contains fileID and extension sections, those sections are read
 * into new Maps, and these Maps are added to the base Map using the section
 * and extension name as the key.
 * 
 * @author jburke
 */
public class FitsValuesMap
{
    private static Logger log = Logger.getLogger(FitsValuesMap.class);
    
    // Indicates a comment line.
    private static final String[] COMMENTS = { "#" };
    
    // Key value pair delimiters.
    private static final String[] KEY_VALUE_DELIMTERS = { "=", ":" };
    
    // End of line string.
    private static final String EOL = "\r\n";
    
    // Start and end of an extension in a section line.
    private static final String EXTENSION_END = "]";
    private static final String EXTENSION_START = "#[";
    
    // Indicates a section line.
    private static final String SECTION_DELIMITER = "?";
    
    // Input reader.
    private Reader reader;
    
    // Name of the config file.
    private String name;
    
    // Contains errors found in processing configuration file.
    private StringBuffer errors;
    
    // Toggle whether to return just the value, or the value plus comment.
    private boolean returnComments;
    
    // Map containing configuration key value pairs.
    private Map<String, Object> map;

    /**
     * Default constructor. Reads the configuration file and attempts
     * to parse the file into a Map.
     * 
     * @param filename Absolute path to the configuration file.
     * @throws ca.nrc.cadc.fits2caom.exceptions.IngestException If the configuration file can not be read,
     *         or if there was an error processing the configuration file.
     */
    public FitsValuesMap(Reader reader, String name)
    {
        this.reader = reader;
        this.name = name;
        
        // Initialize the Map and StringBuffer for errors.
        this.map = new HashMap<String, Object>();
        this.errors = new StringBuffer();
        this.returnComments = false;
                
        // Check if the filename is null. A null filename is a valid argument
        // and results in a ConfigMap with an empty map.
        if (reader == null)
            return;
        
        // Parse the file into the Map.
        parse();
    }
    
    /**
     * Get the name of the file behind the ConfigMap.
     * 
     * @return name of the ConfigMap file. 
     */
    public String getName()
    {
        return name;
    }
    
    /**
     * Get the map backing this ConfigMap.
     * 
     * @return Map backing the ConfigMap.
     */
    public Map getMap()
    {
        return this.map;
    }
    
    /**
     * Add a key and value to the map.
     * 
     * @param key map key.
     * @param value map value.
     */
    public void setKeywordValue(String key, String value)
    {
        map.put(key, new Keyword(value));
    }
    
    /**
     * If true, the values returned from the Map will include comments
     * If false the value returned from the Map will not include any comments 
     * after the value. The default behavior is not to return comments.
     * 
     * @param b if true returns the value and comments, if false returns the value.
     */
    public void setReturnComments(boolean b)
    {
        this.returnComments = b;
    }
    
    /**
     * Returns a Set view of the keys contained in this map.
     *  
     * @return Set of Strings containing the key names.
     */
    public Set<String> getKeySet()
    {
        return map.keySet();
    }
    
    /**
     * Checks if the parent Map contains a mapping for the specified key.
     * Only the parent Map will be searched for the key. Any child Maps
     * containing sections will not be searched for the key.
     * 
     * @deprecated use getValue and check for null.
     * @param key Key word.
     * @return True if the parent Map contains the key, false otherwise.
     */
    public boolean containsKey(String key)
    {
        return containsKey(key, null);
    }
    
    /**
     * Checks if the parent Map, or a child Map for the specified URI,
     * contains a mapping for the specified key.
     * 
     * @deprecated use getValue and check for null.
     * @param key Key word.
     * @param uri String representation of the Section URI.
     * @return True if the parent Map, or child Map for the fileID, contains
     *         the key, false otherwise.
     */
    public boolean containsKey(String key, String uri)
    {
        return containsKey(key, uri, null);
    }
    
    /**
     * Checks if the parent Map, or a child Map for the specified URI 
     * and extension, contains a mapping for the specified key.
     * 
     * First the URI and extension are combined to create a section name.
     * If a child Map exists with the section name, it is searched for the key.
     * If the key is not found, look for a child Map using the URI as the
     * section name. If a child Map exists for the URI, search it for the key.
     * If the key is not found, finally search the parent Map for the key.
     * 
     * @deprecated use getValue and check for null.
     * @param key Key word.
     * @param uri String representation of the Section URI.
     * @param extension extension for the section.
     * @return True if the parent Map, or child Map for the section and extension,
     *         contains the key, false otherwise. 
     */
    public boolean containsKey(String key, String uri, Object extension)
    {    	
        boolean found = false;
        
        // Look for a section using the fileID and extension,
        // and if it exists check if it contains the key.
        if (uri != null && extension != null)
        {
            Map sectionMap = (Map) map.get(SECTION_DELIMITER + uri + 
                                           EXTENSION_START +  
                                           extension + EXTENSION_END);
            if (sectionMap != null)
                found = sectionMap.containsKey(key);
        }
        
        // Look for a section using the uri, and if it exists
        // check if it contains the key.
        if (!found && uri != null)
        {
            Map sectionMap = (Map) map.get(SECTION_DELIMITER + uri);
            if (sectionMap != null)
                found = sectionMap.containsKey(key);
        }
        
        // Check the parent Map for the key.
        if (!found)
            found = map.containsKey(key);
        
        return found;
    }
    
    /**
     * Return any errors created while parsing the configuration file.
     * Can return an empty String if no errors were encountered.
     * 
     * @return String of error messages.
     */
    public String getErrors()
    {
        return errors.toString();
    }
    
    /**
     * Get the value for the specified key. Backed by a HashMap, so follows 
     * the usual HashMap semantics of returning null if the value is not
     * found, and returning null if a null key is specified. 
     * 
     * Only checks the parent Map for the value, not any child Maps.
     * 
     * @param key Key word.
     * @return String value or null if not found.
     */
    public String getValue(String key)
    {
        return getValue(key, null);
    }
    
    /**
     * Get the value for the specified key. Backed by a HashMap, so follows 
     * the usual HashMap semantics of returning null if the value is not
     * found, and returning null if a null key is specified.
     * 
     * Checks if a child Map exists for the URI, and if it does looks
     * for the value in the child Map first. If the value is not found in
     * the child Map, then look in the parent Map for the value.
     *
     * @param key Key word.
     * @param uri String representation of the Section URI.
     * @return String value or null if not found.
     */
    public String getValue(String key, String uri)
    {
        return getValue(key, uri, null);
    }
    
    /**
     * Get the value for the specified key. Backed by a HashMap, so follows 
     * the usual HashMap semantics of returning null if the value is not
     * found, and returning null if a null key is specified.
     * 
     * Checks if a child Map exists for the URI and extension, and if 
     * it does looks for the value in the child Map. If the value is not 
     * found in the child Map, then check if a child Map exists for the 
     * URI, and if it does looks for the value in the child Map. 
     * If the value is not found in the child Map, then look in the 
     * parent Map for the value.
     *
     * @param key Key word.
     * @param uri String representation of the Section URI.
     * @param extension extension for the section.
     * @return String value or null if not found.
     */
    public String getValue(String key, String uri, Object extension)
    {    	
        // First check if a child Map exists using the uri and extension,
        // and if it does get the value for the key.
        if (uri != null && extension != null)
        {
        	String mapKey = SECTION_DELIMITER + uri + EXTENSION_START + 
                            extension + EXTENSION_END;
            Map sectionMap = (Map) map.get(mapKey);
            if (sectionMap != null)
            {
            	Keyword keyword = (Keyword) sectionMap.get(key);
                //log.debug("found: " + name + " for " + mapKey + ", " + key + " = " + keyword);
            	if (keyword != null)
            	{
                    String value;
                    if (returnComments)
                        value = keyword.keyword;
                    else
                        value = keyword.value;
            		log.debug(name + "[extension]: " + mapKey + "." + key + " = " + value);
                    return value;
                }
            }
            //else
            //    log.debug("not found: " + name + " for " + mapKey);
        }
        
        // Next check if child Map exists using just the uri,
        // and if it does get the value for the key.
        if (uri != null)
        {
        	String mapKey = SECTION_DELIMITER + uri;
            Map sectionMap = (Map) map.get(mapKey);
            if (sectionMap != null)
            {
            	Keyword keyword = (Keyword) sectionMap.get(key);
                //log.debug("found: " + name + " for " + mapKey + ", " + key + " = " + keyword);
            	if (keyword != null)
            	{
                    String value;
                    if (returnComments)
                        value = keyword.keyword;
                    else
                        value = keyword.value;
            		log.debug(name + "[uri]: " + mapKey + "." + key + " = " + value);
                    return value;
            	}
            }
            //else
            //    log.debug("not found: " + name + " for " + mapKey);
        }
                
        // Lastly get the value from the parent Map.
        Keyword keyword = (Keyword) map.get(key);
        //log.debug("found: " + name + " in parent map, " + key + " = " + keyword);
        if (keyword != null)
        {
            String value;
            if (returnComments)
                value = keyword.keyword;
            else
                value = keyword.value;
        	log.debug(name + ": " + key + " = " + value);
            return value;
        }
        
        return null;
    }
    
    /**
     * Check if any errors where created during the parsing of the 
     * configuration file.
     * 
     * @return True if any errors where created during the configuration 
     *         file parsing, false otherwise.
     */
    public boolean hasErrors()
    {
        return errors.length() > 0;
    }
        
    // Parse the file into the Map.
    private void parse()
    {
        log.debug("parsing " + name);
        try
        {            
            // Current line number.
            int lineNumber = 0;
            
            // Current section name.
            String section = null;
            
            // Read in the file.
            BufferedReader br = new BufferedReader(reader);
            String line = null;
            while ((line = br.readLine()) != null)
            {
                // Increment the line number.
                lineNumber++;
                
                // Cleanup the line.
                line = line.trim();
                
                // If the line is empty or a comment line skip it.
                if (line.length() == 0 || isCommentLine(line))
                    continue;
                
                // Check if the line starts a new section.
                if (line.startsWith(SECTION_DELIMITER))
                {
                    // Sanity check the section line.
                    section = validateSectionName(line, lineNumber);
                }
                else
                {
                    // Parse the key value pair to add to the current map.
                    updateMap(section, line, lineNumber);
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error processing file " + name, e);
        }
        log.debug("finished parsing file " + name);
    }
    
    // Check if the line is comment line where the first character is a comment delimiter.
    private boolean isCommentLine(String line)
    {
        for (int i = 0; i < COMMENTS.length; i++)
        {
            if (line.startsWith(COMMENTS[i]))
                return true;
        }
        return false;
    }
    
    // Verify that the section name is properly formed.
    private String validateSectionName(String line, int lineNumber)
    {
        log.debug("validateSectionName: " + line);
        
        // Get the line minus the section delimiter.
        String section = line.substring(1);
        
        // Nothing past the delimiter, invalid syntax.
        if (section.length() == 0)
        {
            errors.append(lineNumber);
            errors.append(" - ");
            errors.append(" invalid section syntax, no fileID after delimiter in ");
            errors.append(line);
            errors.append(EOL);
            log.debug("validateSectionName, no fileID after delimiter, return null");
            return null;
        }

        // Look for the start of an extension number.
        int extStart = section.indexOf(EXTENSION_START);
        
        // Check for end of extension delimiter.
        int extEnd = section.indexOf(EXTENSION_END);
        
        // No extension delimiters found.
        if (extStart == -1 && extEnd == -1)
        {
            log.debug("validateSectionName returning " + line);
            return line;
        }

        // If the start delimter is found without the end delimiter.
        if (extStart != -1 && extEnd == -1)
        {
            errors.append(lineNumber);
            errors.append(" - ");
            errors.append(EXTENSION_START);
            errors.append(" without matching ");
            errors.append(EXTENSION_END);
            errors.append(" in ");
            errors.append(line);
            errors.append(EOL);
            log.debug("validateSectionName, invalid syntax, returning null");
            return null;
        }
        
        // If the end delimiter is found without the start delimter.
        if (extStart == -1 && extEnd != -1)
        {
            errors.append(lineNumber);
            errors.append(" - ");
            errors.append(EXTENSION_END);
            errors.append(" without matching ");
            errors.append(EXTENSION_START);
            errors.append(" in ");
            errors.append(line);
            errors.append(EOL);
            log.debug("validateSectionName, invalid syntax, returning null");
            return null;
        }
        
        // If the start delmiter index is greater than end delimiter index.
        if (extStart > extEnd)
        {
            errors.append(lineNumber);
            errors.append(" - ");            
            errors.append(EXTENSION_START);
            errors.append(" is after  ");
            errors.append(EXTENSION_END);
            errors.append(" in ");
            errors.append(line);
            errors.append(EOL);
            log.debug("validateSectionName, invalid syntax, returning null");
            return null;
        }
        
        // Extension delimters found, parse the extension number.
        // If the extension is an empty string.
        if (section.substring(extStart + 1, extEnd).trim().length() == 0)
        {
            errors.append(lineNumber);       
            errors.append(" - extension is empty in ");
            errors.append(line);
            errors.append(EOL);
            log.debug("validateSectionName, empty extension, returning null");
            return null;
        }

        log.debug("validateSectionName returning " + line);
        return line;
    }
        
    // Parse the line into a key value pair and add to the base map, 
    // or if a section name is specified, add the key value pair to the
    // map for the section.
    private void updateMap(String section, String line, int lineNumber)
    {
        
        // Parse line into key value pair,
        // look for the key value delimiter.
        int index = -1;
        for (int i = 0; i < KEY_VALUE_DELIMTERS.length; i++)
        {
            index = line.indexOf(KEY_VALUE_DELIMTERS[i]);
            if (index > 0) break;
        }
        
        // Delimiter not found.
        if (index == -1)
        {
            errors.append(name);
            errors.append("[");
            errors.append(lineNumber);          
            errors.append("] key value delimiter not found in ");
            errors.append(line);
            errors.append(EOL);
            return;
        }

        // Parse the key and value.
        String key = line.substring(0, index).trim();
        String value = line.substring(index + 1).trim();
        
        // Check if key is empty.
        if (key.length() == 0)
        {
            errors.append(name);
            errors.append("[");            
            errors.append(lineNumber);
            errors.append("] empty key in ");
            errors.append(line);
            errors.append(EOL);
            return;
        }
        
        // Get a Keyword of the value and optional comment.
        Keyword keyword = new Keyword(value);
                
        // No section, add to parent Map.
        if (section == null)
        {
            map.put(key, keyword);
            log.debug("put to map " + key + " = " + value);
        }
        else
        {
            // Get the section Map from the parent Map.
            Map<String, Object> sectionMap = (Map<String, Object>) map.get(section);
            if (sectionMap == null)
            {
                sectionMap = new HashMap<String, Object>();
                map.put(section, sectionMap);
                log.debug("put to map section " + section);
            }
            sectionMap.put(key, keyword);
            log.debug("put to map [" + section + "] " + key + " = " + value);
        }        
    }
    
    class Keyword
    {
        String keyword;
        String value;

        public String toString()
        {
            return "KeyWord[" + value +  "]";
        }
        
        /**
         * Keywords can have an optional comment after the value, starting with
         * either a # or //.
         * 
         * @param keyword value of the Map key.
         */
        Keyword(String s)
        {
            if (s == null || s.trim().isEmpty())
                return;
            keyword = s.trim();
            
            
            // Look for a comment within the keyword.
            int index = -1;
            for (int i = 0; i < COMMENTS.length; i++)
            {
                index = this.keyword.indexOf(COMMENTS[i]);
                if (index > 0) break;
            }
            
            // No comment found.
            if (index == -1)
            {
                // Check if value quoted with single quotes.
                if (keyword.length() > 1 && keyword.charAt(0) == '\'' && keyword.charAt(keyword.length() - 1) == '\'')
                {
                    int start = keyword.indexOf("'");
                    int end = keyword.indexOf("'", start + 1);
                    if (start < end)
                        value = keyword.substring(start + 1, end).trim();
                }
                else
                {
                    value = keyword;
                }
            }
            
            // Comment found, parse the value.
            else
            {
                String tmp = keyword.substring(0, index).trim();
                if (tmp.isEmpty())
                {
                    value = null;
                }
                else
                {   
                    // Check if value quoted with single quotes.
                    if (tmp.length() > 1 && tmp.charAt(0) == '\'' && tmp.charAt(tmp.length() - 1) == '\'')
                    {
                        index = tmp.indexOf("'");
                        int end = tmp.indexOf("'", index + 1);
                        if (index < end)
                            value = tmp.substring(index + 1, end).trim();
                    }
                    else
                    {
                        value = tmp;
                    }
                }
            }
        }
        
    }
    
}
