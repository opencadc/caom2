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
package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.FitsValuesMap;
import ca.nrc.cadc.caom2.fits.exceptions.IngestException;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author jburke
 */
public abstract class Util
{
    private static final Logger log = Logger.getLogger(Util.class);
    
    public final static Level DEFAULT_CONSOLE_LOGGING_LEVEL = Level.WARN;
    public final static Level DEFAULT_FILE_LOGGING_LEVEL = Level.INFO;

    static String strip(String s)
    {
        if (s == null)
            return null;
        s = s.trim();
        if (s.isEmpty() || s.indexOf('<') >= 0 || s.indexOf('>') >= 0)
            return null;
        return s;
    }

     /**
     * Load and merge default and user configuration.
     *
     * @return
     * @throws IngestException
     */
    public static Map<String,String> loadConfig(String userConfigFile)
        throws IOException
    {
        // load default config
        URL url = Ingest.class.getClassLoader().getResource("fits2caom2.config");
        Map<String,String> config = loadConfig(url.openStream());

        // load user config
        Map<String,String> userConfig;
        if (userConfigFile != null)
            userConfig = loadConfig(new FileInputStream(new File(userConfigFile)));
        else
            userConfig = new HashMap<String,String>();

        // merge
        Iterator<Map.Entry<String,String>> iter = userConfig.entrySet().iterator();
        while ( iter.hasNext() )
        {
            Map.Entry<String,String> me = iter.next();
            String utype = me.getKey();
            String keyword = strip(me.getValue());

            if (keyword != null)
            {
                String dkw = config.get(utype);
                config.put(utype, keyword); // replace
                if (dkw == null)
                    log.debug("add: " + utype + " = " + keyword);
                else
                    log.debug("replace: " + utype + " = " + keyword);
            }
        }

        return config;
    }

    static Map<String,String> loadConfig(InputStream istream)
        throws IOException
    {
        Map<String,String> ret = new LinkedHashMap<String, String>();
        BufferedReader r = new BufferedReader(new InputStreamReader(istream));
        String line = r.readLine();
        while (line != null)
        {
            line = line.trim();
            if ( !line.isEmpty() && !line.startsWith("#") && !line.startsWith("//"))
            {
                String[] kv = line.split("[ =\t#]+");
                if (kv != null && kv.length > 1)
                {
                    String utype = kv[0];
                    String keyword = kv[1];
                    ret.put(utype,keyword);
                }
            }
            line = r.readLine();
        }
        return ret;
    }

    public static FitsMapping getFitsMapping(Map<String,String> config, String defaultFile, String overrideFile)
    {
        StringBuilder errors = new StringBuilder();

        // Read the default file.
        FitsValuesMap defaults = null;
        if (defaultFile != null)
        {
            File file = new File(defaultFile);
            try
            {
                defaults = new FitsValuesMap(new FileReader(file), "default");
            }
            catch (Exception e)
            {
                errors.append("Unable to access default file ");
                errors.append(file.getPath());
                errors.append(", cause: ");
                errors.append(e.toString());
            }
            if (defaults != null && defaults.hasErrors())
            {
                errors.append(defaults.getErrors());
            }
        }

        // Read the override file.
        FitsValuesMap override = null;
        if (overrideFile != null)
        {
            File file = new File(overrideFile);
            try
            {
                override = new FitsValuesMap(new FileReader(file), "override");
            }
            catch (Exception e)
            {
                errors.append("Unable to access override file ");
                errors.append(file.getPath());
                errors.append(", cause: ");
                errors.append(e.toString());
            }
            if (override != null && override.hasErrors())
            {
                errors.append(override.getErrors());
            }
        }

        // Check if any errors found in the mapping files.
        if (errors.length() > 0)
        {
            // Write the errors to log and throw exception.
            log.error(errors.toString());
            throw new IllegalArgumentException(errors.toString());
        }

        // Create the mapping.
        return new FitsMapping(config, defaults, override);
    }


    /**
     * Check if an int array contains a specified value.
     * 
     * @param array int[] to search.
     * @param value value to search for.
     * @return true if the array contains the value, false otherwise.
     */
    public static boolean arrayContains(int[] array, int value)
    {
        if (array == null)
            return true;
        for (int i = 0; i < array.length; i++)
        {
            if (array[i] == value)
                return true;
        }
        return false;
    }
    
    // From a comma-delimitd String, create an array of URI objects.
    /**
     * 
     * @param argument
     * @return
     * @throws URISyntaxException 
     */
    public static URI[] argumentUriToArray(String argument)
        throws URISyntaxException, IllegalArgumentException
    {
        if (argument == null || argument.isEmpty())
            return null;
        String[] values = argument.split(",");
        URI[] array = new URI[values.length];
        for (int i = 0; i < values.length; i++)
        {
            String value = values[i].trim();
            URI uri = new URI(value);
            if (uri.getScheme() == null)
                throw new IllegalArgumentException("Missing scheme in uri " + value);
            array[i] = uri;
        }
        return array;
    }

    public static File[] argumentFileToArray(String argument)
    {
        if (argument == null || argument.isEmpty())
            return null;
        String[] value = argument.split(",");
        File[] array = new File[value.length];
        for (int i = 0; i < value.length; i++)
        {
            array[i] = new File(value[i]);
        }
        return array;
    }

    public static UriLocal argumentUriToUriLocal(String argument)
            throws URISyntaxException, FileNotFoundException
    {
        UriLocal uriLocal = new UriLocal();
        if (argument == null || argument.isEmpty())
            return uriLocal;

        if (argument.startsWith("@"))
        {
            String filename = argument.substring(1);
            File file = new File(filename);
            if (!file.exists())
            {
                throw new FileNotFoundException(filename + " not found");
            }

            List<URI> uris = new ArrayList<URI>();
            List<File> locals = new ArrayList<File>();
            BufferedReader br = null;
            try
            {
                String line;
                br = new BufferedReader(new FileReader(file));
                while ((line = br.readLine()) != null)
                {
                    if (line.startsWith("#"))
                        continue;
                    String[] tokens = line.trim().split("\\s+");
                    if (tokens.length >= 1)
                    {
                        URI uri = new URI(tokens[0].trim());
                        if (uri.getScheme() == null)
                            throw new IllegalArgumentException("Missing scheme in uri " + tokens[0]);
                        uris.add(uri);
                    }
                    if (tokens.length >= 2)
                    {
                        locals.add(new File(tokens[1].trim()));
                    }
                }

                if (!uris.isEmpty() && !locals.isEmpty() &&
                    (uris.size() != locals.size()))
                {
                    throw new IllegalArgumentException("number of uri and local arguments in " +
                            filename + " do not match");
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error reading from " + filename +
                                           " because " + e.getMessage());
            }
            finally
            {
                if (br != null)
                {
                    try
                    {
                        br.close();
                    }
                    catch(Throwable t) {}
                }
            }
            if (uris.size() > 0)
                uriLocal.uri = uris.toArray(new URI[uris.size()]);
            if (locals.size() > 0)
                uriLocal.local = locals.toArray(new File[locals.size()]);
        }
        else
        {
            uriLocal.uri = argumentUriToArray(argument);
        }
        return uriLocal;
    }
    
    /**
     * Configures logging to either the console or a file, but not both.
     * 
     * If the ArgumentMap contains a key name 'log', a file logger named with the
     * value of 'log' is created. If the value of 'log' is null or an empty string,
     * a file logger will not be created. If the ArgumentMap doesn't contain
     * a 'log' key, a console logger is created.
     *
     * The log level is set using log level arguments in the ArgumentMap. If the
     * ArgumentMap doesn't contain log level arguments, the default log level
     * for the console logger is WARN, and the default log level for
     * the file logger is INFO.
     *
     * @param loggerNames names of the loggers to apply the logging options of
     * in the argsMap to.
     * @param argMap command line argument options.
     * @throws IOException problems with the log file
     * @throws IllegalArgumentException if there is a disagreement with the way 
     *                                  the file is handled.              
     */
    public static void initialize(String[] loggerNames, ArgumentMap argMap)
        throws IOException, IllegalArgumentException
    {
        if (argMap.isSet(Argument.LOG))
        {
            String filename = argMap.getValue(Argument.LOG);
            if (filename.isEmpty())
            {
                filename = "empty or zero-length string";
		throw new IllegalArgumentException("Illegal log file name option: " + filename);
            }
            initFileLogging(loggerNames, argMap, filename);
        }
        else
        {
            initConsoleLogging(loggerNames, argMap);
        }
    }

    /**
    * Initializes logging to a file.
    * @param loggerNames the names of the loggers (usually names of packages
    * or classes or ancestors of packages or classes). Can't be null.
    * @param argMap command line arguments.
    */
    public static synchronized void initFileLogging(String[] loggerNames, ArgumentMap argMap, String filename)
        throws IOException
    {
        Level level = DEFAULT_FILE_LOGGING_LEVEL;
        if (argMap.isSet(Argument.LOG_QUIET_SHORT) || argMap.isSet(Argument.LOG_QUIET)) level = Level.ERROR;
        if (argMap.isSet(Argument.LOG_VERBOSE_SHORT) || argMap.isSet(Argument.LOG_VERBOSE)) level = Level.INFO;
        if (argMap.isSet(Argument.LOG_DEBUG_SHORT) || argMap.isSet(Argument.DEBUG)) level = Level.DEBUG;

        FileAppender fileAppender = new FileAppender(new PatternLayout(), filename);

        boolean append = true;
        Writer fileWriter = new FileWriter(filename, append);
        for (String loggerName : loggerNames)
            Log4jInit.setLevel(loggerName, level, fileWriter);
    }
    
    /**
    * Initializes logging to the console.
    * @param loggerNames the names of the loggers (usually names of packages 
    * or classes or ancestors of packages or classes). Can't be null.
    * @param argMap command line arguments.
    */
    private static synchronized Level initConsoleLogging(String[] loggerNames, ArgumentMap argMap)
    {
        Level level = DEFAULT_CONSOLE_LOGGING_LEVEL;
        if (argMap.isSet(Argument.LOG_QUIET_SHORT) || argMap.isSet(Argument.LOG_QUIET)) level = Level.ERROR;
        if (argMap.isSet(Argument.LOG_VERBOSE_SHORT) || argMap.isSet(Argument.LOG_VERBOSE)) level = Level.INFO;
        if (argMap.isSet(Argument.LOG_DEBUG_SHORT) || argMap.isSet(Argument.DEBUG)) level = Level.DEBUG;

        for (int i = 0; i < loggerNames.length; i++)
            Log4jInit.setLevel(loggerNames[i], level);

        return level;
    }

    public static class UriLocal
    {
        public URI[] uri = null;
        public File[] local = null;

        UriLocal() {}
    }

}
