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

package ca.nrc.cadc.caom2.repo;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CaomRepoConfig
{
    private static final Logger log = Logger.getLogger(CaomRepoConfig.class);

    private static final String CONFIG_FILE = CaomRepoConfig.class.getSimpleName() + ".properties";

    private List<CaomRepoConfig.Item> config;

    public CaomRepoConfig()
        throws IOException
    {
        this.config = loadConfig(new File(System.getProperty("user.home") + "/config", CONFIG_FILE));
    }

    public Item getConfig(String collection)
    {
        Iterator<Item> i = config.iterator();
        while ( i.hasNext() )
        {
            Item item = i.next();
            if (item.collection.equals(collection))
                return item;
        }
        return null;
    }

    public boolean isEmpty() { return config.isEmpty(); }

    public Iterator<Item> iterator()
    {
        return config.iterator();
    }

    public static class Item
    {
        private String collection;
        private String dataSourceName;
        private String database;
        private String schema;
        private String obsTableName;
        private URI readOnlyGroup;
        private URI readWriteGroup;

        Item(String collection, String dataSourceName, String database, String schema, String obsTableName,
            URI readOnlyGroup, URI readWriteGroup)
        {
            this.collection = collection;
            this.dataSourceName = dataSourceName;
            this.database = database;
            this.schema = schema;
            this.obsTableName = obsTableName;
            this.readOnlyGroup = readOnlyGroup;
            this.readWriteGroup = readWriteGroup;
        }

        @Override
        public String toString()
        {
            return "RepoConfig.Item[" + collection + "," + dataSourceName + "," + database + "," + schema + "," + obsTableName + ","
                    + readOnlyGroup + "," + readWriteGroup + "]";
        }

        public String getTestTable()
        {
            return database + "." + schema + "." + obsTableName;
        }

        public String getCollection()
        {
            return collection;
        }

        public String getDataSourceName()
        {
            return dataSourceName;
        }

        public String getDatabase()
        {
            return database;
        }

        public URI getReadOnlyGroup()
        {
            return readOnlyGroup;
        }

        public URI getReadWriteGroup()
        {
            return readWriteGroup;
        }

        public String getSchema()
        {
            return schema;
        }

        private Item() { }
    }

    static List<CaomRepoConfig.Item> loadConfig(File cf)
        throws IOException
    {
        long start = System.currentTimeMillis();
        List<CaomRepoConfig.Item> ret = new ArrayList<CaomRepoConfig.Item>();
        
        // TODO: this is quick and dirty but is very fragile: requires single space between tokens and
        // doesn't handle blanks, comments, etc.
        Properties props = new Properties();
        props.load(new FileReader(cf));
        
        Iterator<String> iter = props.stringPropertyNames().iterator();
        while ( iter.hasNext() )
        {
            String collection = iter.next();
            try
            {
                Item rci = getItem(collection, props);
                ret.add(rci);
            }
            catch(Exception ex)
            {
                log.error("CaomRepoConfig " + cf.getAbsolutePath() + ", invalid config for " + collection + ": " + ex);
            }
        }
        long dur = System.currentTimeMillis() - start;
        log.debug("load time: " + dur+  "ms");
        return ret;
    }

    static CaomRepoConfig.Item getItem(String collection, Properties props)
        throws IllegalArgumentException, URISyntaxException
    {
        String val = props.getProperty(collection);
        log.debug(collection + " = " + val);
        String[] parts = val.split("[ \t]+"); // one or more spaces and tabs
        if (parts.length == 6)
        {
            String dsName=  parts[0];
            String database = parts[1];
            String schema = parts[2];
            String obsTable = parts[3];
            String roGroup = parts[4];
            String rwGroup = parts[5];

            // validation
            URI ro = new URI(roGroup);
            URI rw = new URI(rwGroup);
            if (!"ivo".equals(ro.getScheme()))
                throw new IllegalArgumentException("invalid GMS URI " + ro + ", expected ivo scheme");
            if ( ro.getFragment() == null || ro.getFragment().length() == 0)
                throw new IllegalArgumentException("invalid GMS URI " + ro + ", expected group name in fragment");
            if (!"ivo".equals(rw.getScheme()))
                throw new IllegalArgumentException("invalid GMS URI " + rw + ", expected ivo scheme");
            if ( rw.getFragment() == null || rw.getFragment().length() == 0)
                throw new IllegalArgumentException("invalid GMS URI " + rw + ", expected group name in fragment");

            // create
            CaomRepoConfig.Item rci = new CaomRepoConfig.Item(collection, dsName, database, schema, obsTable, ro, rw);
            return rci;
        }
        else
        {
            throw new IllegalArgumentException("found " + parts.length + " tokens, expected 6");
        }
    }

}
