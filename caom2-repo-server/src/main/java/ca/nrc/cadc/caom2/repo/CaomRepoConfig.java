/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2016.                            (c) 2016.
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

import ca.nrc.cadc.caom2.ac.ReadAccessGenerator;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;

/**
 *
 * @author pdowler
 */
public class CaomRepoConfig {
    private static final Logger log = Logger.getLogger(CaomRepoConfig.class);

    private List<CaomRepoConfig.Item> config;

    public CaomRepoConfig(File config) throws IOException {
        this.config = loadConfig(config);
    }

    public Map<String, Object> getDAOConfig(String collection) throws IOException {
        CaomRepoConfig.Item i = getConfig(collection);
        if (i != null) {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put("jndiDataSourceName", i.getDataSourceName());
            props.put("database", i.getDatabase());
            props.put("schema", i.getSchema());
            props.put("basePublisherID", i.getBasePublisherID().toASCIIString());
            props.put(SQLGenerator.class.getName(), i.getSqlGenerator());
            return props;
        }
        throw new IllegalArgumentException("unknown collection: " + collection);
    }
    
    public Item getConfig(String collection) {
        return getConfig(collection, true);
    }
    
    public Item getConfig(String collection, boolean doInit) {
        Iterator<Item> i = config.iterator();
        while (i.hasNext()) {
            Item item = i.next();
            if (item.collection.equals(collection)) {
                if (doInit) {
                    try {
                        initDB(item);
                    } catch (Exception ex) {
                        log.error("CAOM database INIT FAILED", ex);
                        return null;
                    }
                }
                return item;
            }
        }
        return null;
    }

    public boolean isEmpty() {
        return config.isEmpty();
    }

    /**
     * Get a config item iterator that initializes the database before returning items.
     * 
     * @return item iterator
     */
    public Iterator<Item> iterator() {
        return new Initerator(config.iterator());
    }

    // need this so availability test loop does init
    private class Initerator implements Iterator<Item> {
        private Iterator<Item> iter;

        public Initerator(Iterator<Item> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Item next() {
            Item ret = iter.next();
            initDB(ret);
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private void initDB(CaomRepoConfig.Item i) {
        if (PostgreSQLGenerator.class.equals(i.getSqlGenerator())) {
            try {
                DataSource ds = DBUtil.findJNDIDataSource(i.getDataSourceName());
                InitDatabase init = new InitDatabase(ds, i.getDatabase(), i.getSchema());
                init.doInit();
            } catch (NamingException ex) {
                throw new RuntimeException("CONFIG: failed to connect to database", ex);
            }
        }
    }

    public Iterator<String> collectionIterator() {
        return new CollectionIterator(config.iterator());
    }

    private class CollectionIterator implements Iterator<String> {
        private Iterator<Item> iter;

        public CollectionIterator(Iterator<Item> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public String next() {
            Item ret = iter.next();
            return ret.collection;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static class Item {
        private Class sqlGenerator;
        private String collection;
        private String dataSourceName;
        private String database;
        private String schema;
        private String obsTableName;
        private Boolean publicRead;
        private GroupURI readOnlyGroup;
        private GroupURI readWriteGroup;

        private URI basePublisherID;
        private boolean computeMetadata;
        private boolean proposalGroup;
        private GroupURI operatorGroup;
        private GroupURI staffGroup;
        
        // CADC-specific temporary hack
        private String artifactPattern;

        Item(Class sqlGenerator, String collection, String dataSourceName, String database,
                String schema, String obsTableName, GroupURI readOnlyGroup,
                GroupURI readWriteGroup) {
            this.sqlGenerator = sqlGenerator;
            this.collection = collection;
            this.dataSourceName = dataSourceName;
            this.database = database;
            this.schema = schema;
            this.obsTableName = obsTableName;
            this.readOnlyGroup = readOnlyGroup;
            this.readWriteGroup = readWriteGroup;
        }

        @Override
        public String toString() {
            return "RepoConfig.Item[" + collection + "," + dataSourceName + "," + database + ","
                    + schema + "," + obsTableName + "," 
                    + publicRead + "," + readOnlyGroup + "," + readWriteGroup + ","
                    + sqlGenerator.getSimpleName() + "," 
                    + basePublisherID + ","
                    + computeMetadata + ","
                    + proposalGroup + "," + operatorGroup + "," + staffGroup + "]";
        }

        public URI getBasePublisherID() {
            return basePublisherID;
        }
        
        public Class getSqlGenerator() {
            return sqlGenerator;
        }

        public boolean getComputeMetadata() {
            return computeMetadata;
        }

        public boolean getProposalGroup() {
            return proposalGroup;
        }

        public GroupURI getOperatorGroup() {
            return operatorGroup;
        }

        public GroupURI getStaffGroup() {
            return staffGroup;
        }

        public String getTestTable() {
            return database + "." + schema + "." + obsTableName;
        }

        public String getCollection() {
            return collection;
        }

        public String getDataSourceName() {
            return dataSourceName;
        }

        public String getDatabase() {
            return database;
        }

        public boolean getPublicRead() {
            return publicRead; // unbox
        }
        
        public GroupURI getReadOnlyGroup() {
            return readOnlyGroup;
        }

        public GroupURI getReadWriteGroup() {
            return readWriteGroup;
        }

        public String getSchema() {
            return schema;
        }

        public String getArtifactPattern() {
            return artifactPattern;
        }
        
        private Item() {
        }
    }

    static List<CaomRepoConfig.Item> loadConfig(File cf) throws IOException {
        long start = System.currentTimeMillis();
        List<CaomRepoConfig.Item> ret = new ArrayList<CaomRepoConfig.Item>();

        // TODO: this is quick and dirty but is very fragile: requires single space
        // between tokens
        // and
        // doesn't handle blanks, comments, etc.
        Properties props = new Properties();
        props.load(new FileReader(cf));

        Iterator<String> iter = props.stringPropertyNames().iterator();
        while (iter.hasNext()) {
            String collection = iter.next();
            try {
                Item rci = getItem(collection, props);
                ret.add(rci);
            } catch (Exception ex) {
                log.error("CaomRepoConfig " + cf.getAbsolutePath() + ", invalid config for "
                        + collection + ": " + ex);
            }
        }
        long dur = System.currentTimeMillis() - start;
        log.debug("load time: " + dur + "ms");
        return ret;
    }

    private static void validateProposalGroup(boolean proposalGroup, String staffGroup) {
        if (proposalGroup) {
            if (!StringUtil.hasText(staffGroup)) {
                throw new IllegalArgumentException("staff group is not specified for proposal group");
            }
        }
    }
    
    static CaomRepoConfig.Item getItem(String collection, Properties props)
            throws IllegalArgumentException, URISyntaxException {
        String val = props.getProperty(collection);
        log.debug(collection + " = " + val);
        String[] parts = val.split("[ \t]+"); // one or more spaces and tabs
        if (parts.length >= 7) { 
            String cname = parts[6];
            Class sqlGen = null;
            try {
                sqlGen = Class.forName(cname);
                if (!SQLGenerator.class.isAssignableFrom(sqlGen)) {
                    throw new IllegalArgumentException(
                            "invalid SQLGenerator class: does not implement interface "
                                    + SQLGenerator.class.getName());
                }
            } catch (ClassNotFoundException ex) {
                throw new IllegalArgumentException(
                        "failed to load SQLGenerator class: " + cname, ex);
            }

            // default values for backwards compatible to existing config
            boolean computeMetadata = false;
            boolean proposalGroup = false;
            String operatorGroup = null;
            String staffGroup = null;
            boolean publicRead = false;
            URI basePublisherID = null;
            String pattern = null;
            for (int i = 7; i < parts.length; i++) {
                String option = parts[i]; // key=value pair
                log.debug(collection + " options: " + option);
                String[] kv = option.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException("invalid key=value pair: " + option);
                }
                if ("publicRead".equals(kv[0])) {
                    publicRead = safeParseBoolean(kv[1]);
                } else if ("computeMetadata".equals(kv[0])) {
                    computeMetadata = safeParseBoolean(kv[1]);
                } else if (ReadAccessGenerator.PROPOSAL_GROUP_KEY.equals(kv[0])) {
                    proposalGroup = safeParseBoolean(kv[1]);
                } else if (ReadAccessGenerator.OPERATOR_GROUP_KEY.equals(kv[0])) {
                    operatorGroup = kv[1];
                } else if (ReadAccessGenerator.STAFF_GROUP_KEY.equals(kv[0])) {
                    staffGroup = kv[1];
                } else if ("basePublisherID".equals(kv[0])) {
                    basePublisherID = new URI(kv[1]);
                } else if ("artifactPattern".equals(kv[0])) {
                    pattern = kv[1];
                }
                // else: ignore
            }

            validateProposalGroup(proposalGroup, staffGroup);
            
            String dsName = parts[0];
            String database = parts[1];
            String schema = parts[2];
            String obsTable = parts[3];
            String roGroup = parts[4];
            String rwGroup = parts[5];
            
            GroupURI ro = new GroupURI(roGroup);
            GroupURI rw = new GroupURI(rwGroup);
            
            if (basePublisherID == null) {
                throw new IllegalArgumentException("missing required param: basePublisherID");
            }
            if (!"ivo".equals(basePublisherID.getScheme())
                    || basePublisherID.getAuthority() == null) {
                throw new IllegalArgumentException("invalid basePublisherID: " + basePublisherID + ", expected ivo://<authority>[/<path>]");
            }

            CaomRepoConfig.Item rci = new CaomRepoConfig.Item(sqlGen, collection, dsName, database,
                    schema, obsTable, ro, rw);
            rci.publicRead = publicRead;
            rci.basePublisherID = basePublisherID;
            rci.computeMetadata = computeMetadata;
            rci.operatorGroup = operatorGroup == null ? null : new GroupURI(operatorGroup);
            rci.staffGroup = staffGroup == null ? null : new GroupURI(staffGroup);
            rci.proposalGroup = proposalGroup;
            rci.artifactPattern = pattern;
            
            
            log.debug(collection + ": loaded " + rci);
            return rci;
        } else {
            throw new IllegalArgumentException("found " + parts.length + " tokens, expected 7");
        }
    }

    private static boolean safeParseBoolean(String val) {
        if (val.equalsIgnoreCase("true")) {
            return true;
        }
        if (val.equalsIgnoreCase("false")) {
            return false;
        }

        throw new IllegalArgumentException("invalid boolean value: " + val);
    }
}
