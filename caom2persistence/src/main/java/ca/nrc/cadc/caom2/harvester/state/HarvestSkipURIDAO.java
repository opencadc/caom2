/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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

package ca.nrc.cadc.caom2.harvester.state;

import ca.nrc.cadc.caom2.persistence.Util;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class HarvestSkipURIDAO {

    private static Logger log = Logger.getLogger(HarvestSkipURIDAO.class);

    private static final String[] COLUMNS
            = {
                "source", "cname", "bucket", "skipID", "tryAfter", "errorMessage", 
                "lastModified", "id"
            };

    private final String tableName;
    private final DataSource dataSource;
    private final JdbcTemplate jdbc;
    private final RowMapper extractor;

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);
    
    public static final int BUCKET_LENGTH = 3;

    public HarvestSkipURIDAO(DataSource dataSource, String database, String schema) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.dataSource = dataSource;
        if (database != null) {
            this.tableName = database + "." + schema + ".HarvestSkipURI";
        } else {
            this.tableName = schema + ".HarvestSkipURI";
        }
        this.extractor = new HarvestSkipMapper(Calendar.getInstance(DateUtil.UTC));
    }

    public HarvestSkipURI get(String source, String cname, URI skipID) {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname, skipID, null, null, null);
        List result = jdbc.query(sel, extractor);
        if (result.isEmpty()) {
            return null;
        }
        return (HarvestSkipURI) result.get(0);
    }

    public List<HarvestSkipURI> get(String source, String cname, Date start, Date end, Integer batchSize) {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname, null, start, end, batchSize);
        List result = jdbc.query(sel, extractor);
        List<HarvestSkipURI> ret = new ArrayList<>(result.size());
        for (Object o : result) {
            ret.add((HarvestSkipURI) o);
        }
        return ret;
    }
    
    /**
     * Iterator for use by caom2-artifact-download.The underlying connection remains 
     * open until the ResourceIterator is closed or the iteration reaches the end.
     * 
     * @param name class name of target skip record(s)
     * @param namespace storage namespace (prefix of the skipID aka Artifact.uri)
     * @param bucketPrefix prefix on random hex bucket
     * @param maxTryAfter maximum tryAfter date to consider
     * @return iterator of matching skip records in tryAfter order
     */
    public ResourceIterator<HarvestSkipURI> iterator(String name, String namespace, String bucketPrefix, Date maxTryAfter) {
        IteratorQuery iter = new IteratorQuery(name);
        iter.setNamespace(namespace);
        iter.setBucketPrefix(bucketPrefix);
        iter.setMaxTryAfter(maxTryAfter);
        return iter.query(dataSource);
    }
    
    public ResourceIterator<HarvestSkipURI> iterator(String name, String namespace, 
            String minBucket, String maxBucket, Date maxTryAfter) {
        IteratorQuery iter = new IteratorQuery(name);
        iter.setNamespace(namespace);
        iter.setBucketRange(minBucket, maxBucket);
        iter.setMaxTryAfter(maxTryAfter);
        return iter.query(dataSource);
    }
    
    public void put(HarvestSkipURI skip) {
        boolean update = true;
        if (skip.getID() == null) {
            update = false;
            skip.id = UUID.randomUUID();
            skip.bucket = skip.getID().toString().substring(0, BUCKET_LENGTH);
        }
        skip.lastModified = new Date();
        PutStatementCreator put = new PutStatementCreator(update);
        put.setValue(skip);
        jdbc.update(put);
    }

    public void delete(HarvestSkipURI skip) {
        if (skip == null || skip.id == null) {
            throw new IllegalArgumentException("cannot delete: " + skip);
        }

        // TODO: this is non-portable postgresql-specific (relies on casting string UUID)
        String sql = "DELETE FROM " + tableName + " WHERE id = '" + skip.getID() + "'";
        jdbc.update(sql);
    }

    public void delete(String source, String cname) {
        if (source == null || cname == null) {
            throw new IllegalArgumentException("source and cname are required");
        }

        // TODO: re-implement with PreparedStatement
        String sql = "DELETE FROM " + tableName + " WHERE source = '" + source + "' and cname = '" + cname + "'";
        jdbc.update(sql);
    }
    
    private class SelectStatementCreator implements PreparedStatementCreator {

        private String source;
        private String cname;
        private Integer batchSize;
        private URI skipID;
        private Date start;
        private Date end;

        public SelectStatementCreator() {
        }

        public void setValues(String source, String cname, URI skipID, Date start, Date end, Integer batchSize) {
            this.source = source;
            this.cname = cname;
            this.batchSize = batchSize;
            this.start = start;
            this.end = end;
            this.skipID = skipID;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
                throws SQLException {
            StringBuilder sb = new StringBuilder(SqlUtil.getSelectSQL(COLUMNS, tableName));
            sb.append(" WHERE source = ? AND cname = ?");
            if (skipID != null) {
                sb.append(" AND skipID = ?");
            } else {
                if (start != null) {
                    sb.append(" AND tryAfter >= ?");
                }
                if (end != null) {
                    sb.append(" AND tryAfter <= ?");
                }
                sb.append(" ORDER BY tryAfter ASC");
            }

            if (batchSize != null && batchSize > 0) {
                sb.append(" LIMIT ").append(batchSize.toString());
            }

            String sql = sb.toString();
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            int col = 1;
            ps.setString(col++, source);
            ps.setString(col++, cname);
            if (skipID != null) {
                ps.setString(col++, skipID.toASCIIString());
            }
            if (start != null) {
                ps.setTimestamp(col++, new Timestamp(start.getTime()), utcCalendar);
            }
            if (end != null) {
                ps.setTimestamp(col++, new Timestamp(end.getTime()), utcCalendar);
            }
        }
    }

    private class PutStatementCreator implements PreparedStatementCreator {

        private boolean update;
        private HarvestSkipURI skip;

        PutStatementCreator(boolean update) {
            this.update = update;
        }

        public void setValue(HarvestSkipURI skip) {
            this.skip = skip;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
                throws SQLException {
            String sql = null;
            if (update) {
                sql = SqlUtil.getUpdateSQL(COLUMNS, tableName);
            } else {
                sql = SqlUtil.getInsertSQL(COLUMNS, tableName);
            }
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            int col = 1;
            ps.setString(col++, skip.getSource());
            ps.setString(col++, skip.getName());
            ps.setString(col++, skip.bucket);
            ps.setString(col++, skip.getSkipID().toASCIIString());
            ps.setTimestamp(col++, new Timestamp(skip.getTryAfter().getTime()), utcCalendar);
            String es = skip.errorMessage;
            if (es != null && es.length() > 1024) {
                es = es.substring(0, 1024);
            }
            ps.setString(col++, es);
            Date now = new Date();
            ps.setTimestamp(col++, new Timestamp(now.getTime()), utcCalendar);
            ps.setObject(col++, skip.id);
        }
    }

    private class HarvestSkipMapper implements RowMapper {

        private Calendar utc;
        
        HarvestSkipMapper(Calendar utc) {
            this.utc = utc;
        }
        
        public HarvestSkipURI mapRow(ResultSet rs, int i) throws SQLException {
            
            int col = 1;
            String source = rs.getString(col++);
            String name = rs.getString(col++);
            String bucket = rs.getString(col++);
            URI skipID = Util.getURI(rs, col++);
            Date retryAfter = Util.getDate(rs, col++, utc);
            
            HarvestSkipURI ret = new HarvestSkipURI(source, name, skipID, retryAfter);
            ret.bucket = bucket;
            ret.errorMessage = rs.getString(col++);
            ret.lastModified = Util.getDate(rs, col++, utc);
            ret.id = Util.getUUID(rs, col++);
            
            return ret;
        }
    }
    
    class IteratorQuery {

        private final String name;
        private String namespace;
        
        private Date maxTryAfter;
        
        private String bucketPrefix;
        private String minBucket;
        private String maxBucket;

        public IteratorQuery(String name) {
            if (name == null) {
                throw new IllegalArgumentException("IteratorQuery: name cannot be null");
            }
            this.name = name;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public void setBucketPrefix(String prefix) {
            if (StringUtil.hasText(prefix)) {
                this.bucketPrefix = prefix.trim();
            } else {
                this.bucketPrefix = null;
            }
        }

        public void setBucketRange(String minBucket, String maxBucket) {
            this.minBucket = minBucket;
            this.maxBucket = maxBucket;
        }

        public void setMaxTryAfter(Date end) {
            this.maxTryAfter = end;
        }

        public ResourceIterator<HarvestSkipURI> query(DataSource ds) {
            
            StringBuilder sb = new StringBuilder(SqlUtil.getSelectSQL(COLUMNS, tableName));
            sb.append(" WHERE cname = ?");
            
            if (namespace != null) {
                sb.append(" AND skipID LIKE ?");
            }
            if (bucketPrefix != null) {
                sb.append(" AND bucket LIKE ?");
            } else if (minBucket != null && maxBucket != null) {
                sb.append(" AND bucket BETWEEN ? AND ?");
            }
            if (maxTryAfter != null) {
                
                sb.append(" AND tryAfter <= ?");
                //pre = " AND";
            }
            sb.append(" ORDER BY tryAfter");
            
            String sql = sb.toString();
            log.debug("sql: " + sql);
            
            try {
                Connection con = ds.getConnection();
                log.debug("IteratorQuery: setAutoCommit(false)");
                con.setAutoCommit(false);
                // defaults for options: ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                PreparedStatement ps = con.prepareStatement(sql);
                ps.setFetchSize(1000);
                ps.setFetchDirection(ResultSet.FETCH_FORWARD);
                int col = 1;
                log.debug("name: " + name);
                ps.setString(col++, name);
                    
                if (namespace != null) {
                    String val = namespace + "%";
                    log.debug("namespace prefix: " + val);
                    ps.setString(col++, val);
                }
                if (bucketPrefix != null) {
                    String val = bucketPrefix + "%";
                    log.debug("bucket prefix: " + val);
                    ps.setString(col++, val);
                } else if (minBucket != null && maxBucket != null) {
                    log.debug("bucket range: " + minBucket + "," + maxBucket);
                    ps.setString(col++, minBucket);
                    ps.setString(col++, maxBucket);
                }
                if (maxTryAfter != null) {
                    ps.setTimestamp(col, new Timestamp(maxTryAfter.getTime()), Calendar.getInstance(DateUtil.UTC));
                }
                ResultSet rs = ps.executeQuery();
                
                return new ResultSetIterator(con, rs);
            } catch (SQLException ex) {
                throw new RuntimeException("BUG: iterator query failed", ex);
            }
        }
    }
    
    private class ResultSetIterator implements ResourceIterator<HarvestSkipURI> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final Connection con;
        private final ResultSet rs;
        boolean hasRow;
        private int rowNum = 0;
        HarvestSkipMapper mapper = new HarvestSkipMapper(utc);
        
        ResultSetIterator(Connection con, ResultSet rs) throws SQLException {
            this.con = con;
            this.rs = rs;
            hasRow = rs.next();
            log.debug("ResultSetIterator: " + super.toString() + " ctor " + hasRow);
            if (hasRow) {
                rowNum++;
            } else {
                log.debug("ResultSetIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                con.setAutoCommit(true);
            }
        }

        @Override
        public void close() throws IOException {
            if (hasRow) {
                log.debug("ResultSetIterator:  " + super.toString() + " CLOSE - setAutoCommit(true)");
                try {
                    con.setAutoCommit(true);
                    hasRow = false;
                } catch (SQLException ex) {
                    throw new RuntimeException("BUG: list query failed during close()", ex);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return hasRow;
        }

        @Override
        public HarvestSkipURI next() {
            if (!hasRow) {
                throw new NoSuchElementException();
            }
            try {
                HarvestSkipURI ret = mapper.mapRow(rs, rowNum++);
                hasRow = rs.next();
                if (!hasRow) {
                    log.debug("ResultSetIterator:  " + super.toString() + " DONE - setAutoCommit(true)");
                    con.setAutoCommit(true);
                }
                return ret;
            } catch (Exception ex) {
                if (hasRow) {
                    log.debug("ResultSetIterator:  " + super.toString() + " ResultSet.next() FAILED - setAutoCommit(true)");
                    try {
                        close();
                        hasRow = false;
                    } catch (IOException unexpected) {
                        log.debug("BUG: unexpected IOException from close", unexpected);
                    }
                }
                throw new RuntimeException("BUG: list query failed while iterating", ex);
            }
        }
    }
}
