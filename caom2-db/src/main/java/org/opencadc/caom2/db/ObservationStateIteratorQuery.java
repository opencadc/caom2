/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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
************************************************************************
*/

package org.opencadc.caom2.db;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.io.ResourceIterator;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.caom2.db.mappers.ObservationStateMapper;
import org.opencadc.caom2.util.ObservationState;

/**
 *
 * @author pdowler
 */
public class ObservationStateIteratorQuery  {
    private static final Logger log = Logger.getLogger(ObservationStateIteratorQuery.class);

    private final SQLGenerator gen;
    
    private final String collection;
    private final String namespace;
    
    private String uriBucket;
    private Date minLastModified;
    private Date maxLastModified;
    private Integer batchSize;
    
    public ObservationStateIteratorQuery(SQLGenerator gen, String collection, String namespace) { 
        this.gen = gen;
        this.collection = collection;
        this.namespace = namespace;
    }

    public void setUriBucket(String uriBucket) {
        this.uriBucket = uriBucket;
    }

    public void setMinLastModified(Date minLastModified) {
        this.minLastModified = minLastModified;
    }

    public void setMaxLastModified(Date maxLastModified) {
        this.maxLastModified = maxLastModified;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public ResourceIterator<ObservationState> query(DataSource ds) {
        StringBuilder sb = gen.getSelectSQL(ObservationState.class);
        String alias = gen.getAlias(ObservationState.class);
        
        if (collection != null) {
            sb.append(" WHERE ").append(alias).append(".collection = ?");
        } else if (namespace != null) {
            sb.append(" WHERE ").append(alias).append(".uri LIKE ?");
        } else {
            throw new RuntimeException("BUG: query called without required collection | namespace");
        }

        if (uriBucket != null) {
            sb.append(" AND").append(alias).append(".uriBucket LIKE ?");
        }

        if (minLastModified != null && maxLastModified != null) {
            sb.append(" AND ").append(alias).append(".maxLastModified BETWEEN ? AND ?");
        } else if (minLastModified != null) {
            sb.append(" AND ").append(alias).append(".maxLastModified >= ?");
        } else if (maxLastModified != null) {
            sb.append(" AND ").append(alias).append(".maxLastModified <= ?");
        }
        
        if (collection != null) {
            sb.append(" ORDER BY ").append(alias).append(".maxLastModified ASC");
        } else {
            sb.append(" ORDER BY ").append(alias).append(".uri ASC");
        }
        
        if (batchSize != null) {
            sb.append(" LIMIT ?");
        }
        
        String sql = sb.toString();
        log.warn("SQL: " + sb.toString());
        
        Calendar utc = Calendar.getInstance(DateUtil.UTC);
        try {
            Connection con = ds.getConnection();
            log.debug("ObservationStateIteratorQuery: setAutoCommit(false)");
            con.setAutoCommit(false);
            // defaults for options: ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setFetchSize(1000);
            ps.setFetchDirection(ResultSet.FETCH_FORWARD);
            int col = 1;
            if (collection != null) {
                ps.setString(col++, collection);
            } else {
                ps.setString(col++, namespace + "%");
            }
            if (uriBucket != null) {
                ps.setString(col++, uriBucket + "%");
            }
            if (minLastModified != null) {
                ps.setTimestamp(col++, new Timestamp(minLastModified.getTime()), utc);
            } else if (maxLastModified != null) {
                ps.setTimestamp(col++, new Timestamp(maxLastModified.getTime()), utc);
            }
            if (batchSize != null) {
                ps.setInt(col++, batchSize);
            }
            
            ResultSet rs = ps.executeQuery();
            return new ObservationStateIterator(con, rs);
        } catch (SQLException ex) {
            throw new RuntimeException("BUG: ObservationState iterator query failed", ex);
        }
    }

    private class ObservationStateIterator implements ResourceIterator<ObservationState> {
        final Calendar utc = Calendar.getInstance(DateUtil.UTC);
        private final Connection con;
        private final ResultSet rs;
        private boolean hasRow;
        private int rowNum = 0;
        
        private ObservationStateMapper mapper = new ObservationStateMapper();

        public ObservationStateIterator(Connection con, ResultSet rs) {
            this.con = con;
            this.rs = rs;
            try {
                hasRow = rs.next();
                log.debug("ObservationStateIterator: " + super.toString() + " ctor " + hasRow);
                if (!hasRow) {
                    log.debug("ObservationStateIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        con.close(); // return to pool
                    } catch (SQLException unexpected) {
                        log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException("FAIL: failed to check ResultSet", ex);
            }
        }

        @Override
        public boolean hasNext() {
            return hasRow;
        }

        @Override
        public ObservationState next() {
            try {
                ObservationState ret = mapper.mapRow(rs, ++rowNum);
                hasRow = rs.next();
                if (!hasRow) {
                    log.debug("ArtifactResultSetIterator:  " + super.toString() + " DONE - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        con.close(); // return to pool
                    } catch (SQLException unexpected) {
                        log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                    }
                }
                return ret;
            } catch (SQLException ex) {
                if (hasRow) {
                    log.debug("ArtifactResultSetIterator:  " + super.toString() + " ResultSet.next() FAILED - setAutoCommit(true)");
                    try {
                        con.setAutoCommit(true); // commit txn
                        con.close(); // return to pool
                        hasRow = false;
                    } catch (SQLException unexpected) {
                        log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                    }
                }
                throw new RuntimeException("BUG: artifact list query failed while iterating", ex);
            }
        }

        @Override
        public void close() throws IOException {
            if (hasRow) {
                log.debug("ObservationStateIterator:  " + super.toString() + " ctor - setAutoCommit(true)");
                try {
                    con.setAutoCommit(true); // commit txn
                    con.close(); // return to pool
                    hasRow = false; 
                } catch (SQLException unexpected) {
                    log.error("Connection.setAutoCommit(true) & close() failed", unexpected);
                }
            }
        }
        
    }
}
