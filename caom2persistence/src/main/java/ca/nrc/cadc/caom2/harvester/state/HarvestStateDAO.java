/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2017.                            (c) 2017.
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Queries to manage state in the harvest table.
 *
 * @version $Revision: 159 $
 * @author $Author: pdowler $
 */
public abstract class HarvestStateDAO {

    private static Logger log = Logger.getLogger(HarvestStateDAO.class);

    private static final String[] COLUMNS
            = {
                "source", "cname", "curLastModified", "curID",
                "lastModified", "stateID"
            };

    protected String fakeSchemaTablePrefix = null;
    private String database;
    private String schema;
    private String tableName;
    private JdbcTemplate jdbc;
    private ResultSetExtractor extractor;

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    public HarvestStateDAO(DataSource dataSource, String database, String schema) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.database = database;
        this.schema = schema;
        this.extractor = new StateExtractor();
    }

    protected void init() {
        StringBuilder sb = new StringBuilder();
        sb.append(database).append(".").append(schema).append(".");
        if (fakeSchemaTablePrefix != null) {
            sb.append(fakeSchemaTablePrefix);
        }
        sb.append(HarvestState.class.getSimpleName());
        this.tableName = sb.toString();
    }

    // get the current state from the database
    public HarvestState get(String source, String cname) {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname);
        Object o = jdbc.query(sel, extractor);
        if (o == null) {
            HarvestState s = new HarvestState();
            s.cname = cname;
            s.source = source;
            log.debug("created: " + s);
            return s;
        }
        return (HarvestState) o;
    }

    // put the new state in the database (insert or update as necessary)
    public void put(HarvestState state) {
        boolean update = true;
        if (state.id == null) {
            update = false;
            state.id = UUID.randomUUID();
        }
        state.lastModified = new Date();
        PutStatementCreator put = new PutStatementCreator(update);
        put.setValue(state);
        jdbc.update(put);
    }

    protected abstract void setUUID(PreparedStatement ps, int col, UUID val)
            throws SQLException;

    private class SelectStatementCreator implements PreparedStatementCreator {

        private String source;
        private String cname;

        public SelectStatementCreator() {
        }

        public void setValues(String source, String cname) {
            this.source = source;
            this.cname = cname;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
                throws SQLException {
            String sql = SqlUtil.getSelectSQL(COLUMNS, tableName)
                    + " WHERE source = ? AND cname = ?";
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }

        private void loadValues(PreparedStatement ps)
                throws SQLException {
            if (source == null || cname == null) {
                throw new IllegalStateException("null arg(s): " + source + "," + cname);
            }

            ps.setString(1, source);
            ps.setString(2, cname);
        }
    }

    private class PutStatementCreator implements PreparedStatementCreator {

        private boolean update;
        private HarvestState state;

        PutStatementCreator(boolean update) {
            this.update = update;
        }

        public void setValue(HarvestState state) {
            this.state = state;
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
            StringBuilder sb = new StringBuilder("values: ");
            int col = 1;
            ps.setString(col++, state.source);
            sb.append(state.source).append(",");
            ps.setString(col++, state.cname);
            sb.append(state.cname).append(",");
            if (state.curLastModified != null) {
                ps.setTimestamp(col++, new Timestamp(state.curLastModified.getTime()), utcCalendar);
                sb.append(state.curLastModified).append(",");
            } else {
                ps.setNull(col++, Types.TIMESTAMP);
                sb.append("NULL,");
            }
            setUUID(ps, col++, state.curID);
            sb.append(state.curID).append(",");

            ps.setTimestamp(col++, new Timestamp(state.lastModified.getTime()), utcCalendar);
            sb.append(state.lastModified).append(",");
            setUUID(ps, col++, state.id);
            sb.append(state.id).append("");
            log.debug(sb.toString());
        }
    }

    private class StateExtractor implements ResultSetExtractor {

        /**
         * @param rs ResultSet
         * @return a java.util.Date value
         * @throws SQLException if accessing result set fails
         */
        public Object extractData(ResultSet rs)
                throws SQLException {
            HarvestState ret = null;
            if (rs.next()) {
                ret = new HarvestState();
                int col = 1;
                ret.source = rs.getString(col++);
                ret.cname = rs.getString(col++);
                ret.curLastModified = Util.getDate(rs, col++, utcCalendar);
                ret.curID = Util.getUUID(rs, col++);

                ret.lastModified = Util.getDate(rs, col++, utcCalendar);
                ret.id = Util.getUUID(rs, col++);
            }
            return ret;
        }
    }

}
