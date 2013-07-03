
package ca.nrc.cadc.caom.harvester.state;

import ca.nrc.cadc.caom2.CaomIDGenerator;
import ca.nrc.cadc.caom2.persistence.Util;
import ca.nrc.cadc.date.DateUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Calendar;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;

/**
 * Queries to manage state in the harvest table.
 * 
 * @version $Revision: 159 $
 * @author $Author: pdowler $
 */
public class HarvestStateDAO
{
    private static Logger log = Logger.getLogger(HarvestStateDAO.class);

    private static final String[] COLUMNS =
    {
        "source", "code", "cname", "curLastModified", "curID",
        "lastModified", "stateID"
    };
    
    private String tableName;
    private JdbcTemplate jdbc;
    private ResultSetExtractor extractor;

    private Calendar CAL = Calendar.getInstance(DateUtil.UTC);

    public HarvestStateDAO(DataSource dataSource, String database, String schema)
    {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tableName = database + "." +  schema + ".HarvestState";
        this.extractor = new StateExtractor();
    }

    // get the current state from the database
    public HarvestState get(String source, String cname)
    {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname);
        Object o = jdbc.query(sel, extractor);
        if (o == null)
        {
            HarvestState s = new HarvestState();
            s.cname = cname;
            s.source = source;
            s.code = source.hashCode();
            log.debug("created: " + s);
            return s;
        }
        return (HarvestState) o;
    }

    // put the new state in the database (insert or update as necessary)
    public void put(HarvestState state)
    {
        boolean update = true;
        if (state.id == null)
        {
            update = false;
            state.id = CaomIDGenerator.getInstance().generateID();
        }
        state.lastModified = new Date();
        PutStatementCreator put = new PutStatementCreator(update);
        put.setValue(state);
        jdbc.update(put);
    }
    
    private class SelectStatementCreator  implements PreparedStatementCreator
    {
        private String source;
        private String cname;

        public SelectStatementCreator() { }

        public void setValues(String source, String cname)
        {
            this.source = source;
            this.cname = cname;
        }
        public PreparedStatement createPreparedStatement(Connection conn)
            throws SQLException
        {
            String sql = SqlUtil.getSelectSQL(COLUMNS, tableName)
                    + " WHERE source = ? AND cname = ?";
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }
        private void loadValues(PreparedStatement ps)
            throws SQLException
        {
            if (source == null || cname == null)
                throw new IllegalStateException("null arg(s): " + source + "," + cname);

            ps.setString(1, source);
            ps.setString(2, cname);
        }
    }


    private class PutStatementCreator implements PreparedStatementCreator
    {
        private boolean update;
        private HarvestState state;

        PutStatementCreator(boolean update) { this.update = update; }

        public void setValue(HarvestState state)
        {
            this.state = state;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
            throws SQLException
        {
            String sql = null;
            if (update)
                sql = SqlUtil.getUpdateSQL(COLUMNS, tableName);
            else
                sql = SqlUtil.getInsertSQL(COLUMNS, tableName);
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }
        private void loadValues(PreparedStatement ps)
            throws SQLException
        {
            int col = 1;
            ps.setString(col++, state.source);
            ps.setInt(col++, state.code);
            ps.setString(col++, state.cname);
            if (state.curLastModified != null)
                ps.setTimestamp(col++, new Timestamp(state.curLastModified.getTime()), CAL);
            else
                ps.setNull(col++, Types.TIMESTAMP);
            if (state.curID != null)
                ps.setLong(col++, state.curID);
            else
                ps.setNull(col++, Types.BIGINT);

            ps.setTimestamp(col++, new Timestamp(state.lastModified.getTime()), CAL);
            ps.setLong(col++, state.id);
        }
    }
    
    private class StateExtractor implements ResultSetExtractor
    {
        /**
         * @param rs ResultSet
         * @return a java.util.Date value
         * @throws SQLException if accessing result set fails
         */
        public Object extractData(ResultSet rs) 
            throws SQLException
        {
            HarvestState ret = null;
            if (rs.next())
            {
                ret = new HarvestState();
                int col = 1;
                ret.source = rs.getString(col++);
                ret.code = rs.getInt(col++);
                ret.cname = rs.getString(col++);
                ret.curLastModified = Util.getDate(rs, col++, CAL);
                ret.curID = Util.getLong(rs, col++);
                
                ret.lastModified = Util.getDate(rs, col++, CAL);
                ret.id = Util.getLong(rs, col++);
            }
            return ret;
        }
    }

    
}

