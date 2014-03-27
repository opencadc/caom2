
package ca.nrc.cadc.caom.harvester.state;

import ca.nrc.cadc.caom2.CaomIDGenerator;
import ca.nrc.cadc.caom2.persistence.Util;
import ca.nrc.cadc.date.DateUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class HarvestSkipDAO
{
    private static Logger log = Logger.getLogger(HarvestSkipDAO.class);

    private static final String[] COLUMNS =
    {
        "source", "cname", "skipID",
        "lastModified", "id"
    };
    
    private String tableName;
    private Integer batchSize;
    private JdbcTemplate jdbc;
    private RowMapper extractor;

    private Calendar CAL = Calendar.getInstance(DateUtil.UTC);

    public HarvestSkipDAO(DataSource dataSource, String database, String schema, Integer batchSize)
    {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tableName = database + "." +  schema + ".HarvestSkip";
        this.batchSize = batchSize;
        this.extractor = new HarvestSkipMapper();
    }

    public HarvestSkip get(String source, String cname, Long skipID)
    {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname, skipID, null, null);
        List result = jdbc.query(sel, extractor);
        if (result.isEmpty())
            return null;
        return (HarvestSkip) result.get(0);
    }
    
    public List<HarvestSkip> get(String source, String cname, Date start)
    {
        SelectStatementCreator sel = new SelectStatementCreator();
        sel.setValues(source, cname, null, batchSize, start);
        List result = jdbc.query(sel, extractor);
        List<HarvestSkip> ret = new ArrayList<HarvestSkip>(result.size());
        for (Object o : result)
            ret.add((HarvestSkip) o);
        return ret;
    }
    
    public void put(HarvestSkip skip)
    {
        boolean update = true;
        if (skip.id == null)
        {
            update = false;
            skip.id = CaomIDGenerator.getInstance().generateID();
        }
        skip.lastModified = new Date();
        PutStatementCreator put = new PutStatementCreator(update);
        put.setValue(skip);
        jdbc.update(put);
    }

    public void delete(HarvestSkip skip)
    {
        if (skip == null || skip.id == null)
            throw new IllegalArgumentException("cannot delete: " + skip);

        String sql = "DELETE FROM " + tableName + " WHERE id = " + skip.id;
        jdbc.update(sql);
    }

    private class SelectStatementCreator implements PreparedStatementCreator
    {
        private String source;
        private String cname;
        private Integer batchSize;
        private Long skipID;
        private Date start;

        public SelectStatementCreator() { }

        public void setValues(String source, String cname, Long skipID, Integer batchSize, Date start)
        {
            this.source = source;
            this.cname = cname;
            this.batchSize = batchSize;
            this.start = start;
            this.skipID = skipID;
        }

        public PreparedStatement createPreparedStatement(Connection conn)
            throws SQLException
        {
            StringBuilder sb = new StringBuilder(SqlUtil.getSelectSQL(COLUMNS, tableName));
            sb.append(" WHERE source = ? AND cname = ?");
            if (skipID != null)
                sb.append(" AND skipID = ?");
            else if (start != null)
            {
                sb.append(" AND lastModified >= ?");
            }
            sb.append(" ORDER BY lastModified ASC");
            
            if (batchSize != null && batchSize.intValue() > 0)
                sb.append(" LIMIT ").append(batchSize.toString());
            
            String sql = sb.toString();
            PreparedStatement prep = conn.prepareStatement(sql);
            log.debug(sql);
            loadValues(prep);
            return prep;
        }
        private void loadValues(PreparedStatement ps)
            throws SQLException
        {
            ps.setString(1, source);
            ps.setString(2, cname);
            if (skipID != null)
                ps.setLong(3, skipID);
            else if (start != null)
                ps.setTimestamp(3, new Timestamp(start.getTime()), CAL);
        }
    }

    private class PutStatementCreator implements PreparedStatementCreator
    {
        private boolean update;
        private HarvestSkip skip;

        PutStatementCreator(boolean update) { this.update = update; }

        public void setValue(HarvestSkip skip)
        {
            this.skip = skip;
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
            Date now = new Date();
            int col = 1;
            ps.setString(col++, skip.source);
            ps.setString(col++, skip.cname);
            ps.setLong(col++, skip.skipID);
            ps.setTimestamp(col++, new Timestamp(now.getTime()), CAL);
            ps.setLong(col++, skip.id);
        }
    }

    private class HarvestSkipMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int i) throws SQLException
        {
            HarvestSkip ret = new HarvestSkip();
            int col = 1;
            ret.source = rs.getString(col++);
            ret.cname = rs.getString(col++);
            ret.skipID = rs.getLong(col++);
            ret.lastModified = Util.getDate(rs, col++, CAL);
            ret.id = rs.getLong(col++);
            return ret;
        }

    }
}
