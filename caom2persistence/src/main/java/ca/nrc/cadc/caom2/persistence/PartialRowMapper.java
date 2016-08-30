
package ca.nrc.cadc.caom2.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public interface PartialRowMapper<T> extends RowMapper
{
    /**
     * 
     * @return the number of columns this RowMapper consumes
     */
    public int getColumnCount();
    
    /**
     * 
     * @param rs
     * @param row
     * @param offset the first column from which to get domain object state
     * @return the domain object constructed from the columns of the current row,
     *         with just the ID set if it is the same as the last object mapped
     * @throws java.sql.SQLException
     */
    public T mapRow(ResultSet rs, int row, int offset)
        throws SQLException;
}
