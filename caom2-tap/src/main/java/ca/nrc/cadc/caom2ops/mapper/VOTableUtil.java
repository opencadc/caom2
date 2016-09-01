/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.dali.tables.votable.VOTableField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class VOTableUtil
{
    private static final Logger log = Logger.getLogger(VOTableUtil.class);
    
    /**
     * Construct a map of utype values to column index for a table data. The map
     * can be used with a VOTableRowMapper to construct an object from a row.
     * 
     * @param fields
     * @return a map of utype to column-index
     */
    public static Map<String,Integer> buildUTypeMap(final List<VOTableField> fields)
    {
        Map<String,Integer> rowMap = new HashMap<String,Integer>();
        for (int index = 0; index < fields.size(); index++)
        {
            VOTableField field = fields.get(index);
            // foreign key columns have null utype so utype should be unique now
            if (field.utype != null)
            {
                Integer cur = rowMap.get(field.utype);
                if (cur != null)
                    throw new IllegalStateException("found multiple columns with utype " + field.utype
                            + ": " + cur + " and " + index);
                log.debug(field.utype + " -> " + index);
                rowMap.put(field.utype, index);
            }
        }

        return rowMap;
    }
}
