/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package ca.nrc.cadc.caom2.harvester.state;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.persistence.Util;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class SkipLoader
{
    private static final Logger log = Logger.getLogger(SkipLoader.class);
    
    public static void main(String[] args)
    {
        try
        {
            Log4jInit.setLevel("ca.nrc.cadc.caom2.harvester", Level.INFO);
            ArgumentMap am = new ArgumentMap(args);
            
            if (am.isSet("h"))
            {
                usage(null);
            }
            
            boolean dryrun = am.isSet("dryrun");
            
            String source = am.getValue("source");
            String destination = am.getValue("destination");
            if (source == null || destination == null)
                    usage("missing: --source and/or --destination");
            
            DB src = getDataSource("source", source);
            DB dest = getDataSource("destination", destination);
                
            String cname = Observation.class.getSimpleName();
            
            HarvestSkipDAO dao = null;
            if (!dryrun)
            {
                // need second datasource for dest
                DB ds2 = getDataSource("destination", destination);
                dao = new HarvestSkipDAO(ds2.ds, ds2.database, ds2.schema, 10);
            }
            
            List<String> collections = getCollections(src, true);
            Iterator<Item> srcIter = getObsIterator(src, true, null);   // new LineIterator(new File(srcFile));
            Iterator<Item> destIter = getObsIterator(dest, false, collections); // new LineIterator(new File(destFile));
            
            Item di = null;
            while (srcIter.hasNext())
            {   
                Item si = srcIter.next();
                if (di == null && destIter.hasNext())
                    di = destIter.next();
                
                // lists are sorted by observationID so this is a marge-join
                int comp = -1; // defalt: reached end of destIter
                boolean np = true;
                if (di != null)
                {
                    comp = si.observationID.compareTo(di.observationID);
                    if (comp == 0)
                        np = (si.num == di.num);
                }                
                if (comp == 0 && np)
                {
                    // observation present in both lists
                    log.info("ok: " + di);
                    if (destIter.hasNext())
                        di = destIter.next();
                    else
                        di = null;
                }
                else if (comp < 0 || !np)
                {
                    if (comp < 0)
                        log.info("missing: " + si);
                    if (!np)
                        log.info("mismatch: " + si);
                    
                    // observation only in source
                    Thread.sleep(1L); // make sure timestamps are spread out
                    HarvestSkip h = new HarvestSkip(source, cname, si.obsID, null);
                    try
                    {
                        if (!dryrun)
                        {
                            log.info("put: " + h);
                            dao.put(h);
                        }
                    }
                    catch(DataIntegrityViolationException ex)
                    {
                        log.warn("put: " + h + " duplicate: skip");
                    }
                }
                else if (comp > 0)
                {
                    // observation only in destination
                    while (destIter.hasNext() && comp > 0)
                    {
                        log.warn("unexpected: " + di);
                        di = destIter.next();
                        comp = si.observationID.compareTo(di.observationID);
                    }
                }
                
            }
        }
        catch(Exception ex)
        {
            log.error("unexpected fail", ex);
        }
        finally
        {
            log.info("DONE");
        }
    }
    
    private static List<String> getCollections(DB db, boolean fakeSchemaPrefix)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT distinct(collection)");
        sb.append(" FROM ").append(db.database).append(".").append(db.schema).append(".");
        if (fakeSchemaPrefix)
            sb.append("caom2_");
        sb.append("Observation");
        String sql = sb.toString();
        log.info("getCollections SQL: " + sql);
        JdbcTemplate jdbc = new JdbcTemplate(db.ds);
        List<String> ret = (List<String>) jdbc.queryForList(sql, String.class);
        log.info("found: " + ret.size() + " collections");
        return ret;
    }
    
    private static Iterator<Item> getObsIterator(DB db, boolean fakeSchemaPrefix, List<String> collections)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT o.observationID, o.obsID, count(*)");
        sb.append(" FROM ").append(db.database).append(".").append(db.schema).append(".");
        if (fakeSchemaPrefix)
            sb.append("caom2_");
        sb.append("Observation AS o JOIN ");
        sb.append(db.database).append(".").append(db.schema).append(".");
        if (fakeSchemaPrefix)
            sb.append("caom2_");
        sb.append("Plane AS p ON o.obsID = p.obsID");
        if (collections != null && !collections.isEmpty())
        {
            sb.append(" WHERE o.collection IN (");
            for (String col : collections)
                sb.append("'").append(col).append("'").append(",");
            sb.setCharAt(sb.length() - 1, ')'); // replace last ,  with )
        }
        sb.append(" GROUP BY o.observationID, o.obsID");
        String sql = sb.toString();
        log.info("getObsIterator SQL: " + sql);
        JdbcTemplate jdbc = new JdbcTemplate(db.ds);
        List<Item> ret = (List<Item>) jdbc.query(sql, new ItemMapper());
        log.info("found: " + ret.size() + " sorting...");
        Collections.sort(ret);
        return ret.iterator();
    }
    
    private static class ItemMapper implements RowMapper
    {
        public Object mapRow(ResultSet rs, int i) throws SQLException
        {
            Item ret = new Item();
            ret.observationID = rs.getString(1);
            ret.obsID = Util.getUUID(rs, 2);
            ret.num = rs.getInt(3);
            return ret;
        }
        
    }
    private static class DB
    {
        DataSource ds;
        String database;
        String schema;
        DB(String database, String schema, DataSource ds)
        {
            this.database = database;
            this.schema = schema;
            this.ds = ds;
        }
    }
    
    private static DB getDataSource(String arg, String s)
        throws FileNotFoundException, IOException
    {
        String[] sds = s.split("[.]");
        if (sds.length != 3)
            usage("invalid input: --"+arg+"=" + s + " " + sds.length);
        String server = sds[0];
        String database = sds[1];
        String schema = sds[2];
        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
        return new DB(database, schema, DBUtil.getDataSource(cc));
    }
    
    private static void usage(String msg)
    {
        log.info("usage: skipLoader [--dryrun] --source=<srv.db.schema> --destination=<srv.db.schema>");
        if (msg != null)
            log.error(msg);
        System.exit(1);
    }
    
    private static class Item implements Comparable<Item>
    {
        String observationID;
        UUID obsID;
        int num;
        
        @Override
        public String toString()
        {
            return observationID + " " + obsID;
        }

        public int compareTo(Item t)
        {
            return observationID.compareTo(t.observationID);
        }
        
        
    }
}
