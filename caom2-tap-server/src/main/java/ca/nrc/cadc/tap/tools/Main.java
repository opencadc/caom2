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

package ca.nrc.cadc.tap.tools;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.tap.impl.CaomAdqlQuery;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import ca.nrc.cadc.tap.schema.TapSchemaDAOImpl;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * Simple tool that parses and converts and ADQL query to SQL using the 
 * provided AdqlQueryImpl. 
 * 
 * @author pdowler
 */
public class Main implements Runnable
{
    private static final Logger log = Logger.getLogger(Main.class);
    
    private static String[] logPackages = new String[]
    {
        "ca.nrc.cadc.tap",
        "ca.nrc.cadc.util.db",
        "ca.nrc.cadc.ac.client"
    };
    
    public static void main(String[] args)
    {
        try
        {
            ArgumentMap am = new ArgumentMap(args);
            Level level = Level.WARN;
            if (am.isSet("d") || am.isSet("debug"))
                level = Level.DEBUG;
            else if (am.isSet("v") || am.isSet("verbose"))
                level = Level.INFO;
            for (String s : logPackages)
            {
                Log4jInit.setLevel(s, level);
            }
            
            // required args or help
            if ( !am.isSet("in") || am.isSet("h") || am.isSet("help"))
                usage();
            
            Subject subject;
            if (am.isSet("auth"))
                subject = CertCmdArgUtil.initSubject(am, false);
            else
                subject = AuthenticationUtil.getAnonSubject();
                
            String str = am.getValue("in");
            File f = new File(str);
            if ( !f.exists() )
            {
                log.error("input file does not exist: " + str);
                usage();
            }
            
            DataSource ds = null;
            String server = am.getValue("server");
            String database = am.getValue("database");
            if (server != null && database != null)
            {
                DBConfig dbrc = new DBConfig();
                ConnectionConfig cc = dbrc.getConnectionConfig(server, database);
                log.debug("creating DataSource: " + cc);
                ds = DBUtil.getDataSource(cc);
                if (ds == null)
                {
                    log.error("failed to create DataSource for: " + server + " " + database);
                    usage();
                }
            }
            else
            {
                log.error("missing --server and/or --database: " + server + " " + database);
                usage();
            }
            
            Integer maxrec = null;
            str = am.getValue("maxrec");
            if (str != null)
            {
                try { maxrec = new Integer(str); }
                catch(NumberFormatException nex)
                {
                    log.error("--maxrec value must be an integer: " + str);
                    usage();
                }
            }
                
            
            String adql = StringUtil.readFromInputStream(new FileInputStream(f), "UTF-8");
            log.info("ADQL:\n\n" + adql + "\n");
            Job adqlJob = new Job()
            {
                @Override
                public String getID() { return "internal-jobID"; }
            };
            adqlJob.getParameterList().add(new Parameter("QUERY", adql));
            
            Main m = new Main(adqlJob, maxrec, ds);
            Subject.doAs(subject, new RunnableAction(m));
        }
        catch(Throwable t)
        {
            log.error("unexpected failure", t);
            System.exit(1);
        }
        System.exit(0);
    }
    
    private Job job;
    private Integer maxrec;
    private DataSource ds;
    Main(Job job, Integer maxrec, DataSource ds)
    {
        this.job = job;
        this.maxrec = maxrec;
        this.ds = ds;
    }
    
    public void run()
    {
        long t1 = System.currentTimeMillis();
        TapSchema ts = loadTapSchema(ds);
            
        long t2 = System.currentTimeMillis();
        
        CaomAdqlQuery validator = new CaomAdqlQuery();
        validator.setJob(job);
        validator.setMaxRowCount(maxrec);
        validator.setTapSchema(ts);

        String sql = validator.getSQL();
        long t3 = System.currentTimeMillis();
        log.info("SQL:\n\n" + sql + "\n");

        List<String> plan = explainQuery(sql, ds);
        long t4 = System.currentTimeMillis();
        log.info("query plan:\n\n");
        for (String p : plan)
        {
            System.out.println(p);
        }
        System.out.println("loadTapSchema: " + (t2-t1));
        System.out.println("getSQL: " + (t3-t2));
        System.out.println("explainSQL: " + (t4-t3));
        
    }
    
    private static TapSchema loadTapSchema(DataSource ds)
    {
        TapSchemaDAO dao = new TapSchemaDAOImpl();
        dao.setDataSource(ds);
        return dao.get();
    }
    
    private static List<String> explainQuery(String sql, DataSource ds)
    {
        String explain = "explain  " + sql;
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        
        return (List<String>) jdbc.query(explain, new PlanExtractor());
    }
    
    private static class PlanExtractor implements RowMapper
    {
        @Override
        public Object mapRow(ResultSet rs, int row) throws SQLException
        {
            StringBuilder sb = new StringBuilder();
            
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            for (int i = 0; i<cols; i++)
            {
                Object o = rs.getObject(i+1);
                sb.append(o);
            }
            return sb.toString();
        }

    }
    
    private static void usage()
    {
        System.out.println("usage: tapExplainADQL [-v|--verbose|-d|--debug]");
        System.out.println("          [--maxrec=<max number of result rows>]");
        System.out.println("          [--auth] use identity from $HOME/.ssl/cadcproxy.pem to process query");
        System.out.println("          --server=<server>");
        System.out.println("          --database=<caom2 database>");
        System.out.println("          --in=<file with ADQL query>");
        System.exit(-1);
    }
}
