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
 ************************************************************************
 */

package ca.nrc.cadc.caom2.version;

import ca.nrc.cadc.caom2.persistence.DatabaseTransactionManager;
import ca.nrc.cadc.caom2.persistence.TransactionManager;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Utility class to setup the caom2 tables in the database. This currently works
 * for the postgresql implementation only.
 *
 * @author pdowler
 */
public class InitDatabase {

    private static final Logger log = Logger.getLogger(InitDatabase.class);

    public static final String MODEL_NAME = "CAOM";
    public static final String MODEL_VERSION = "2.3.5";
    public static final String PREV_MODEL_VERSION = "2.3";

    static String[] CREATE_SQL = new String[]{
        "caom2.ModelVersion.sql",
        "caom2.Observation.sql",
        "caom2.Plane.sql",
        "caom2.Artifact.sql",
        "caom2.Part.sql",
        "caom2.Chunk.sql",
        "caom2.HarvestState.sql",
        "caom2.HarvestSkip.sql",
        "caom2.HarvestSkipURI.sql",
        "caom2.access.sql",
        "caom2.deleted.sql",
        "caom2.extra_indices.sql",
        "caom2.ObsCore.sql",
        "caom2.ObsCore-x.sql",
        "caom2.SIAv1.sql"
    };

    static String[] UPGRADE_SQL = new String[]{
        "caom2.upgrade-2.3.5.sql",
        "caom2.ObsCore.sql",
        "caom2.ObsCore-x.sql",
        "caom2.SIAv1.sql"
    };

    private final DataSource dataSource;
    private final String database;
    private final String schema;

    public InitDatabase(DataSource dataSource, String database, String schema) {
        this.dataSource = dataSource;
        this.database = database;
        this.schema = schema;
    }

    /**
     * Create or upgrade the configured database with CAOM tables and indices.
     *
     * @return true if tables were created/upgraded; false for no-op
     */
    public boolean doInit() {
        log.debug("doInit: " + MODEL_NAME + " " + MODEL_VERSION);
        long t = System.currentTimeMillis();
        String prevVersion = null;

        TransactionManager txn = new DatabaseTransactionManager(dataSource);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        try {
            // get current ModelVersion
            ModelVersionDAO vdao = new ModelVersionDAO(dataSource, database, schema);
            ModelVersion cur = vdao.get(MODEL_NAME);
            log.debug("found: " + cur);
            prevVersion = cur.version;

            // select SQL to execute
            String[] ddls = CREATE_SQL; // default
            boolean upgrade = false;
            if (cur.version != null && MODEL_VERSION.equals(cur.version)) {
                log.debug("doInit: already up to date - nothing to do");
                return false;
            }
            if (cur.version != null && cur.version.equals(PREV_MODEL_VERSION)) {
                ddls = UPGRADE_SQL;
                upgrade = true;
            } else if (cur.version != null) {
                throw new UnsupportedOperationException("doInit: version upgrade not supported: " + cur.version + " -> " + MODEL_VERSION);
            }

            // start transaction
            txn.startTransaction();

            // execute SQL
            for (String fname : ddls) {
                log.info("process file: " + fname);
                List<String> statements = parseDDL(fname, schema);
                for (String sql : statements) {
                    if (upgrade) {
                        log.info("execute:\n" + sql);
                    } else {
                        log.debug("execute:\n" + sql);
                    }
                    jdbc.execute(sql);
                }
            }
            // update ModelVersion
            cur.version = MODEL_VERSION;
            vdao.put(cur);

            // commit transaction
            txn.commitTransaction();
            return true;
        } catch (Exception ex) {
            log.debug("epic fail", ex);

            if (txn.isOpen()) {
                try {
                    txn.rollbackTransaction();
                } catch (Exception oops) {
                    log.error("failed to rollback transaction", oops);
                }
            }
            throw new RuntimeException("failed to init database", ex);
        } finally {
            // check for open transaction
            if (txn.isOpen()) {
                log.error("BUG: open transaction in finally");
                try {
                    txn.rollbackTransaction();
                } catch (Exception ex) {
                    log.error("failed to rollback transaction in finally", ex);
                }
            }

            long dt = System.currentTimeMillis() - t;
            log.debug("doInit: " + MODEL_NAME + " " + prevVersion + " to " + MODEL_VERSION + " " + dt + "ms");
        }
    }
    
    static List<String> parseDDL(String fname, String schema) throws IOException {
        List<String> ret = new ArrayList<>();

        // find file
        URL url = InitDatabase.class.getClassLoader().getResource("postgresql/" + fname);
        log.debug("found " + fname + ": " + url);

        // read
        InputStreamReader isr = new InputStreamReader(url.openStream());
        LineNumberReader r = new LineNumberReader(isr);
        try {
            StringBuilder sb = new StringBuilder();
            String line = r.readLine();
            boolean eos = false;
            while (line != null) {
                line = line.trim();
                if (line.startsWith("--")) {
                    line = "";
                }
                if (!line.isEmpty()) {
                    if (line.endsWith(";")) {
                        eos = true;
                        line = line.substring(0, line.length() - 1);
                    }
                    sb.append(line).append(" ");
                    if (eos) {
                        String st = sb.toString();
                        
                        st = st.replaceAll("<schema>", schema);
                        
                        log.debug("statement: " + st);
                        ret.add(st);
                        sb = new StringBuilder();
                        eos = false;
                    }
                }
                line = r.readLine();
            }
        } finally {
            r.close();
        }

        return ret;
    }

}
