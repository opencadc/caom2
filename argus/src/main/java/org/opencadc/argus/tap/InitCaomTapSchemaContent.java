/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.argus.tap;

import ca.nrc.cadc.db.version.InitDatabase;
import java.net.URL;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * This class automates adding/updating the description of CAOM tables and views
 * in the tap_schema. This class assumes that it can re-use the tap_schema.ModelVersion
 * table (usually created by InitDatabaseTS in cadc-tap-schema library) and does
 * not try to create it.  The init includes base CAOM tables and IVOA views (ObsCore++),
 * but <em>does not include</em> aggregate (simple or materialised) views. The service
 * operator must create simple views manually or implement a mechanism to create and
 * update materialised views periodically.
 * 
 * @author pdowler
 */
public class InitCaomTapSchemaContent extends InitDatabase {
    private static final Logger log = Logger.getLogger(InitCaomTapSchemaContent.class);

    public static final String MODEL_NAME = "caom2-schema";
    public static final String MODEL_VERSION = "2.5.0-g";

    // the SQL is tightly coupled to cadc-tap-schema table names (for TAP-1.1)
    static String[] BASE_SQL = new String[] {
        "caom2.tap_schema_content11.sql",
        "ivoa.tap_schema_content11.sql"
    };

    // upgrade is normally the same as create since SQL is idempotent
    static String[] BASE_EXTRA_SQL = new String[] {
        "caom2.tap_schema_content11.sql",
        "ivoa.tap_schema_content11.sql"
    };
    
    /**
     * Constructor. The schema argument is used to query the ModelVersion table
     * as {schema}.ModelVersion.
     * 
     * @param dataSource connection with write permission to tap_schema tables
     * @param database database name (should be null if not needed in SQL)
     * @param schema schema name (usually tap_schema)
     * @param extras add tap_schema content for extra tables (materialised views)
     */
    public InitCaomTapSchemaContent(DataSource dataSource, String database, String schema, boolean extras) {
        // use MODELVERSION/extras so changing extras will cause a recreate
        // eg 1.2.13/false <-> 1.2.13/true
        super(dataSource, database, schema, MODEL_NAME, MODEL_VERSION + "/" + extras);
        String[] src = BASE_SQL;
        if (extras) {
            src = BASE_EXTRA_SQL;
        }
        for (String s : src) {
            createSQL.add(s);
            upgradeSQL.add(s);
        }
    }
    
    @Override
    protected URL findSQL(String fname) {
        return InitCaomTapSchemaContent.class.getClassLoader().getResource("sql/" + fname);
    }
}
