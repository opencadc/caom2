/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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

package org.opencadc.caom2.metasync;

import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.PostgreSQLGenerator;
import ca.nrc.cadc.caom2.persistence.SQLGenerator;
import ca.nrc.cadc.date.DateUtil;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public abstract class Harvester implements Runnable {

    private static final Logger log = Logger.getLogger(Harvester.class);

    public static final String POSTGRESQL = "postgresql";

    protected String source;
    protected String cname;
    protected Class entityClass;
    protected int batchSize;
    protected boolean full;
    protected Date minDate;
    protected Date maxDate;
    protected String collection;
    protected HarvesterResource src;
    protected HarvesterResource dest;
    protected HarvestStateDAO harvestStateDAO;

    protected Harvester() {
    }

    protected Harvester(Class entityClass, HarvesterResource src, HarvesterResource dest, String collection,
                        Integer batchSize, boolean full) {
        this.entityClass = entityClass;
        this.src = src;
        this.dest = dest;
        this.collection = collection;
        this.batchSize = batchSize;
        this.full = full;
    }

    public void setMinDate(Date d) {
        this.minDate = d;
    }
    
    public void setMaxDate(Date d) {
        this.maxDate = d;
    }

    protected Map<String, Object> getConfigDAO(HarvesterResource harvestResource) {
        Map<String, Object> ret = new HashMap<>();
        if (harvestResource.getJdbcUrl().contains(POSTGRESQL)) {
            ret.put(SQLGenerator.class.getName(), PostgreSQLGenerator.class);
            ret.put("disableHashJoin", Boolean.TRUE);
        } else {
            throw new IllegalArgumentException("unknown SQL dialect: " + harvestResource.getDatabaseServer());
        }
        ret.put("server", harvestResource.getDatabaseServer());
        ret.put("database", harvestResource.getDatabase());
        ret.put("schema", harvestResource.getSchema());
        return ret;
    }

    /**
     * @param ds
     * DataSource from the destination DAO class
     * @param c
     * class being persisted via the destination DAO class
     */
    protected void initHarvestState(DataSource ds, Class c) {
        this.cname = c.getSimpleName();

        log.debug("creating HarvestState tracker: " + cname + " in " + dest.getDatabase() + "." + dest.getSchema());
        this.harvestStateDAO = new PostgresqlHarvestStateDAO(ds, dest.getDatabase(), dest.getSchema());

        log.debug("creating HarvestSkip tracker: " + cname + " in " + dest.getDatabase() + "." + dest.getSchema());

        this.source = src.getIdentifier(collection);
    }

    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

    protected String format(Date d) {
        if (d == null) {
            return "null";
        }
        return df.format(d);
    }
}
