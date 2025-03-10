/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2025.                            (c) 2025.
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

package org.opencadc.caom2.db.mappers;

import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Artifact;
import org.opencadc.caom2.ReleaseType;
import org.opencadc.caom2.db.SQLDialect;
import org.opencadc.caom2.db.Util;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.vocab.DataLinkSemantics;

/**
 *
 * @author pdowler
 */
public class ArtifactMapper implements PartialRowMapper<Artifact> {

    private static final Logger log = Logger.getLogger(ArtifactMapper.class);

    public static final String[] COLUMNS = new String[]{
        "planeID", // FK
        "uri", "uriBucket",
        "productType", "releaseType",
        "contentType", "contentLength", "contentChecksum",
        "contentRelease", "contentReadGroups",
        "descriptionID",
        "lastModified", "maxLastModified",
        "metaChecksum", "accMetaChecksum", "metaProducer",
        "artifactID" // PK
    };

    private final SQLDialect dbDialect;
    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    public ArtifactMapper(SQLDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    @Override
    public UUID getID(ResultSet rs, int row, int offset) throws SQLException {
        int n = getColumnCount() - 1;
        UUID id = Util.getUUID(rs, offset + n);
        log.debug("found: entity ID = " + id);
        return id;
    }

    @Override
    public int getColumnCount() {
        return COLUMNS.length;
    }

    public Artifact mapRow(ResultSet rs, int row)
        throws SQLException {
        return mapRow(rs, row, 1);
    }

    /**
     * Map columns from the current row into an Artifact, starting at the
     * specified column offset.
     *
     * @param rs
     * @param row
     * @param col JDBC column offset where plane columns are located
     * @return
     * @throws java.sql.SQLException
     */
    public Artifact mapRow(ResultSet rs, int row, int col)
        throws SQLException {
        UUID planeID = Util.getUUID(rs, col++); // FK
        if (planeID == null) {
            return null;
        }

        URI uri = Util.getURI(rs, col++);
        log.debug("found a.uri = " + uri);
        String uriBucket = rs.getString(col++); // unused

        String pt = rs.getString(col++);
        log.debug("found a.productType = " + pt);
        DataLinkSemantics ptype = DataLinkSemantics.toValue(pt);

        String rt = rs.getString(col++);
        log.debug("found a.releaseType = " + rt);
        ReleaseType rtype = ReleaseType.toValue(rt);

        Artifact a = new Artifact(uri, ptype, rtype);

        a.contentType = rs.getString(col++);
        log.debug("found a.contentType = " + a.contentType);
        a.contentLength = Util.getLong(rs, col++);
        log.debug("found a.contentLength = " + a.contentLength);
        a.contentChecksum = Util.getURI(rs, col++);
        log.debug("found a.contentChecksum = " + a.contentChecksum);
        a.contentRelease = Util.getDate(rs, col++, utcCalendar);
        log.debug("found a.contentRelease = " + a.contentRelease);
        dbDialect.extractMultiURI(rs, col++, a.getContentReadGroups());
        log.debug("found a.contentReadGrouops: " + a.getContentReadGroups().size());
        a.descriptionID = Util.getURI(rs, col++);
        log.debug("a.descriptionID: " + a.descriptionID);

        Date lastModified = Util.getDate(rs, col++, utcCalendar);
        Date maxLastModified = Util.getDate(rs, col++, utcCalendar);
        CaomUtil.assignLastModified(a, lastModified, "lastModified");
        CaomUtil.assignLastModified(a, maxLastModified, "maxLastModified");

        URI metaChecksum = Util.getURI(rs, col++);
        URI accMetaChecksum = Util.getURI(rs, col++);
        CaomUtil.assignMetaChecksum(a, metaChecksum, "metaChecksum");
        CaomUtil.assignMetaChecksum(a, accMetaChecksum, "accMetaChecksum");
        a.metaProducer = Util.getURI(rs, col++);

        UUID id = Util.getUUID(rs, col++);
        log.debug("found artifact.id = " + id);
        CaomUtil.assignID(a, id);

        return a;
    }
}
