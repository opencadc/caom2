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
 *  $Revision: 5 $
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
import org.opencadc.caom2.db.Util;
import org.opencadc.caom2.db.skel.ArtifactSkeleton;
import org.opencadc.caom2.db.skel.ChunkSkeleton;
import org.opencadc.caom2.db.skel.ObservationSkeleton;
import org.opencadc.caom2.db.skel.PartSkeleton;
import org.opencadc.caom2.db.skel.PlaneSkeleton;
import org.opencadc.caom2.db.skel.Skeleton;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class ObservationSkeletonExtractor implements ResultSetExtractor {

    private static final Logger log = Logger.getLogger(ObservationSkeletonExtractor.class);

    // lastModified,id,... for obs,plane,artifact,part,chunk
    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    // extract from join
    @Override
    public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
        ObservationSkeleton ret = null;
        PlaneSkeleton curPlane = null;
        ArtifactSkeleton curArtifact = null;
        PartSkeleton curPart = null;
        ChunkSkeleton curChunk = null;
        int ncol = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            if (ret == null) {
                ret = new ObservationSkeleton();
            }

            Date d;
            Date md;
            UUID id;
            URI cs;
            URI acs;
            int col = 1;

            if (ret.id == null) {
                d = Util.getDate(rs, col++, utcCalendar);
                md = Util.getDate(rs, col++, utcCalendar);
                cs = Util.getURI(rs, col++);
                acs = Util.getURI(rs, col++);
                id = Util.getUUID(rs, col++);
                ret.id = id;
                ret.lastModified = d;
                ret.maxLastModified = md;
                ret.metaChecksum = cs;
                ret.accMetaChecksum = acs;
            } else {
                col += 5; // skip
            }
            if (ncol > col) {
                // plane
                col++; // skip FK
                d = Util.getDate(rs, col++, utcCalendar);
                md = Util.getDate(rs, col++, utcCalendar);
                cs = Util.getURI(rs, col++);
                acs = Util.getURI(rs, col++);
                id = Util.getUUID(rs, col++);
                if (id != null) {
                    if (curPlane == null || !curPlane.id.equals(id)) {
                        curPlane = new PlaneSkeleton();
                        curPlane.id = id;
                        curPlane.lastModified = d;
                        curPlane.maxLastModified = md;
                        curPlane.metaChecksum = cs;
                        curPlane.accMetaChecksum = acs;
                        log.debug("add: " + curPlane + " to " + ret);
                        ret.planes.add(curPlane);
                    }
                    if (ncol > col) {
                        // artifact
                        col++; // skip FK
                        d = Util.getDate(rs, col++, utcCalendar);
                        md = Util.getDate(rs, col++, utcCalendar);
                        cs = Util.getURI(rs, col++);
                        acs = Util.getURI(rs, col++);
                        id = Util.getUUID(rs, col++);
                        if (id != null) {
                            if (curArtifact == null || !curArtifact.id.equals(id)) {
                                curArtifact = new ArtifactSkeleton();
                                curArtifact.id = id;
                                curArtifact.lastModified = d;
                                curArtifact.maxLastModified = md;
                                curArtifact.metaChecksum = cs;
                                curArtifact.accMetaChecksum = acs;
                                log.debug("add: " + curArtifact + " to " + curPlane);
                                curPlane.artifacts.add(curArtifact);
                            }
                            if (ncol > col) {
                                // part
                                col++; // skip FK
                                d = Util.getDate(rs, col++, utcCalendar);
                                md = Util.getDate(rs, col++, utcCalendar);
                                cs = Util.getURI(rs, col++);
                                acs = Util.getURI(rs, col++);
                                id = Util.getUUID(rs, col++);
                                if (id != null) {
                                    if (curPart == null || !curPart.id.equals(id)) {
                                        curPart = new PartSkeleton();
                                        curPart.id = id;
                                        curPart.lastModified = d;
                                        curPart.maxLastModified = md;
                                        curPart.metaChecksum = cs;
                                        curPart.accMetaChecksum = acs;
                                        log.debug("add: " + curPart + " to " + curArtifact);
                                        curArtifact.parts.add(curPart);
                                    }
                                    if (ncol > col) {
                                        // chunk
                                        col++; // skip FK
                                        d = Util.getDate(rs, col++, utcCalendar);
                                        md = Util.getDate(rs, col++, utcCalendar);
                                        cs = Util.getURI(rs, col++);
                                        acs = Util.getURI(rs, col++);
                                        id = Util.getUUID(rs, col++);
                                        if (id != null) {
                                            curChunk = new ChunkSkeleton();
                                            curChunk.id = id;
                                            curChunk.lastModified = d;
                                            curChunk.maxLastModified = md;
                                            curChunk.metaChecksum = cs;
                                            curChunk.accMetaChecksum = acs;
                                            log.debug("add: " + curChunk + " to " + curPart);
                                            curPart.chunks.add(curChunk);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return ret;
    }

    public void extract(Skeleton ret, ResultSet rs, int col) throws SQLException {
        ret.lastModified = Util.getDate(rs, col++, utcCalendar);
        ret.maxLastModified = Util.getDate(rs, col++, utcCalendar);
        ret.metaChecksum = Util.getURI(rs, col++);
        ret.accMetaChecksum = Util.getURI(rs, col++);
        ret.id = Util.getUUID(rs, col++);
    }
    
    public final RowMapper<ObservationSkeleton> observationMapper = new RowMapper<ObservationSkeleton>() {
        @Override
        public ObservationSkeleton mapRow(ResultSet rs, int i) throws SQLException {
            ObservationSkeleton ret = new ObservationSkeleton();
            extract(ret, rs, 1);
            return ret;
        }
    };

    public final RowMapper<PlaneSkeleton> planeMapper = new RowMapper<PlaneSkeleton>() {
        @Override
        public PlaneSkeleton mapRow(ResultSet rs, int i) throws SQLException {
            PlaneSkeleton ret = new PlaneSkeleton();
            extract(ret, rs, 2); // skip FK
            return ret;
        }
    };

    public final RowMapper<ArtifactSkeleton> artifactMapper = new RowMapper<ArtifactSkeleton>() {
        @Override
        public ArtifactSkeleton mapRow(ResultSet rs, int i) throws SQLException {
            ArtifactSkeleton ret = new ArtifactSkeleton();
            extract(ret, rs, 2); // skip FK
            return ret;
        }
    };

    public final RowMapper<PartSkeleton> partMapper = new RowMapper<PartSkeleton>() {
        @Override
        public PartSkeleton mapRow(ResultSet rs, int i) throws SQLException {
            PartSkeleton ret = new PartSkeleton();
            extract(ret, rs, 2); // skip FK
            return ret;
        }
    };

    public final RowMapper<ChunkSkeleton> chunkMapper = new RowMapper<ChunkSkeleton>() {
        @Override
        public ChunkSkeleton mapRow(ResultSet rs, int i) throws SQLException {
            ChunkSkeleton ret = new ChunkSkeleton();
            extract(ret, rs, 2); // skip FK
            return ret;
        }
    };
}
