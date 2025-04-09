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

import ca.nrc.cadc.dali.Point;
import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Algorithm;
import org.opencadc.caom2.DerivedObservation;
import org.opencadc.caom2.Environment;
import org.opencadc.caom2.Instrument;
import org.opencadc.caom2.Observation;
import org.opencadc.caom2.ObservationIntentType;
import org.opencadc.caom2.Proposal;
import org.opencadc.caom2.Requirements;
import org.opencadc.caom2.SimpleObservation;
import org.opencadc.caom2.Status;
import org.opencadc.caom2.Target;
import org.opencadc.caom2.TargetPosition;
import org.opencadc.caom2.TargetType;
import org.opencadc.caom2.Telescope;
import org.opencadc.caom2.db.SQLDialect;
import org.opencadc.caom2.db.SQLGenerator;
import org.opencadc.caom2.db.Util;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.vocab.Tracking;

/**
 *
 * @author pdowler
 */
public class ObservationMapper implements PartialRowMapper<Observation> {

    private static final Logger log = Logger.getLogger(ObservationMapper.class);

    public static final String[] COLUMNS = new String[] {
        "typeCode",
        "uri", "uriBucket", "collection",
        "algorithm_name",
        "obstype", "intent", "sequenceNumber", "metaRelease", "metaReadGroups",
        "proposal_id", "proposal_pi", "proposal_project", "proposal_title",
        "proposal_keywords", "proposal_reference",
        "target_name", "target_targetID", "target_type", "target_standard",
        "target_redshift", "target_moving", "target_keywords",
        "targetPosition_coordsys", "targetPosition_coordinates", "targetPosition_equinox",
        "telescope_name", "telescope_geoLocationX", "telescope_geoLocationY", "telescope_geoLocationZ",
        "telescope_keywords", "telescope_trackingMode",
        "instrument_name", "instrument_keywords",
        "environment_seeing", "environment_humidity", "environment_elevation",
        "environment_tau", "environment_wavelengthTau", "environment_ambientTemp",
        "environment_photometric",
        "requirements_flag",
        "members",
        "lastModified", "maxLastModified",
        "metaChecksum", "accMetaChecksum", "metaProducer",
        "obsID" // PK
    };
    
    public static final String[] OPT_COLUMNS =  new String[]{
        "_q_targetPosition_coordinates"
    };

    private final SQLDialect dbDialect;
    private final boolean persistOptimisations;
    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    public ObservationMapper(SQLDialect dbDialect, boolean persistOptimisations) {
        this.dbDialect = dbDialect;
        this.persistOptimisations = persistOptimisations;
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
        if (persistOptimisations) {
            return COLUMNS.length + OPT_COLUMNS.length;
        }
        return COLUMNS.length;
    }

    public Observation mapRow(ResultSet rs, int row)
        throws SQLException {
        return mapRow(rs, row, 1);
    }

    /**
     * Map columns from the current row into an Observation, starting at the
     * specified column offset.
     *
     * @param rs
     * @param row
     * @param col JDBC column offset where observation columns are located
     * @return
     * @throws java.sql.SQLException
     */
    public Observation mapRow(ResultSet rs, int row, int col)
        throws SQLException {
        // first column is a constant that dictates the type
        String typeCode = rs.getString(col++);
        if (typeCode == null) {
            return null;
        }

        URI uri = Util.getURI(rs, col++);
        String uriBucket = rs.getString(col++); // unused
        String collection = rs.getString(col++);
        Algorithm algorithm = new Algorithm(rs.getString(col++));
        log.debug("found: algorithm = " + algorithm);

        Observation o = null;
        if (SQLGenerator.SIMPLE_TYPE.equals(typeCode)) {
            o = new SimpleObservation(collection, uri, algorithm);
        } else if (SQLGenerator.DERIVED_TYPE.equals(typeCode)) {
            o = new DerivedObservation(collection, uri, algorithm);
        } else {
            throw new RuntimeException("BUG: unexpected observation.typeCode " + typeCode);
        }

        o.type = rs.getString(col++);
        String intent = rs.getString(col++);
        log.debug("found: intent = " + intent);
        if (intent != null) {
            o.intent = ObservationIntentType.toValue(intent);
        }

        o.sequenceNumber = Util.getInteger(rs, col++);
        o.metaRelease = Util.getDate(rs, col++, utcCalendar);
        log.debug("found metaRelease: " + o.metaRelease);
        dbDialect.extractMultiURI(rs, col++, o.getMetaReadGroups());
        log.debug("found metaReadGroups: " + o.getMetaReadGroups().size());

        String pid = rs.getString(col++);
        if (pid != null) {
            o.proposal = new Proposal(pid);
            o.proposal.pi = rs.getString(col++);
            o.proposal.project = rs.getString(col++);
            o.proposal.title = rs.getString(col++);
            dbDialect.getKeywords(rs, col++, o.proposal.getKeywords());
            o.proposal.reference = Util.getURI(rs, col++);

        } else {
            col += 6; // skip
        }
        log.debug("found proposal: " + o.proposal);

        String targ = rs.getString(col++);
        if (targ != null) {
            o.target = new Target(targ);
            o.target.targetID = Util.getURI(rs, col++);
            String tt = rs.getString(col++);
            if (tt != null) {
                o.target.type = TargetType.toValue(tt);
            }
            o.target.standard = Util.getBoolean(rs, col++);
            o.target.redshift = Util.getDouble(rs, col++);
            o.target.moving = Util.getBoolean(rs, col++);
            dbDialect.getKeywords(rs, col++, o.target.getKeywords());
        } else {
            col += 6; // skip
        }
        log.debug("found target: " + o.target);

        String tposCs = rs.getString(col++);
        if (tposCs != null) {
            Point tpos = dbDialect.getPoint(rs, col++);
            o.targetPosition = new TargetPosition(tposCs, tpos);
            o.targetPosition.equinox = Util.getDouble(rs, col++);
        } else {
            col += 2; // skip
        }
        log.debug("found targetPosition: " + o.targetPosition);

        String tn = rs.getString(col++);
        if (tn != null) {
            o.telescope = new Telescope(tn);
            o.telescope.geoLocationX = Util.getDouble(rs, col++);
            o.telescope.geoLocationY = Util.getDouble(rs, col++);
            o.telescope.geoLocationZ = Util.getDouble(rs, col++);
            dbDialect.getKeywords(rs, col++, o.telescope.getKeywords());
            String tm = rs.getString(col++);
            if (tm != null) {
                o.telescope.trackingMode = Tracking.toValue(tm);
            }
        } else {
            col += 4; // skip
        }
        log.debug("found telescope: " + o.telescope);

        String in = rs.getString(col++);
        if (in != null) {
            o.instrument = new Instrument(in);
            dbDialect.getKeywords(rs, col++, o.instrument.getKeywords());
        } else {
            col += 1; // skip
        }
        log.debug("found instrument: " + o.instrument);

        Environment e = new Environment();
        e.seeing = Util.getDouble(rs, col++);
        e.humidity = Util.getDouble(rs, col++);
        e.elevation = Util.getDouble(rs, col++);
        e.tau = Util.getDouble(rs, col++);
        e.wavelengthTau = Util.getDouble(rs, col++);
        e.ambientTemp = Util.getDouble(rs, col++);
        e.photometric = Util.getBoolean(rs, col++);

        if (e.seeing != null || e.humidity != null || e.elevation != null
            || e.tau != null || e.wavelengthTau != null || e.ambientTemp != null
            || e.photometric != null) {
            o.environment = e;
        }
        log.debug("found environment: " + o.environment);

        String rflag = rs.getString(col++);
        if (rflag != null) {
            o.requirements = new Requirements(Status.toValue(rflag));
        }
        log.debug("found requirements: " + o.requirements);

        if (o instanceof DerivedObservation) {
            DerivedObservation der = (DerivedObservation) o;
            dbDialect.extractMultiURI(rs, col++, der.getMembers());
            log.debug("found members: " + der.getMembers().size());
        } else {
            col += 1; // skip
        }

        if (persistOptimisations) {
            col += OPT_COLUMNS.length;
        }
        Date lastModified = Util.getDate(rs, col++, utcCalendar);
        Date maxLastModified = Util.getDate(rs, col++, utcCalendar);
        CaomUtil.assignLastModified(o, lastModified, "lastModified");
        CaomUtil.assignLastModified(o, maxLastModified, "maxLastModified");

        URI metaChecksum = Util.getURI(rs, col++);
        URI accMetaChecksum = Util.getURI(rs, col++);
        CaomUtil.assignMetaChecksum(o, metaChecksum, "metaChecksum");
        CaomUtil.assignMetaChecksum(o, accMetaChecksum, "accMetaChecksum");
        o.metaProducer = Util.getURI(rs, col++);

        UUID id = Util.getUUID(rs, col++);
        log.debug("found: observation.id = " + id);
        CaomUtil.assignID(o, id);

        return o;
    }

}
