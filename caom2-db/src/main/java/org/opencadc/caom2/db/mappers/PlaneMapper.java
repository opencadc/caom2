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

import ca.nrc.cadc.dali.Interval;
import ca.nrc.cadc.dali.MultiShape;
import ca.nrc.cadc.dali.Shape;
import ca.nrc.cadc.date.DateUtil;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.CalibrationLevel;
import org.opencadc.caom2.CustomAxis;
import org.opencadc.caom2.DataQuality;
import org.opencadc.caom2.Energy;
import org.opencadc.caom2.EnergyTransition;
import org.opencadc.caom2.Metrics;
import org.opencadc.caom2.Observable;
import org.opencadc.caom2.Plane;
import org.opencadc.caom2.Polarization;
import org.opencadc.caom2.Position;
import org.opencadc.caom2.Provenance;
import org.opencadc.caom2.Quality;
import org.opencadc.caom2.Time;
import org.opencadc.caom2.Visibility;
import org.opencadc.caom2.db.SQLDialect;
import org.opencadc.caom2.db.Util;
import org.opencadc.caom2.util.CaomUtil;
import org.opencadc.caom2.vocab.CalibrationStatus;
import org.opencadc.caom2.vocab.DataProductType;
import org.opencadc.caom2.vocab.UCD;

/**
 *
 * @author pdowler
 */
public class PlaneMapper implements PartialRowMapper<Plane> {

    private static final Logger log = Logger.getLogger(PlaneMapper.class);

    public static final String[] COLUMNS = new String[]{
        "obsID", // FK
        "uri",
        "publisherID",
        "metaRelease", "metaReadGroups",
        "dataRelease", "dataReadGroups",
        "dataProductType",
        "calibrationLevel",
        "provenance_name", "provenance_reference", "provenance_version", "provenance_project",
        "provenance_producer", "provenance_runID", "provenance_lastExecuted",
        "provenance_keywords", "provenance_inputs",
        "metrics_sourceNumberDensity", "metrics_background", "metrics_backgroundStddev",
        "metrics_fluxDensityLimit", "metrics_magLimit", "metrics_sampleSNR",
        "quality_flag",
        "observable_ucd", "observable_calibration",
        "position_bounds", "position_samples", "position_minBounds",
        "position_dimension",
        "position_maxRecoverableScale",
        "position_resolution", "position_resolutionBounds",
        "position_sampleSize",
        "position_calibration",
        "energy_bounds", "energy_samples",
        "energy_bandpassName", "energy_energyBands",
        "energy_dimension",
        "energy_resolvingPower", "energy_resolvingPowerBounds",
        "energy_resolution", "energy_resolutionBounds",
        "energy_sampleSize",
        "energy_transition_species", "energy_transition_transition",
        "energy_rest",
        "energy_calibration",
        "time_bounds", "time_samples",
        "time_dimension",
        "time_exposure", "time_exposureBounds",
        "time_resolution", "time_resolutionBounds",
        "time_sampleSize",
        "time_calibration",
        "polarization_states", "polarization_dimension",
        "custom_ctype", "custom_bounds", "custom_samples", "custom_dimension",
        "uv_distance", "uv_distributionEccentricity", "uv_distributionFill",
        "lastModified", "maxLastModified",
        "metaChecksum", "accMetaChecksum", "metaProducer",
        "planeID" // PK
    };

    public static final String[] OPT_COLUMNS = new String[] {
        "_q_position_bounds",
        "_q_position_bounds_centroid", "_q_position_bounds_area", "_q_position_bounds_size",
        "_q_position_minBounds",
        "_q_position_minBounds_centroid", "_q_position_minBounds_area", "_q_position_minBounds_size",
        "_q_position_maxRecoverableScale","_q_position_resolutionBounds",
        "_q_energy_bounds", 
        "_q_energy_samples", 
        "_q_energy_resolvingPowerBounds",
        "_q_energy_resolutionBounds",
        "_q_time_bounds", 
        "_q_time_samples", 
        "_q_time_exposureBounds", 
        "_q_time_resolutionBounds",
        "_q_custom_bounds", 
        "_q_custom_samples",
        "_q_uv_distance",
    };

    private final SQLDialect dbDialect;
    private final boolean persistOptimisations;
    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);

    public PlaneMapper(SQLDialect dbDialect, boolean persistOptimisations) {
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

    public Plane mapRow(ResultSet rs, int row)
        throws SQLException {
        return mapRow(rs, row, 1);
    }

    /**
     * Map columns from the current row into a Plane, starting at the
     * specified column offset.
     *
     * @param rs
     * @param row
     * @param col JDBC column offset where plane columns are located
     * @return
     * @throws java.sql.SQLException
     */
    public Plane mapRow(ResultSet rs, int row, int col)
        throws SQLException {
        UUID obsID = Util.getUUID(rs, col++); // FK
        if (obsID == null) {
            return null;
        }

        URI uri = Util.getURI(rs, col++);
        if (uri == null) {
            return null;
        }
        log.debug("found p.uri = " + uri);

        Plane p = new Plane(uri);
        p.publisherID = Util.getURI(rs, col++);

        p.metaRelease = Util.getDate(rs, col++, utcCalendar);
        log.debug("found p.metaRelease = " + p.metaRelease);
        dbDialect.extractMultiURI(rs, col++, p.getMetaReadGroups());
        p.dataRelease = Util.getDate(rs, col++, utcCalendar);
        log.debug("found p.dataRelease = " + p.dataRelease);
        dbDialect.extractMultiURI(rs, col++, p.getDataReadGroups());

        String dpt = rs.getString(col++);
        log.debug("found p.dataProductType = " + dpt);
        if (dpt != null) {
            p.dataProductType = DataProductType.toValue(dpt);
        }

        Integer cl = Util.getInteger(rs, col++);
        log.debug("found p.calibrationLevel = " + cl);
        if (cl != null) {
            p.calibrationLevel = CalibrationLevel.toValue(cl.intValue());
        }

        String pname = rs.getString(col++);
        log.debug("found p.provenance.name = " + pname);
        if (pname != null) {
            p.provenance = new Provenance(pname);
            p.provenance.reference = Util.getURI(rs, col++);
            log.debug("found p.provenance.reference = " + p.provenance.reference);
            p.provenance.version = rs.getString(col++);
            log.debug("found p.provenance.version = " + p.provenance.version);
            p.provenance.project = rs.getString(col++);
            log.debug("found p.provenance.project = " + p.provenance.project);
            p.provenance.producer = rs.getString(col++);
            log.debug("found p.provenance.producer = " + p.provenance.producer);
            p.provenance.runID = rs.getString(col++);
            log.debug("found p.provenance.runID = " + p.provenance.runID);
            p.provenance.lastExecuted = Util.getDate(rs, col++, utcCalendar);
            log.debug("found p.provenance.lastExecuted = " + p.provenance.lastExecuted);
            dbDialect.getKeywords(rs, col++, p.provenance.getKeywords());
            log.debug("found p.provenance.keywords: " + p.provenance.getKeywords().size());
            dbDialect.extractMultiURI(rs, col++, p.provenance.getInputs());
            log.debug("found p.provenance.inpts: " + p.provenance.getInputs().size());
        } else {
            col += 8;
        }

        Metrics m = new Metrics();
        m.sourceNumberDensity = Util.getDouble(rs, col++);
        m.background = Util.getDouble(rs, col++);
        m.backgroundStddev = Util.getDouble(rs, col++);
        m.fluxDensityLimit = Util.getDouble(rs, col++);
        m.magLimit = Util.getDouble(rs, col++);
        m.sampleSNR = Util.getDouble(rs, col++);
        if (m.sourceNumberDensity != null || m.background != null || m.backgroundStddev != null
            || m.fluxDensityLimit != null || m.magLimit != null || m.sampleSNR != null) {
            p.metrics = m;
        }

        String qflag = rs.getString(col++);
        if (qflag != null) {
            p.quality = new DataQuality(Quality.toValue(qflag));
        }

        // observable
        String oucd = rs.getString(col++);
        if (oucd != null) {
            p.observable = new Observable(new UCD(oucd));
            String cs = rs.getString(col++);
            if (cs != null) {
                p.observable.calibration = new CalibrationStatus(cs);
            }
        } else {
            col += 1;
        }

        // position
        Shape s = dbDialect.getShape(rs, col++);
        if (s != null) {
            MultiShape ms = dbDialect.getMultiShape(rs, col++);
            Position pos = new Position(s, ms);
            pos.minBounds = dbDialect.getShape(rs, col++);
            pos.dimension = dbDialect.getDimension(rs, col++);
            log.debug("position.dimension: " + pos.dimension);

            pos.maxRecoverableScale = dbDialect.getInterval(rs, col++);
            log.debug("position.maxRecoverableScale: " + pos.maxRecoverableScale);
            pos.resolution = Util.getDouble(rs, col++);
            log.debug("position.resolution: " + pos.resolution);
            pos.resolutionBounds = dbDialect.getInterval(rs, col++);
            log.debug("position.resolutionBounds: " + pos.resolutionBounds);
            pos.sampleSize = Util.getDouble(rs, col++);
            log.debug("position_sampleSize: " + pos.sampleSize);
            String cs = rs.getString(col++);
            log.debug("position_calibration: " + cs);
            if (cs != null) {
                pos.calibration = new CalibrationStatus(cs);
            }
            p.position = pos;
        } else {
            col += 8;
        }

        // energy
        Interval<Double> eb = dbDialect.getInterval(rs, col++);
        if (eb != null) {
            Energy nrg = new Energy(eb);
            log.debug("energy_bounds: " + eb);
            dbDialect.extractIntervalList(rs, col++, nrg.getSamples());
            nrg.bandpassName = rs.getString(col++);
            log.debug("energy.bandpassName: " + nrg.bandpassName);
            String emStr = rs.getString(col++);
            CaomUtil.decodeBands(emStr, nrg.getEnergyBands());
            log.debug("energy.energyBands: " + nrg.getEnergyBands().size());
            nrg.dimension = Util.getLong(rs, col++);
            log.debug("energy.dimension: " + nrg.dimension);

            nrg.resolvingPower = Util.getDouble(rs, col++);
            log.debug("energy.resolvingPower: " + nrg.resolvingPower);
            nrg.resolvingPowerBounds = dbDialect.getInterval(rs, col++);
            log.debug("energy.resolvingPowerBounds: " + nrg.resolvingPowerBounds);

            nrg.resolution = Util.getDouble(rs, col++);
            log.debug("energy.resolution: " + nrg.resolution);
            nrg.resolutionBounds = dbDialect.getInterval(rs, col++);
            log.debug("energy.resolutionBounds: " + nrg.resolutionBounds);

            nrg.sampleSize = Util.getDouble(rs, col++);
            log.debug("energy.sampleSize: " + nrg.sampleSize);

            String ets = rs.getString(col++);
            String ett = rs.getString(col++);
            if (ets != null) {
                nrg.transition = new EnergyTransition(ets, ett);
            }
            log.debug("energy.transition: " + nrg.transition);

            nrg.rest = Util.getDouble(rs, col++);
            log.debug("energy.rest: " + nrg.rest);
            p.energy = nrg;

            String cs = rs.getString(col++);
            if (cs != null) {
                nrg.calibration = new CalibrationStatus(cs);
            }
        } else {
            col += 13;
        }

        // time
        Interval<Double> tb = dbDialect.getInterval(rs, col++);
        if (tb != null) {
            Time tim = new Time(tb);
            dbDialect.extractIntervalList(rs, col++, tim.getSamples());
            tim.dimension = Util.getLong(rs, col++);
            log.debug("time.dimension: " + tim.dimension);

            tim.exposure = Util.getDouble(rs, col++);
            log.debug("time.exposure: " + tim.exposure);
            tim.exposureBounds = dbDialect.getInterval(rs, col++);
            log.debug("time.exposureBounds: " + tim.exposureBounds);

            tim.resolution = Util.getDouble(rs, col++);
            log.debug("time.resolution: " + tim.resolution);
            tim.resolutionBounds = dbDialect.getInterval(rs, col++);
            log.debug("time.resolutionBounds: " + tim.resolutionBounds);

            tim.sampleSize = Util.getDouble(rs, col++);
            log.debug("time_sampleSize: " + tim.sampleSize);
            p.time = tim;

            String cs = rs.getString(col++);
            if (cs != null) {
                tim.calibration = new CalibrationStatus(cs);
            }
        } else {
            col += 8;
        }

        // polarization
        String polStr = rs.getString(col++);
        log.debug("polarization.states: " + polStr);
        if (polStr != null) {
            p.polarization = new Polarization();
            CaomUtil.decodeStates(polStr, p.polarization.getStates());
            p.polarization.dimension = Util.getInteger(rs, col++);
            log.debug("polarization.dimension: " + p.polarization.dimension);
        } else {
            col += 1;
        }

        // custom
        String cct = rs.getString(col++);
        if (cct != null) {
            log.debug("custom.ctype: " + cct);
            Interval<Double> cb = dbDialect.getInterval(rs, col++);
            p.custom = new CustomAxis(cct, cb);
            dbDialect.extractIntervalList(rs, col++, p.custom.getSamples());
            p.custom.dimension = Util.getLong(rs, col++);
            log.debug("custom.dimension: " + p.custom.dimension);
        } else {
            col += 3;
        }

        // visibility
        Interval<Double> uvd = dbDialect.getInterval(rs, col++);
        if (uvd != null) {
            log.debug("visibility.distance: " + uvd);
            Double ecc = Util.getDouble(rs, col++);
            log.debug("visibility.ecc: " + ecc);
            Double fill = Util.getDouble(rs, col++);
            log.debug("visibility.fill: " + fill);
            p.visibility = new Visibility(uvd, ecc, fill);
        } else {
            col += 2;
        }

        if (persistOptimisations) {
            col += OPT_COLUMNS.length;
        }

        Date lastModified = Util.getDate(rs, col++, utcCalendar);
        Date maxLastModified = Util.getDate(rs, col++, utcCalendar);
        CaomUtil.assignLastModified(p, lastModified, "lastModified");
        CaomUtil.assignLastModified(p, maxLastModified, "maxLastModified");

        URI metaChecksum = Util.getURI(rs, col++);
        URI accMetaChecksum = Util.getURI(rs, col++);
        CaomUtil.assignMetaChecksum(p, metaChecksum, "metaChecksum");
        CaomUtil.assignMetaChecksum(p, accMetaChecksum, "accMetaChecksum");
        p.metaProducer = Util.getURI(rs, col++);

        UUID id = Util.getUUID(rs, col++);
        log.debug("found: plane.id = " + id);
        CaomUtil.assignID(p, id);

        return p;
    }
}
