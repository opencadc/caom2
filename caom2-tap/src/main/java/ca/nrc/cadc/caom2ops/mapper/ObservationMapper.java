/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.caom2.Algorithm;
import ca.nrc.cadc.caom2.DerivedObservation;
import ca.nrc.cadc.caom2.Environment;
import ca.nrc.cadc.caom2.Instrument;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationIntentType;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.Requirements;
import ca.nrc.cadc.caom2.SimpleObservation;
import ca.nrc.cadc.caom2.Status;
import ca.nrc.cadc.caom2.Target;
import ca.nrc.cadc.caom2.TargetPosition;
import ca.nrc.cadc.caom2.TargetType;
import ca.nrc.cadc.caom2.Telescope;
import ca.nrc.cadc.caom2.types.Point;
import ca.nrc.cadc.caom2ops.Util;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class ObservationMapper implements VOTableRowMapper<Observation> {

    private static final Logger log = Logger.getLogger(ObservationMapper.class);

    private Map<String, Integer> map;

    public ObservationMapper(Map<String, Integer> map) {
        this.map = map;
    }

    public Observation mapRow(List<Object> data, DateFormat dateFormat) {
        log.debug("mapping Observation");
        UUID id = Util.getUUID(data, map.get("caom2:Observation.id"));
        if (id == null) {
            return null;
        }

        try {
            String collection = Util.getString(data, map.get("caom2:Observation.collection"));
            String observationID = Util.getString(data, map.get("caom2:Observation.observationID"));
            String algName = Util.getString(data, map.get("caom2:Observation.algorithm.name"));
            String mem = Util.getString(data, map.get("caom2:Observation.members"));
            String typeCode = Util.getString(data, map.get("caom2:Observation.typeCode"));
            Observation obs;
            if ("C".equals(typeCode)) // used to be mem != null ... also typeCode may change to D in CAOM-2.4
            {
                DerivedObservation co = new DerivedObservation(collection, observationID, new Algorithm(algName));
                Util.decodeObservationURIs(mem, co.getMembers());
                obs = co;
            } else {
                obs = new SimpleObservation(collection, observationID);
            }

            obs.type = Util.getString(data, map.get("caom2:Observation.type"));
            String intentStr = Util.getString(data, map.get("caom2:Observation.intent"));
            if (intentStr != null) {
                obs.intent = ObservationIntentType.toValue(intentStr);
            }
            obs.sequenceNumber = Util.getInteger(data, map.get("caom2:Observation.sequenceNumber"));
            obs.metaRelease = Util.getDate(data, map.get("caom2:Observation.metaRelease"));
            // TODO: fill Observation.metaReadGroups // CAOM-2.4

            String proposalID = Util.getString(data, map.get("caom2:Observation.proposal.id"));
            if (proposalID != null) {
                obs.proposal = new Proposal(proposalID);
                obs.proposal.pi = Util.getString(data, map.get("caom2:Observation.proposal.pi"));
                obs.proposal.project = Util.getString(data, map.get("caom2:Observation.proposal.project"));
                obs.proposal.title = Util.getString(data, map.get("caom2:Observation.proposal.title"));
                Util.decodeKeywordList(Util.getString(data, map.get("caom2:Observation.proposal.keywords")), obs.proposal.getKeywords());
            }

            String targetName = Util.getString(data, map.get("caom2:Observation.target.name"));
            if (targetName != null) {
                obs.target = new Target(targetName);
                obs.target.targetID = Util.getURI(data, map.get("caom2:Observation.target.targetID")); // CAOM-2.4
                obs.target.moving = Util.getBoolean(data, map.get("caom2:Observation.target.moving"));
                obs.target.redshift = Util.getDouble(data, map.get("caom2:Observation.target.redshift"));
                obs.target.standard = Util.getBoolean(data, map.get("caom2:Observation.target.standard"));
                String tType = Util.getString(data, map.get("caom2:Observation.target.type"));
                if (tType != null) {
                    obs.target.type = TargetType.toValue(tType);
                }
                Util.decodeKeywordList(Util.getString(data, map.get("caom2:Observation.target.keywords")), obs.target.getKeywords());
            }

            String tpcsys = Util.getString(data, map.get("caom2:Observation.targetPosition.coordsys"));
            if (tpcsys != null) {
                Double tpc1 = Util.getDouble(data, map.get("caom2:Observation.targetPosition.coordinates.cval1"));
                Double tpc2 = Util.getDouble(data, map.get("caom2:Observation.targetPosition.coordinates.cval2"));
                obs.targetPosition = new TargetPosition(tpcsys, new Point(tpc1, tpc2));
                obs.targetPosition.equinox = Util.getDouble(data, map.get("caom2:Observation.targetPosition.equinox"));
            }

            String telName = Util.getString(data, map.get("caom2:Observation.telescope.name"));
            if (telName != null) {
                obs.telescope = new Telescope(telName);
                obs.telescope.geoLocationX = Util.getDouble(data, map.get("caom2:Observation.telescope.geoLocationX"));
                obs.telescope.geoLocationY = Util.getDouble(data, map.get("caom2:Observation.telescope.geoLocationY"));
                obs.telescope.geoLocationZ = Util.getDouble(data, map.get("caom2:Observation.telescope.geoLocationZ"));
                Util.decodeKeywordList(Util.getString(data, map.get("caom2:Observation.telescope.keywords")), obs.telescope.getKeywords());
            }

            String instName = Util.getString(data, map.get("caom2:Observation.instrument.name"));
            if (instName != null) {
                obs.instrument = new Instrument(instName);
                Util.decodeKeywordList(Util.getString(data, map.get("caom2:Observation.instrument.keywords")), obs.instrument.getKeywords());
            }

            String reqFlag = Util.getString(data, map.get("caom2:Observation.requirements.flag"));
            if (reqFlag != null) {
                obs.requirements = new Requirements(Status.toValue(reqFlag));
            }
            obs.environment = new Environment();
            obs.environment.ambientTemp = Util.getDouble(data, map.get("caom2:Observation.environment.ambientTemp"));
            obs.environment.elevation = Util.getDouble(data, map.get("caom2:Observation.environment.elevation"));
            obs.environment.humidity = Util.getDouble(data, map.get("caom2:Observation.environment.humidity"));
            obs.environment.photometric = Util.getBoolean(data, map.get("caom2:Observation.environment.photometric"));
            obs.environment.seeing = Util.getDouble(data, map.get("caom2:Observation.environment.seeing"));
            obs.environment.tau = Util.getDouble(data, map.get("caom2:Observation.environment.tau"));
            obs.environment.wavelengthTau = Util.getDouble(data, map.get("caom2:Observation.environment.wavelengthTau"));

            Date lastModified = Util.getDate(data, map.get("caom2:Observation.lastModified"));
            Date maxLastModified = Util.getDate(data, map.get("caom2:Observation.maxLastModified"));
            Util.assignLastModified(obs, lastModified, "lastModified");
            Util.assignLastModified(obs, maxLastModified, "maxLastModified");

            URI metaChecksum = Util.getURI(data, map.get("caom2:Observation.metaChecksum"));
            URI accMetaChecksum = Util.getURI(data, map.get("caom2:Observation.accMetaChecksum"));
            Util.assignMetaChecksum(obs, metaChecksum, "metaChecksum");
            Util.assignMetaChecksum(obs, accMetaChecksum, "accMetaChecksum");

            Util.assignID(obs, id);

            return obs;
        } catch (URISyntaxException ex) {
            throw new UnexpectedContentException("invalid URI", ex);
        } finally {
        }
    }

}
