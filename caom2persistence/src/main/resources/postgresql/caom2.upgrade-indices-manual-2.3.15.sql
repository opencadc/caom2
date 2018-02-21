
-- to modify this file with a specific <schema>, try:
-- cat caom2.upgrade-indices-manual.sql | sed 's/<schema>/caom2/g' > doit.sql

-- recreate varchar indices with portable collate option and partial where it seems useful
drop index if exists <schema>.i_observationURI;
create unique index i_observationURI on <schema>.Observation (observationURI COLLATE "C")
;

drop index if exists <schema>.i_observationURI2;
create unique index i_observationURI2 on <schema>.Observation (collection COLLATE "C", observationID COLLATE "C")
;

drop index <schema>.Observation_i_observationID_lower;
create index Observation_i_observationID_lower
    on <schema>.Observation ( lower(observationID) COLLATE "C" )
;

drop index if exists <schema>.i_planeURI;
create unique index i_planeURI on <schema>.Plane(planeURI COLLATE "C")
;

drop index if exists <schema>.i_publisherID;
create unique index i_publisherID on <schema>.Plane(publisherID COLLATE "C")
;

drop index if exists <schema>.i_creatorID;
create unique index i_creatorID on <schema>.Plane(creatorID COLLATE "C")
    where creatorID is not null
;

drop index if exists <schema>.Plane_polar_is;
create index Plane_polar_is
    on <schema>.Plane (polarization_states COLLATE "C")
    where polarization_states is not null
;

drop index if exists <schema>.i_collection_instrument;
create index i_collection_instrument
    on <schema>.Observation (collection COLLATE "C", instrument_name COLLATE "C")
;

drop index if exists <schema>.i_telescope_instrument;
create index i_telescope_instrument
    on <schema>.Observation (telescope_name COLLATE "C", instrument_name COLLATE "C")
;

drop index if exists <schema>.Plane_i_emBand_dataProductType;
create index Plane_i_emBand_dataProductType
    on <schema>.Plane (energy_emBand COLLATE "C", dataProductType COLLATE "C")
;

drop index if exists <schema>.i_bandpassName;
create index i_bandpassName
    on <schema>.Plane (energy_bandpassName COLLATE "C")
    where energy_bandpassName is not null
;

drop index if exists <schema>.i_provenance_runid;
create index i_provenance_runid
    on <schema>.Plane (provenance_runID COLLATE "C")
    where provenance_runID is not null
;

drop index caom2.Observation_i_observationID_lower;
create index Observation_i_observationID_lower
    on <schema>.Observation ( lower(observationID)  COLLATE "C")
;

drop index if exists <schema>.Observation_i_targ_lower;
create index Observation_i_targ_lower
    on <schema>.Observation ( (lower(target_name))  COLLATE "C")
    where target_name is not null
;

drop index caom2.Observation_i_proposal_id_lower;
create index Observation_i_proposal_id_lower
    on <schema>.Observation ( lower(proposal_id)  COLLATE "C")
    where proposal_id is not null
;

drop index <schema>.Observation_i_targ_lower_pattern;

drop index if exists <schema>.Observation_i_proposal_id_lower_pattern;

drop index if exists <schema>.Artifact_i_uri;
create index Artifact_i_uri
    on <schema>.Artifact ( uri  COLLATE "C" )
;
