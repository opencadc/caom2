
create table <schema>.Observation
(
-- polymorphic
    typeCode char not null,

    uri varchar(512) not null,
    uriBucket char(3) not null,
    collection varchar(64) not null,
    algorithm_name varchar(64) not null,
    obstype varchar(32),
    intent varchar(32),
    sequenceNumber int null,
    metaRelease timestamp,
    metaReadGroups text[],

    proposal_id varchar(256),
    proposal_pi varchar(256),
    proposal_project varchar(64),
    proposal_title citext,
    proposal_keywords citext,
    proposal_reference text,

    target_name varchar(64),
    target_targetID varchar(128),
    target_type varchar(64),
    target_standard integer,
    target_redshift double precision,
    target_moving integer,
    target_keywords citext,

    targetPosition_coordsys varchar(16),
    targetPosition_equinox double precision,
    targetPosition_coordinates double precision[],
-- alternate representation for optimized query execution
    _q_targetPosition_coordinates spoint,

    telescope_name varchar(64),
    telescope_geoLocationX double precision,
    telescope_geoLocationY double precision,
    telescope_geoLocationZ double precision,
    telescope_keywords citext,
    telescope_trackingMode varchar(64),

    instrument_name varchar(64),
    instrument_keywords citext,

    requirements_flag varchar(16),

    environment_seeing double precision,
    environment_humidity double precision,
    environment_elevation double precision,
    environment_tau double precision,
    environment_wavelengthTau double precision,
    environment_ambientTemp double precision,
    environment_photometric integer,

    members text[],

-- optimisation
--    metaReadAccessGroups tsvector default '',

-- entity
    obsID uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    metaProducer varchar(128),
-- caom entity
    maxLastModified timestamp not null,
    accMetaChecksum varchar(136) not null
)
;

create unique index i_observationURI on <schema>.Observation (uri)
;

-- harvesting index
create index i_maxLastModified on <schema>.Observation (maxLastModified)
;

-- member join support
create table <schema>.ObservationMember
(
    parentID uuid not null references <schema>.Observation (obsID),
    memberID varchar(512) not null
)
;

create unique index i_parent2member on <schema>.ObservationMember (parentID,memberID)
;

create unique index i_member2parent on <schema>.ObservationMember (memberID,parentID)
;

