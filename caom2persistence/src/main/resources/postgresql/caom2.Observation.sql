
create table <schema>.Observation
(
    observationURI varchar(512) not null,
    collection varchar(64) not null,
    observationID varchar(256) not null,
    algorithm_name varchar(64) not null,
    type varchar(32),
    intent varchar(32),
    sequenceNumber int null,
    metaRelease timestamp,

    proposal_id varchar(64),
    proposal_pi varchar(64),
    proposal_project varchar(64),
    proposal_title citext,
    proposal_keywords citext,

    target_name varchar(64),
    target_targetID varchar(128),
    target_type varchar(64),
    target_standard integer,
    target_redshift double precision,
    target_moving integer,
    target_keywords citext,

    targetPosition_coordsys varchar(16),
    targetPosition_equinox double precision,
    targetPosition_coordinates_cval1 double precision,
    targetPosition_coordinates_cval2 double precision,

    telescope_name varchar(64),
    telescope_geoLocationX double precision,
    telescope_geoLocationY double precision,
    telescope_geoLocationZ double precision,
    telescope_keywords citext,

    requirements_flag varchar(16),

    instrument_name varchar(64),
    instrument_keywords citext,

    environment_seeing double precision,
    environment_humidity double precision,
    environment_elevation double precision,
    environment_tau double precision,
    environment_wavelengthTau double precision,
    environment_ambientTemp double precision,
    environment_photometric integer,

    members text,
    metaReadGroups text,

-- optimisation
    metaReadAccessGroups tsvector default '',

-- internal
    typeCode char not null,
    obsID uuid not null primary key,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    metaChecksum varchar(136) not null,
    accMetaChecksum varchar(136) not null,
    metaProducer varchar(128)
)
;

create unique index i_observationURI on <schema>.Observation (observationURI)
;

create unique index i_observationURI2 on <schema>.Observation (collection, observationID)
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

