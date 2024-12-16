
create table caom2_Observation
(
    collection varchar(64) not null,
    observationID varchar(256) not null, 
    algorithm_name varchar(64) not null,
    type varchar(32) null,
    intent varchar(32) null,
    sequenceNumber int null,
    metaRelease datetime null,

    proposal_id varchar(64) null,
    proposal_pi varchar(64) null,
    proposal_project varchar(64) null,
    proposal_title varchar(256) null,
    proposal_keywords text null,

    target_name varchar(64) null,
    target_type varchar(32) null,
    target_standard int null,
    target_redshift double precision null,
    target_moving int null,
    target_keywords text null,

    targetPosition_coordsys varchar(16) null,
    targetPosition_equinox double precision null,
    targetPosition_coordinates_cval1 double precision null,
    targetPosition_coordinates_cval2 double precision null,

    requirements_flag varchar(16) null,

    telescope_name varchar(64) null,
    telescope_geoLocationX double precision null,
    telescope_geoLocationY double precision null,
    telescope_geoLocationZ double precision null,
    telescope_keywords text null,

    instrument_name varchar(64) null,
    instrument_keywords text null,

    environment_seeing double precision null,
    environment_humidity double precision null,
    environment_elevation double precision null,
    environment_tau double precision null,
    environment_wavelengthTau double precision null,
    environment_ambientTemp double precision null,
    environment_photometric int null,

    members text null,
-- needed by 2.4-alpha1
    metaReadGroups char(1) null,

-- internal
    typeCode char not null,
    obsID bigint not null primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    metaChecksum varchar(136),
    accMetaChecksum varchar(136)
)
lock datarows
partition by roundrobin 16
;

create unique index observationURI on caom2_Observation (collection, observationID)
;

create index lastModified on caom2_Observation (lastModified)
;

-- primary harvesting index
create index maxLastModified on caom2_Observation (maxLastModified)
;

-- reference/join table for composites 
-- not currently used/tested
create table caom2_ObservationMember
(
    compositeID bigint not null references caom2_Observation (obsID),
    simpleID varchar(512) not null
)
lock datarows
;

create unique clustered index composite2simple on caom2_ObservationMember (compositeID,simpleID)
;

create unique nonclustered index simple2composite on caom2_ObservationMember (simpleID,compositeID)
;
