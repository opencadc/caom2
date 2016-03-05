
create table caom2.Observation
(
    uri varchar(512) not null,                -- change: rename observationURI to uri
    uri_collection varchar(64) not null,      -- change: rename collection to uri_collection
    uri_observationID varchar(256) not null,  -- change: rename observationID to uri_observationID 
    algorithm_name varchar(64) not null,
    type varchar(32),
    intent varchar(32),
    sequenceNumber int null,
    metaRelease timestamp,

    proposal_id varchar(64),
    proposal_pi varchar(64),
    proposal_project varchar(64),
    proposal_title citext,       -- change: varchar(256) to citext
    proposal_keywords text,      -- change: text to tsvector

    target_name varchar(64),
    target_type varchar(64),
    target_standard integer,
    target_redshift double precision,
    target_moving integer,
    target_keywords text, -- change: text to tsvector

    targetPosition_coordsys varchar(16),
    targetPosition_equinox double precision,
    targetPosition_coordinates_cval1 double precision,
    targetPosition_coordinates_cval2 double precision,

    telescope_name varchar(64),
    telescope_geoLocationX double precision,
    telescope_geoLocationY double precision,
    telescope_geoLocationZ double precision,
    telescope_keywords text, -- change: text to tsvector

    requirements_flag varchar(16),

    instrument_name varchar(64),
    instrument_keywords text, -- change: text to tsvector

    environment_seeing double precision,
    environment_humidity double precision,
    environment_elevation double precision,
    environment_tau double precision,
    environment_wavelengthTau double precision,
    environment_ambientTemp double precision,
    environment_photometric integer,

    members text null,

-- optimisation
    metaReadAccessGroups tsvector default '',

-- internal
    typeCode char not null,
    obsID uuid not null primary key using index tablespace caom_index, -- change: UUID
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create unique index i_observationURI on caom2.Observation (uri_collection, uri_observationID)
tablespace caom_index
;

-- harvesting index for recompute mode of caom2harvester
create index i_maxLastModified on caom2.Observation (maxLastModified)
tablespace caom_index
;

-- member join support
-- not currently used/tested
create table caom2.Observation_members
(
    compositeID uuid not null references caom2.Observation (obsID), -- change: UUID
    simpleID uuid not null references caom2.Observation (obsID)     -- change: UUID
)
tablespace caom_data
;

create unique index i_composite2simple on caom2.Observation_members (compositeID,simpleID)
tablespace caom_index
;

create unique index i_simple2composite on caom2.Observation_members (simpleID,compositeID)
tablespace caom_index
;
