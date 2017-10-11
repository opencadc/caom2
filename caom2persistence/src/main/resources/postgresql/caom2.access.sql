
-- ObservationMetaReadAccess --
create table <schema>.ObservationMetaReadAccess
(
    assetID         uuid not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null,
    metaChecksum varchar(136) not null
)
;

create unique index i_omra_tuple 
    on <schema>.ObservationMetaReadAccess (groupID, assetID)
;

-- PlaneMetaReadAccess --
create table <schema>.PlaneMetaReadAccess
(
    assetID         uuid not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null,
    metaChecksum varchar(136) not null
)
;

create unique index i_pmra_tuple 
    on <schema>.PlaneMetaReadAccess (groupID, assetID)
;

-- PlaneDataReadAccess --
create table <schema>.PlaneDataReadAccess
(
    assetID         uuid not null, 
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null,
    metaChecksum varchar(136) not null
)
;

create unique index i_pdra_tuple 
    on <schema>.PlaneDataReadAccess (groupID, assetID)
;