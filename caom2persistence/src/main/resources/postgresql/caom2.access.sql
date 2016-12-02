
-- ObservationMetaReadAccess --
create table caom2.ObservationMetaReadAccess
(
    assetID         uuid not null, -- change: UUID
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null
)
;

create unique index i_omra_tuple 
    on caom2.ObservationMetaReadAccess (groupID, assetID)
;

-- PlaneMetaReadAccess --
create table caom2.PlaneMetaReadAccess
(
    assetID         uuid not null, -- change: UUID
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null
)
;

create unique index i_pmra_tuple 
    on caom2.PlaneMetaReadAccess (groupID, assetID)
;

-- PlaneDataReadAccess --
create table caom2.PlaneDataReadAccess
(
    assetID         uuid not null, -- change: UUID
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key,
    lastModified    timestamp not null,
    stateCode int not null
)
;

create unique index i_pdra_tuple 
    on caom2.PlaneDataReadAccess (groupID, assetID)
;