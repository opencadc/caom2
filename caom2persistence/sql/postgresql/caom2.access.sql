
-- ObservationMetaReadAccess --
create table caom2.ObservationMetaReadAccess
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_ObservationMetaReadAccess_group
    on caom2.ObservationMetaReadAccess (groupID)
tablespace caom_index
;

create unique index i_omra_tuple 
    on caom2.ObservationMetaReadAccess (assetID, groupID)
;


-- PlaneMetaReadAccess --
create table caom2.PlaneMetaReadAccess
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_PlaneMetaReadAccess_group
    on caom2.PlaneMetaReadAccess (groupID)
tablespace caom_index
;

create unique index i_pmra_tuple 
    on caom2.PlaneMetaReadAccess (assetID, groupID)
;

-- PlaneDataReadAccess --
create table caom2.PlaneDataReadAccess
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_PlaneDataReadAccess_group
    on caom2.PlaneDataReadAccess (groupID)
tablespace caom_index
;

create unique index i_pdra_tuple 
    on caom2_PlaneDataReadAccess (assetID, groupID)
;