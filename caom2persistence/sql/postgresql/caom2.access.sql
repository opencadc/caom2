
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
