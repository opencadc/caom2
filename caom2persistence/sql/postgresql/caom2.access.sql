
-- ObservationMetaReadAccess --
drop table if exists caom2.ObservationMetaReadAccess_new;

create table caom2.ObservationMetaReadAccess_new
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_omra_group
    on caom2.ObservationMetaReadAccess_new (groupID)
tablespace caom_index
;

-- PlaneMetaReadAccess --
drop table if exists caom2.PlaneMetaReadAccess_new;

create table caom2.PlaneMetaReadAccess_new
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_pmra_group
    on caom2.PlaneMetaReadAccess_new (groupID)
tablespace caom_index
;

-- PlaneDataReadAccess --
drop table if exists caom2.PlaneDataReadAccess_new;
create table caom2.PlaneDataReadAccess_new
(
    assetID         bigint not null,
    groupID         varchar(128) not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_pdra_group
    on caom2.PlaneDataReadAccess_new (groupID)
tablespace caom_index
;
