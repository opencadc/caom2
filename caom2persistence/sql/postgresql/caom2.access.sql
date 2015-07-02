
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

create index i_asset_group1
    on caom2.ObservationMetaReadAccess_new ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset1
    on caom2.ObservationMetaReadAccess_new ( groupID, assetID )
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

create index i_asset_group2
    on caom2.PlaneMetaReadAccess_new ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset2
    on caom2.PlaneMetaReadAccess_new ( groupID, assetID )
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

create index i_asset_group3
    on caom2.PlaneDataReadAccess_new ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset3
    on caom2.PlaneDataReadAccess_new ( groupID, assetID )
tablespace caom_index
;

