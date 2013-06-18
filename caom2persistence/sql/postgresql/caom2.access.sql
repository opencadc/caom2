
-- ObservationMetaReadAccess --
drop table if exists caom2.ObservationMetaReadAccess;

create table caom2.ObservationMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group1
    on caom2.ObservationMetaReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset1
    on caom2.ObservationMetaReadAccess ( groupID, assetID )
tablespace caom_index
;


-- PlaneMetaReadAccess --
drop table if exists caom2.PlaneMetaReadAccess;

create table caom2.PlaneMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group2
    on caom2.PlaneMetaReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset2
    on caom2.PlaneMetaReadAccess ( groupID, assetID )
tablespace caom_index
;

-- PlaneDataReadAccess --
drop table if exists caom2.PlaneDataReadAccess;
create table caom2.PlaneDataReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group3
    on caom2.PlaneDataReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset3
    on caom2.PlaneDataReadAccess ( groupID, assetID )
tablespace caom_index
;

-- ArtifactMetaReadAccess --
drop table if exists caom2.ArtifactMetaReadAccess;

create table caom2.ArtifactMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group4
    on caom2.ArtifactMetaReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset4
    on caom2.ArtifactMetaReadAccess ( groupID, assetID )
tablespace caom_index
;

-- PartMetaReadAccess --
drop table if exists caom2.PartMetaReadAccess;

create table caom2.PartMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group5
    on caom2.PartMetaReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset5
    on caom2.PartMetaReadAccess ( groupID, assetID )
tablespace caom_index
;

-- ChunkMetaReadAccess --
drop table if exists caom2.ChunkMetaReadAccess;

create table caom2.ChunkMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    bigint not null primary key using index tablespace caom_index,
    lastModified    timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

create index i_asset_group6
    on caom2.ChunkMetaReadAccess ( assetID, groupID )
tablespace caom_index
;
create index i_group_asset6
    on caom2.ChunkMetaReadAccess ( groupID, assetID )
tablespace caom_index
;