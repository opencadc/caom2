
-- ObservationMetaReadAccess --
drop table if exists caom2.ObservationMetaReadAccess;

create table caom2.ObservationMetaReadAccess
(
    assetID         bigint not null,
    groupID         bigint not null,

    readAccessID    uuid not null primary key using index tablespace caom_index,
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

    readAccessID    uuid not null primary key using index tablespace caom_index,
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

    readAccessID    uuid not null primary key using index tablespace caom_index,
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

