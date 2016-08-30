--
-- note: indexes support harvesting by lastModfied only
--

-- temporary table names --

-- ObservationMetaReadAccess --
create table caom2_ObservationMetaReadAccess
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified
    on caom2_ObservationMetaReadAccess ( lastModified )
;

create unique index i_omra_tuple 
    on caom2_ObservationMetaReadAccess (assetID, groupID)
;

-- PlaneMetaReadAccess --
create table caom2_PlaneMetaReadAccess
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified
    on caom2_PlaneMetaReadAccess ( lastModified )
;

create unique index i_pmra_tuple 
    on caom2_PlaneMetaReadAccess (assetID, groupID)
;

-- PlaneDataReadAccess --
create table caom2_PlaneDataReadAccess
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified
    on caom2_PlaneDataReadAccess ( lastModified )
;

create unique index i_pdra_tuple 
    on caom2_PlaneDataReadAccess (assetID, groupID)
;
