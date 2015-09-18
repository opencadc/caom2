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
