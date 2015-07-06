--
-- note: indexes support harvesting by lastModfied only
--

-- ObservationMetaReadAccess --
create table caom2_ObservationMetaReadAccess_new
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified1
    on caom2_ObservationMetaReadAccess_new ( lastModified )
;

-- PlaneMetaReadAccess --
create table caom2_PlaneMetaReadAccess_new
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified2
    on caom2_PlaneMetaReadAccess_new ( lastModified )
;

-- PlaneDataReadAccess --
create table caom2_PlaneDataReadAccess_new
(
    assetID           bigint not null,
    groupID           varchar(128) not null,
    lastModified      datetime not null,

    readAccessID      binary(16) primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified3
    on caom2_PlaneDataReadAccess_new ( lastModified )
;
