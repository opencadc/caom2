--
-- note: indexes support harvesting by lastModfied only
--

-- ObservationMetaReadAccess --
create table caom2_ObservationMetaReadAccess
(
    gr_permission_id  bigint not null,
    assetID           bigint not null,
    groupID           bigint not null,
    lastModified      datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified1
    on caom2_ObservationMetaReadAccess ( lastModified )
;

-- PlaneMetaReadAccess --
create table caom2_PlaneMetaReadAccess
(
    gr_permission_id  bigint not null,
    assetID           bigint not null,
    groupID           bigint not null,
    lastModified      datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified2
    on caom2_PlaneMetaReadAccess ( lastModified )
;

-- PlaneDataReadAccess --
create table caom2_PlaneDataReadAccess
(
    gr_permission_id  bigint not null,
    assetID           bigint not null,
    groupID           bigint not null,
    lastModified      datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified3
    on caom2_PlaneDataReadAccess ( lastModified )
;
