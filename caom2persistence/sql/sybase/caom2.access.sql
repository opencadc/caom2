--
-- note: indexes and triggers support harvesting by lastModfied only
--

-- ObservationMetaReadAccess --
create table caom2_ObservationMetaReadAccess
(
    gr_permission_id  bigint not null,
    asset_id          bigint not null,
    group_id          bigint not null,
    lastmod           datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered ,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified1
    on caom2_ObservationMetaReadAccess ( lastmod )
;

-- PlaneMetaReadAccess --
create table caom2_PlaneMetaReadAccess
(
    gr_permission_id  bigint not null,
    asset_id          bigint not null,
    group_id          bigint not null,
    lastmod           datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified2
    on caom2_PlaneMetaReadAccess ( lastmod )
;

-- PlaneDataReadAccess --
create table caom2_PlaneDataReadAccess
(
    gr_permission_id  bigint not null,
    asset_id          bigint not null,
    group_id          bigint not null,
    lastmod           datetime not null,

    readAccessID      binary(16) default newid() primary key nonclustered,
    stateCode         int null
)
lock datarows
with identity_gap = 512
partition by roundrobin 16
;

create index i_lastModified3
    on caom2_PlaneDataReadAccess ( lastmod )
;
