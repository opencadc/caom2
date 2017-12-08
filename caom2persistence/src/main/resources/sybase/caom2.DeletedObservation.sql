
create table caom2_DeletedObservation
(
    collection varchar(64) not null,
    observationID varchar(256) not null,
    id bigint primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedObservation (lastModified)
;

