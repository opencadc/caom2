

-- clear previous deletion tracking
drop trigger caom2_Observation_delete;
drop trigger caom2_Observation_insert;
drop table caom2_DeletedObservation;

create table caom2_DeletedObservation
(
    collection varchar(64) not null,
    observationID varchar(256) not null,
    id bigint not null,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedObservation (lastModified)
;

