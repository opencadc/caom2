
create table <schema>.DeletedObservation
(
    collection varchar(64) not null,
    observationID varchar(256) not null,
    id uuid not null primary key,
    lastModified timestamp not null
)
;
create index i_delobs_lastModified on <schema>.DeletedObservation (lastModified)
;
