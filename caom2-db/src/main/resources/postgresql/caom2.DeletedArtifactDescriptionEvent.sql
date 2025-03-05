
create table <schema>.DeletedArtifactDescriptionEvent
(
    uri text not null,

-- entity
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    metaProducer varchar(128)
)
;

-- incremental sync
create index i_dad_lastModified on <schema>.DeletedArtifactDescriptionEvent (lastModified)
;
