
create table <schema>.ArtifactDescription
(
    uri text not null,
    description text not null,

-- entity
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    metaProducer varchar(128)
)
;

-- logical primary key
create unique index ArtifactDescription_i_uri
    on <schema>.ArtifactDescription (uri)
;

