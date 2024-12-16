
create table <schema>.Artifact
(
    uri varchar(1024) not null,
    productType varchar(64) not null,
    releaseType varchar(16) not null,
    contentType varchar(128),
    contentLength bigint,
    contentChecksum varchar(136),
    contentRelease timestamp,
    contentReadGroups text,

-- optimisation
    metaReadGroups text,
    metaReadAccessGroups tsvector default '',

-- internal
    metaRelease timestamp,
    obsID uuid not null,
    planeID uuid not null references <schema>.Plane (planeID),
    artifactID uuid not null primary key,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    metaChecksum varchar(136) not null,
    accMetaChecksum varchar(136) not null,
    metaProducer varchar(128)
)
;

-- this is for Plane join Artifact
create index i_planeID on <schema>.Artifact (planeID)
;

-- tag the clustering index
cluster i_planeID on <schema>.Artifact
;

create index Artifact_i_uri
    on <schema>.Artifact (uri)
;

