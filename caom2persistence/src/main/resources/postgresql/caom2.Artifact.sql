
create table caom2.Artifact
(
    uri varchar(1024) not null,
    productType varchar(64) not null,
    releaseType varchar(16) not null,
    contentType varchar(128),
    contentLength bigint,

-- optimisation
    metaReadAccessGroups tsvector default '',

-- internal
    metaRelease timestamp,
    obsID uuid not null,
    planeID uuid not null references caom2.Plane (planeID),
    artifactID uuid not null primary key,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    stateCode int not null
)
;

-- this is for Plane join Artifact
create index i_planeID on caom2.Artifact (planeID)
;

-- tag the clustering index
cluster i_planeID on caom2.Artifact
;
