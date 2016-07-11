
drop table if exists caom2.Artifact;

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
    obsID bigint not null,
    metaRelease timestamp,
    planeID bigint not null references caom2.Plane (planeID),
    artifactID bigint not null primary key using index tablespace caom_index,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    stateCode int not null
)
tablespace caom_data
;

-- this is for Plane join Artifact
create index i_planeID on caom2.Artifact (planeID)
tablespace caom_index
;

-- tag the clustering index
cluster i_planeID on caom2.Artifact
;
