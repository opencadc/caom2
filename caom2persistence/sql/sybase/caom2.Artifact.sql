
create table caom2_Artifact
(
    uri varchar(1024) not null,

    productType varchar(64) null,
    contentType varchar(128) null,
    contentLength bigint null,
    alternative integer not null,

-- internal
    obsID bigint not null,
    metaRelease datetime null,
    planeID bigint not null references caom2_Plane (planeID),
    artifactID bigint not null primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    stateCode int not null
)
lock datarows
;

create clustered index plane2artifact on caom2_Artifact (planeID)
;

create index lastModified on caom2_Artifact (lastModified)
;
