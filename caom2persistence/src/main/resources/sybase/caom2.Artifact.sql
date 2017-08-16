
create table caom2_Artifact
(
    uri varchar(1024) not null,

    productType varchar(64) not null,
    releaseType varchar(16) not null,
    contentType varchar(128) null,
    contentLength bigint null,
    contentChecksum varchar(136) null,

-- internal
    obsID bigint not null,
    metaRelease datetime null,
    planeID bigint not null references caom2_Plane (planeID),
    artifactID bigint not null primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    stateCode int not null,
    metaChecksum varchar(36) null,
    accMetaChecksum varchar(36) null
)
lock datarows
;

create clustered index plane2artifact on caom2_Artifact (planeID)
;

create index lastModified on caom2_Artifact (lastModified)
;

-- special index to support caom2_isPublic and caom2_getAuth lookup by uri
create index uri_md5 on caom2_Artifact( hash(uri, 'md5') )
;
