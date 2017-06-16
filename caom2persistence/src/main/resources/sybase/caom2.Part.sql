
create table caom2_Part
(
    name varchar(1024) not null,
    productType varchar(64) null,

-- internal
    obsID bigint not null,
    planeID bigint not null,
    metaRelease datetime null,
    artifactID bigint not null references caom2_Artifact (artifactID),
    partID bigint not null primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    stateCode int not null,
    metaChecksum varchar(36),
    accMetaChecksum varchar(36)
)
lock datarows
;

create clustered index artifact2part on caom2_Part (artifactID)
;

create index lastModified on caom2_Part (lastModified)
;
