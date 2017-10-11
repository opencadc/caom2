
-- checksum length 136 allows for 128 chars (sha512) + colon + 7 char algorithm

alter table <schema>.Observation
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);
    
alter table <schema>.Plane
    add column creatorID varchar(512),
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table <schema>.Artifact
    add column contentChecksum varchar(136),
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table <schema>.Part
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table <schema>.Chunk
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table <schema>.ObservationMetaReadAccess
    add column metaChecksum varchar(136);

alter table <schema>.PlaneMetaReadAccess
    add column metaChecksum varchar(136);

alter table <schema>.PlaneDataReadAccess
    add column metaChecksum varchar(136);

create unique index i_creatorID on <schema>.Plane(creatorID);
