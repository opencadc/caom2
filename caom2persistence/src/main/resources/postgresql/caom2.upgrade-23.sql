
-- checksum length 136 allows for 128 chars (sha512) + colon + 7 char algorithm

alter table caom2.Observation
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);
    
alter table caom2.Plane
    add column creatorID varchar(512),
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table caom2.Artifact
    add column contentChecksum varchar(136),
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table caom2.Part
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table caom2.Chunk
    add column metaChecksum varchar(136),
    add column accMetaChecksum varchar(136);

alter table caom2.ObservationMetaReadAccess
    add column metaChecksum varchar(136);

alter table caom2.PlaneMetaReadAccess
    add column metaChecksum varchar(136);

alter table caom2.PlaneDataReadAccess
    add column metaChecksum varchar(136);

create unique index i_creatorID on caom2.Plane(creatorID);
