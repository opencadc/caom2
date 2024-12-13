
-- checksum length 36 allows for 32 chars (md5 128-bit) + colon + 3 char algorithm

alter table caom2_Observation add metaChecksum varchar(36) null;
alter table caom2_Observation add accMetaChecksum varchar(36) null;
    
alter table caom2_Plane add creatorID varchar(512) null;
alter table caom2_Plane add metaChecksum varchar(36) null;
alter table caom2_Plane add accMetaChecksum varchar(36) null;

alter table caom2_Artifact add contentChecksum varchar(36) null;
alter table caom2_Artifact add metaChecksum varchar(36) null;
alter table caom2_Artifact add accMetaChecksum varchar(36) null;

alter table caom2_Part add metaChecksum varchar(36) null;
alter table caom2_Part add accMetaChecksum varchar(36) null;

alter table caom2_Chunk add metaChecksum varchar(36) null;
alter table caom2_Chunk add accMetaChecksum varchar(36) null;

alter table caom2_ObservationMetaReadAccess
    add metaChecksum varchar(36) null;

alter table caom2_PlaneMetaReadAccess
    add metaChecksum varchar(36) null;

alter table caom2_PlaneDataReadAccess
    add metaChecksum varchar(36) null;
