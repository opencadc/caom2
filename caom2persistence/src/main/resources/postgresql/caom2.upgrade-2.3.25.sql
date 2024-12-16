
-- add read access groups columns directly to Observation and Plane table
-- remove unused stateCode column from all entity tables

alter table <schema>.Observation drop column stateCode;

alter table <schema>.Plane drop column stateCode;

alter table <schema>.Artifact drop column stateCode;

alter table <schema>.Part drop column stateCode;

alter table <schema>.Chunk drop column stateCode;

alter table <schema>.Observation add column metaReadGroups text;
alter table <schema>.Plane add column metaReadGroups text, add column dataReadGroups text;
-- optimisations
alter table <schema>.Artifact add column metaReadGroups text;
alter table <schema>.Part add column metaReadGroups text;
alter table <schema>.Chunk add column metaReadGroups text;

drop table <schema>.ObservationMetaReadAccess;
drop table <schema>.PlaneMetaReadAccess;
drop table <schema>.PlaneDataReadAccess;

drop table <schema>.DeletedObservationMetaReadAccess;
drop table <schema>.DeletedPlaneMetaReadAccess;
drop table <schema>.DeletedPlaneDataReadAccess;
