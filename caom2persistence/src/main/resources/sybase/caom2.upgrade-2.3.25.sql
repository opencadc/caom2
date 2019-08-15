
-- add dummy read access groups columns directly to Observation and Plane table
-- remove unused stateCode column from all entity tables

alter table caom2_Observation drop stateCode;

alter table caom2_Plane drop stateCode;

alter table caom2_Artifact drop stateCode;

alter table caom2_Part drop stateCode;

alter table caom2_Chunk drop stateCode;

alter table caom2_Observation add metaReadGroups text null;

alter table caom2_Plane add metaReadGroups text null;
alter table caom2_Plane add dataReadGroups text null;

sp_chgattribute caom2_Observation,"dealloc_first_txtpg",1;

sp_chgattribute caom2_Plane,"dealloc_first_txtpg",1;


--drop table caom2_ObservationMetaReadAccess;
--drop table caom2_PlaneMetaReadAccess;
--drop table caom2_PlaneDataReadAccess;

--drop table caom2_DeletedObservationMetaReadAccess;
--drop table caom2_DeletedPlaneMetaReadAccess;
--drop table caom2_DeletedPlaneDataReadAccess;
