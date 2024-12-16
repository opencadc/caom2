
delete from table caom2_Chunk;
delete from table caom2_Part;
delete from table caom2_Artifact;
delete from table caom2_Plane;
delete from table caom2_Observation;

delete from table caom2_ObservationMetaReadAccess;
delete from table caom2_PlaneMetaReadAccess;
delete from table caom2_PlaneDataReadAccess;

delete from table caom2_HarvestState;
delete from table caom2_HarvestSkip;

truncate table caom2_DeletedObservation;
truncate table caom2_DeletedObservationMetaReadAccess;
truncate table caom2_DeletedPlaneMetaReadAccess;
truncate table caom2_DeletedPlaneDataReadAccess;
