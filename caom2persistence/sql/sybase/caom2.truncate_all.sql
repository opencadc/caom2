
delete from table caom2_Chunk;
delete from table caom2_Part;
delete from table caom2_Artifact;
delete from table caom2_Plane;
delete from table caom2_Observation;

delete from table caom2_ObservationMetaReadAccess_new;
delete from table caom2_PlaneMetaReadAccess_new;
delete from table caom2_PlaneDataReadAccess_new;

delete from table caom2_HarvestState;
delete from table caom2_HarvestSkip;

truncate table caom2_DeletedObservation;
truncate table caom2_DeletedObservationMetaReadAccess_new;
truncate table caom2_DeletedPlaneMetaReadAccess_new;
truncate table caom2_DeletedPlaneDataReadAccess_new;
