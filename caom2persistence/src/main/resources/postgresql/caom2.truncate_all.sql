
truncate table caom2.Plane_inputs;
truncate table caom2.Observation_members;

truncate table caom2.Chunk;
delete from caom2.Part;
delete from caom2.Artifact;
delete from caom2.Plane;
delete from caom2.Observation;

truncate table caom2.DeletedObservation;

truncate table caom2.ObservationMetaReadAccess;
truncate table caom2.PlaneMetaReadAccess;
truncate table caom2.PlaneDataReadAccess;

truncate table caom2.DeletedObservationMetaReadAccess;
truncate table caom2.DeletedPlaneMetaReadAccess;
truncate table caom2.DeletedPlaneDataReadAccess;

truncate table caom2.HarvestState;
truncate table caom2.HarvestSkip;
truncate table caom2.HarvestSkipURI;
