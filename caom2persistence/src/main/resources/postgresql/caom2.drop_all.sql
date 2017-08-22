
drop view if exists caom2.ObsPart;
drop view if exists caom2.ObsFile;
drop view if exists caom2.ObsCore;
drop view if exists caom2.SIAv1;

drop table if exists caom2.Chunk;
drop table if exists caom2.Part;
drop table if exists caom2.Artifact;
drop table if exists caom2.Plane_inputs;
drop table if exists caom2.Plane;
drop table if exists caom2.Observation_members;
drop table if exists caom2.Observation;

drop table if exists caom2.ObservationMetaReadAccess;
drop table if exists caom2.PlaneMetaReadAccess;
drop table if exists caom2.PlaneDataReadAccess;

drop table if exists caom2.DeletedObservation;
drop table if exists caom2.DeletedObservationMetaReadAccess;
drop table if exists caom2.DeletedPlaneMetaReadAccess;
drop table if exists caom2.DeletedPlaneDataReadAccess;

drop table if exists caom2.HarvestState;
drop table if exists caom2.HarvestSkip;
drop table if exists caom2.HarvestSkipURI;

drop table if exists caom2.ModelVersion;

