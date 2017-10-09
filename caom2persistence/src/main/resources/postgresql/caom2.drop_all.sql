
drop view if exists <schema>.ObsPart;
drop view if exists <schema>.ObsFile;
drop view if exists <schema>.ObsCore;
drop view if exists <schema>.SIAv1;

drop table if exists <schema>.Chunk;
drop table if exists <schema>.Part;
drop table if exists <schema>.Artifact;
drop table if exists <schema>.Plane_inputs;
drop table if exists <schema>.Plane;
drop table if exists <schema>.Observation_members;
drop table if exists <schema>.Observation;

drop table if exists <schema>.ObservationMetaReadAccess;
drop table if exists <schema>.PlaneMetaReadAccess;
drop table if exists <schema>.PlaneDataReadAccess;

drop table if exists <schema>.DeletedObservation;
drop table if exists <schema>.DeletedObservationMetaReadAccess;
drop table if exists <schema>.DeletedPlaneMetaReadAccess;
drop table if exists <schema>.DeletedPlaneDataReadAccess;

drop table if exists <schema>.HarvestState;
drop table if exists <schema>.HarvestSkip;
drop table if exists <schema>.HarvestSkipURI;

drop table if exists <schema>.ModelVersion;

