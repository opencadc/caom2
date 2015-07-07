
drop view if exists caom2.obscore;

drop table if exists caom2.Chunk;
drop table if exists caom2.Part;
drop table if exists caom2.Artifact;
drop table if exists caom2.Plane_inputs;
drop table if exists caom2.Plane;
drop table if exists caom2.Observation_members;
drop table if exists caom2.Observation;

drop table if exists caom2.ObservationMetaReadAccess_new;
drop table if exists caom2.PlaneMetaReadAccess_new;
drop table if exists caom2.PlaneDataReadAccess_new;
drop table if exists caom2.ArtifactMetaReadAccess;
drop table if exists caom2.PartMetaReadAccess;
drop table if exists caom2.ChunkMetaReadAccess;

drop table if exists caom2.DeletedObservation;
drop table if exists caom2.DeletedObservationMetaReadAccess_new;
drop table if exists caom2.DeletedPlaneMetaReadAccess_new;
drop table if exists caom2.DeletedPlaneDataReadAccess_new;

drop table if exists caom2.HarvestState;
drop table if exists caom2.HarvestSkip;
