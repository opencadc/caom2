
-- to modify this file with a specific <schema>, try:
-- cat caom2.drop_all.sql | sed 's/<schema>/caom2/g' > doit.sql

drop view if exists <schema>.ObsPart;
drop view if exists <schema>.ObsFile;
drop view if exists <schema>.ObsCore;
drop view if exists <schema>.SIAv1;

drop table if exists <schema>.Chunk;
drop table if exists <schema>.Part;
drop table if exists <schema>.Artifact;
drop table if exists <schema>.ProvenanceInput;
drop table if exists <schema>.Plane;
drop table if exists <schema>.ObservationMember;
drop table if exists <schema>.Observation;

drop table if exists <schema>.DeletedObservation;

drop table if exists <schema>.HarvestState;
drop table if exists <schema>.HarvestSkip;
drop table if exists <schema>.HarvestSkipURI;

drop table if exists <schema>.ModelVersion;

