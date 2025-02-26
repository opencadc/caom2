
-- to modify this file with a specific <schema>, try:
-- cat caom2.drop_all.sql | sed 's/<schema>/caom2/g' > doit.sql

truncate table <schema>.Chunk;
truncate table <schema>.Part;
truncate table <schema>.Artifact;
truncate table <schema>.ProvenanceInput;
truncate table <schema>.Plane;
truncate table <schema>.ObservationMember;
truncate table <schema>.Observation;
truncate table <schema>.ArtifactDescription;

truncate table <schema>.DeletedObservation;
truncate table <schema>.DeletedArtifactDescription;

truncate table <schema>.HarvestState;
truncate table <schema>.HarvestSkip;

truncate table <schema>.ModelVersion;

