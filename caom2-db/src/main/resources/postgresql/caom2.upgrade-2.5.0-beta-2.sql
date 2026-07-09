-- minor index naming tweaks for consistency
alter index <schema>.i_observationURI rename to i_observation_uri;

alter index <schema>.i_uri rename to i_plane_uri;

alter index <schema>.i_publisherID rename to i_plane_publisherID;

alter index <schema>.Artifact_i_uri rename to i_artifact_uri;

alter index <schema>.ArtifactDescription_i_uri rename to i_artifactdescription_uri;


-- foreign key indices
alter index <schema>.i_obsID rename to plane_fkey;
cluster plane_fkey on <schema>.Plane;

alter index <schema>.i_planeID rename to artifact_fkey;
cluster artifact_fkey on <schema>.Artifact;


