
grant select on caom2.ModelVersion to CVOPUB;

grant select on caom2.Chunk to CVOPUB;
grant select on caom2.Part to CVOPUB;
grant select on caom2.Artifact to CVOPUB;
grant select on caom2.Plane to CVOPUB;
grant select on caom2.Observation to CVOPUB;

grant select on caom2.Observation_members to CVOPUB;
grant select on caom2.Plane_inputs to CVOPUB;

grant select on caom2.PlaneMetaReadAccess to CVOPUB;
grant select on caom2.PlaneDataReadAccess to CVOPUB;
grant select on caom2.ObservationMetaReadAccess to CVOPUB;

grant select on caom2.HarvestState to CVOPUB;
grant select on caom2.HarvestSkip to CVOPUB;
grant select on caom2.HarvestSkipURI to CVOPUB;

-- caom2 account used by caom2-repo-server web service
grant insert, update, delete on caom2.ModelVersion to caom2;
grant insert, update, delete on caom2.Chunk to caom2;
grant insert, update, delete on caom2.Part to caom2;
grant insert, update, delete on caom2.Artifact to caom2;
grant insert, update, delete on caom2.Plane to caom2;
grant insert, update, delete on caom2.Observation to caom2;

grant insert on caom2.DeletedObservation to caom2;
grant insert, update, delete on caom2.ObservationMetaReadAccess to caom2;
grant insert, update, delete on caom2.PlaneMetaReadAccess to caom2;
grant insert, update, delete on caom2.PlaneDataReadAccess to caom2;

grant insert on caom2.DeletedObservationMetaReadAccess to caom2;
grant insert on caom2.DeletedPlaneMetaReadAccess to caom2;
grant insert on caom2.DeletedPlaneDataReadAccess to caom2;
