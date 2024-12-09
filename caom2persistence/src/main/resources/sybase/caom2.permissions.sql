
grant select on caom2_Chunk to public;
grant select on caom2_Part to public;
grant select on caom2_Artifact to public;
grant select on caom2_Plane to public;
grant select on caom2_Observation to public;

grant select on caom2_DeletedObservation to public;

grant select on caom2_ObservationMetaReadAccess to public;
grant select on caom2_PlaneMetaReadAccess to public;
grant select on caom2_PlaneDataReadAccess to public;

grant select on caom2_DeletedObservationMetaReadAccess to public;
grant select on caom2_DeletedPlaneMetaReadAccess to public;
grant select on caom2_DeletedPlaneDataReadAccess to public;

grant select on caom2_HarvestState to public;

-- caom2 account used by caom2repo web service
grant insert, update, delete on caom2_Chunk to caom2;
grant insert, update, delete on caom2_Part to caom2;
grant insert, update, delete on caom2_Artifact to caom2;
grant insert, update, delete on caom2_Plane to caom2;
grant insert, update, delete on caom2_Observation to caom2;

grant insert, update, delete on caom2_DeletedObservation to caom2;
grant insert, update, delete on caom2_ObservationMetaReadAccess to caom2;
grant insert, update, delete on caom2_PlaneMetaReadAccess to caom2;
grant insert, update, delete on caom2_PlaneDataReadAccess to caom2;

grant insert on caom2_DeletedObservationMetaReadAccess to caom2;
grant insert on caom2_DeletedPlaneMetaReadAccess to caom2;
grant insert on caom2_DeletedPlaneDataReadAccess to caom2;
