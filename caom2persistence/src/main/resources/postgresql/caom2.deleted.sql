
create table caom2.DeletedObservation
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delobs_lastModified on caom2.DeletedObservation (lastModified)
;

create table caom2.DeletedObservationMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delomra_lastModified on caom2.DeletedObservationMetaReadAccess (lastModified)
;

create table caom2.DeletedPlaneMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delpmra_lastModified on caom2.DeletedPlaneMetaReadAccess (lastModified)
;

create table caom2.DeletedPlaneDataReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delpdra_lastModified on caom2.DeletedPlaneDataReadAccess (lastModified)
;
