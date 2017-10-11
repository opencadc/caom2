
create table <schema>.DeletedObservation
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delobs_lastModified on <schema>.DeletedObservation (lastModified)
;

create table <schema>.DeletedObservationMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delomra_lastModified on <schema>.DeletedObservationMetaReadAccess (lastModified)
;

create table <schema>.DeletedPlaneMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delpmra_lastModified on <schema>.DeletedPlaneMetaReadAccess (lastModified)
;

create table <schema>.DeletedPlaneDataReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
;
create index i_delpdra_lastModified on <schema>.DeletedPlaneDataReadAccess (lastModified)
;
