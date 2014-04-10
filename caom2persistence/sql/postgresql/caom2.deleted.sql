
create table caom2.DeletedObservation
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedObservationMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneMetaReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneDataReadAccess
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;
