
create table caom2.DeletedObservation
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedObservationMetaReadAccess_new
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneMetaReadAccess_new
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneDataReadAccess_new
(
    id uuid not null,
    lastModified timestamp not null
)
tablespace caom_data
;
