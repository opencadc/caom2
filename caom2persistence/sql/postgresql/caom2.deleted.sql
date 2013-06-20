
create table caom2.DeletedObservation
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedObservationMetaReadAccess
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneMetaReadAccess
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;

create table caom2.DeletedPlaneDataReadAccess
(
    id bigint not null,
    lastModified timestamp not null
)
tablespace caom_data
;
