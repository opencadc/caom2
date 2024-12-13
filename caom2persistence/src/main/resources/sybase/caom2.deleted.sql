
create table caom2_DeletedObservationMetaReadAccess
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedObservationMetaReadAccess (lastModified)
;

create table caom2_DeletedPlaneMetaReadAccess
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedPlaneMetaReadAccess (lastModified)
;

create table caom2_DeletedPlaneDataReadAccess
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedPlaneDataReadAccess (lastModified)
;


-- delete triggers to automatically insert into Deleted tables
create trigger caom2_ObservationMetaReadAccess_delete
on caom2_ObservationMetaReadAccess
for delete
as
  insert caom2_DeletedObservationMetaReadAccess
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;

create trigger caom2_PlaneMetaReadAccess_delete
on caom2_PlaneMetaReadAccess
for delete
as
  insert caom2_DeletedPlaneMetaReadAccess
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;

create trigger caom2_PlaneDataReadAccess_delete
on caom2_PlaneDataReadAccess
for delete
as
  insert caom2_DeletedPlaneDataReadAccess
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;
