
create table caom2_DeletedObservation
(
    id bigint primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_lastModified on caom2_DeletedObservation (lastModified)
;

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

create trigger caom2_Observation_delete
on caom2_Observation
for delete
as
        insert into caom2_DeletedObservation (id,lastModified)
	(select obsID,getdate() FROM deleted)
;

-- work-around for insert-delete-insert to remove the delete marker
create trigger caom2_Observation_insert
on caom2_Observation
for insert
as
        delete from caom2_DeletedObservation where id in
	(select obsID from inserted)
;

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
