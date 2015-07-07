
create table caom2_DeletedObservation
(
    id bigint primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_do_lastModified on caom2_DeletedObservation (lastModified)
;

create table caom2_DeletedObservationMetaReadAccess_new
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_domra_lastModified on caom2_DeletedObservationMetaReadAccess_new (lastModified)
;

create table caom2_DeletedPlaneMetaReadAccess_new
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_dpmra_lastModified on caom2_DeletedPlaneMetaReadAccess_new (lastModified)
;

create table caom2_DeletedPlaneDataReadAccess_new
(
    id binary(16) primary key nonclustered,
    lastModified datetime not null
)
lock datarows
partition by roundrobin 16
;

create index i_dpdra_lastModified on caom2_DeletedPlaneDataReadAccess_new (lastModified)
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

create trigger caom2_ObservationMetaReadAccess_new_delete
on caom2_ObservationMetaReadAccess_new
for delete
as
  insert caom2_DeletedObservationMetaReadAccess_new
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;

create trigger caom2_PlaneMetaReadAccess_new_delete
on caom2_PlaneMetaReadAccess_new
for delete
as
  insert caom2_DeletedPlaneMetaReadAccess_new
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;

create trigger caom2_PlaneDataReadAccess_new_delete
on caom2_PlaneDataReadAccess_new
for delete
as
  insert caom2_DeletedPlaneDataReadAccess_new
  (id, lastModified)
  (select readAccessID, getdate() from deleted)
;
