
-- update caom2.DeletedObservation table
delete from <schema>.DeletedObservation;

alter table <schema>.DeletedObservation 
    add column collection varchar(64) not null,
    add column observationID varchar(256) not null;

create unique index i_planeURI on <schema>.Plane(planeURI)
;
