
-- primary keys for deleted entity tables to support replication
create unique index DeletedObservation_pkey on <schema>.DeletedObservation (id);
alter table <schema>.DeletedObservation ADD PRIMARY USING INDEX DeletedObservation_pkey;

create unique index DeletedObservationMetaReadAccess_pkey on <schema>.DeletedObservationMetaReadAccess (id);
alter table <schema>.DeletedObservationMetaReadAccess ADD PRIMARY USING INDEX DeletedObservationMetaReadAccess_pkey;

create unique index DeletedPlaneMetaReadAccess_pkey on <schema>.DeletedPlaneMetaReadAccess (id);
alter table <schema>.DeletedPlaneMetaReadAccess ADD PRIMARY USING INDEX DeletedPlaneMetaReadAccess_pkey;

create unique index DeletedPlaneDataReadAccess_pkey on <schema>.DeletedPlaneDataReadAccess (id);
alter table <schema>.DeletedPlaneDataReadAccess ADD PRIMARY USING INDEX DeletedPlaneDataReadAccess_pkey;

-- recreate join tables to support loose coupling to target
drop table if exists <schema>.Observation_members;
create table <schema>.Observation_members
(
    compositeID uuid not null references <schema>.Observation (obsID),
    simpleID varchar(512) not null
)
;
create unique index i_composite2simple on <schema>.Observation_members (compositeID,simpleID)
;
create unique index i_simple2composite on <schema>.Observation_members (simpleID,compositeID)
;

drop table if exists <schema>.Plane_inputs;
create table <schema>.Plane_inputs
(
    outputID uuid not null references <schema>.Plane (planeID),
    inputID varchar(512) not null
)
;
create unique index i_output2input on <schema>.Plane_inputs (outputID,inputID)
;
create unique index i_input2output on <schema>.Plane_inputs (inputID,outputID)
;
