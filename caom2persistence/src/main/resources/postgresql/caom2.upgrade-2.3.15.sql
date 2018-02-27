
-- primary keys for deleted entity tables to support replication
create unique index DeletedObservation_pkey on <schema>.DeletedObservation (id);
alter table <schema>.DeletedObservation ADD PRIMARY KEY USING INDEX DeletedObservation_pkey;

create unique index DeletedObservationMetaReadAccess_pkey on <schema>.DeletedObservationMetaReadAccess (id);
alter table <schema>.DeletedObservationMetaReadAccess ADD PRIMARY KEY USING INDEX DeletedObservationMetaReadAccess_pkey;

create unique index DeletedPlaneMetaReadAccess_pkey on <schema>.DeletedPlaneMetaReadAccess (id);
alter table <schema>.DeletedPlaneMetaReadAccess ADD PRIMARY KEY USING INDEX DeletedPlaneMetaReadAccess_pkey;

create unique index DeletedPlaneDataReadAccess_pkey on <schema>.DeletedPlaneDataReadAccess (id);
alter table <schema>.DeletedPlaneDataReadAccess ADD PRIMARY KEY USING INDEX DeletedPlaneDataReadAccess_pkey;

-- recreate join tables to support loose coupling to target
drop table if exists <schema>.Observation_members;
create table <schema>.ObservationMember
(
    compositeID uuid not null references <schema>.Observation (obsID),
    simpleID varchar(512) not null
)
;
create unique index i_composite2simple on <schema>.ObservationMember (compositeID,simpleID)
;
create unique index i_simple2composite on <schema>.ObservationMember (simpleID,compositeID)
;

drop table if exists <schema>.Plane_inputs;
create table <schema>.ProvenanceInput
(
    outputID uuid not null references <schema>.Plane (planeID),
    inputID varchar(512) not null
)
;
create unique index i_output2input on <schema>.ProvenanceInput (outputID,inputID)
;
create unique index i_input2output on <schema>.ProvenanceInput (inputID,outputID)
;

