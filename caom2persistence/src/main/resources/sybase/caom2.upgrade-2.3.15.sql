
drop table caom2_Observation_members;
create table caom2_ObservationMember
(
    compositeID bigint not null references caom2_Observation (obsID),
    simpleID varchar(512) not null
)
lock datarows
;

create unique clustered index composite2simple on caom2_ObservationMember (compositeID,simpleID)
;

create unique nonclustered index simple2composite on caom2_ObservationMember (simpleID,compositeID)
;

create table caom2_ProvenanceInput
(
    outputID bigint not null references caom2_Plane (planeID),
    inputID varchar(512) not null
)
lock datarows
;

create unique index i_output2input on caom2_ProvenanceInput (outputID,inputID)
;

create unique index i_input2output on caom2_ProvenanceInput (inputID,outputID)
;
