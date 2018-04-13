
create table caom2_Plane
(
    productID varchar(64) not null,
    creatorID varchar(512) null,

    metaRelease datetime null,
    dataRelease datetime null,
    dataProductType varchar(64) null,
    calibrationLevel int null,

    provenance_name varchar(64) null,
    provenance_reference text null,
    provenance_version varchar(64) null,
    provenance_project varchar(256) null,
    provenance_producer varchar(256) null,
    provenance_runID varchar(256) null,
    provenance_lastExecuted datetime null,
    provenance_keywords text null,
    provenance_inputs text null,

    metrics_sourceNumberDensity double precision null,
    metrics_background double precision null,
    metrics_backgroundStddev double precision null,
    metrics_fluxDensityLimit double precision null,
    metrics_magLimit double precision null,

    quality_flag varchar(16) null,

-- internal
    obsID bigint not null references caom2_Observation (obsID),
    planeID bigint not null  primary key nonclustered,
    lastModified datetime not null,
    maxLastModified datetime not null,
    stateCode int not null,
    metaChecksum varchar(36),
    accMetaChecksum varchar(36)
)
lock datarows
;

create clustered index observation2plane on caom2_Plane (obsID)
;

create index lastModified on caom2_Plane (lastModified)
;


-- reference/join table for provanence 
-- not currently used/tested
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
