
create table <schema>.Plane
(
    uri varchar(512) not null,
    publisherID varchar(512) not null,

    metaRelease timestamp,
    metaReadGroups text[],
    dataRelease timestamp,
    dataReadGroups text[],
    dataProductType varchar(64),
    calibrationLevel integer,

-- provenance
    provenance_name varchar(64),
    provenance_reference text,
    provenance_version varchar(64),
    provenance_project varchar(256),
    provenance_producer varchar(256),
    provenance_runID varchar(256),
    provenance_lastExecuted timestamp,
    provenance_keywords citext,
    provenance_inputs text[],

-- metrics
    metrics_sourceNumberDensity double precision,
    metrics_background double precision,
    metrics_backgroundStddev double precision,
    metrics_fluxDensityLimit double precision,
    metrics_magLimit double precision,
    metrics_sampleSNR double precision,

    quality_flag varchar(16),

-- observable
    observable_ucd varchar(64),
    observable_calibration varchar(64),

-- position
    position_bounds              double precision[],
    position_samples             text,
    position_minBounds           double precision[],
    position_dimension           bigint[2],
    position_maxRecoverableScale double precision[2],
    position_resolution          double precision,
    position_resolutionBounds    double precision[2],
    position_sampleSize          double precision,
    position_calibration         varchar(64),
-- alternate representation for optimized query execution or ADQL function implementation
--    _q_position_bounds           spoly,
--    _q_position_minBounds        spoly,
--    _q_position_bounds_center    spoint,
--    _q_position_bounds_area      double precision,
--    _q_position_bounds_size      double precision,
--    _q_position_maxRecoverableScale polygon,
--    _q_position_resolutionBounds polygon,

-- energy
    energy_bounds                   double precision[2],
    energy_samples                  double precision[],
    energy_bandpassName             varchar(64),
    energy_dimension                bigint,
    energy_energyBands              text,
    energy_resolvingPower           double precision,
    energy_resolvingPowerBounds     double precision[2],
    energy_resolution               double precision,
    energy_resolutionBounds         double precision[2],
    energy_sampleSize               double precision,
    energy_transition_species       varchar(32),
    energy_transition_transition    varchar(32),
    energy_rest                     double precision,
    energy_calibration              varchar(64),
-- alternate representation for optimized query execution or ADQL function implementation
--    _q_energy_bounds               polygon,
--    _q_energy_samples              polygon,
--    _q_energy_resolvingPowerBounds polygon,
--    _q_energy_resolutionBounds     polygon,

-- time
    time_bounds              double precision[2],
    time_samples             double precision[],
    time_dimension           bigint,
    time_exposure            double precision,
    time_exposureBounds      double precision[2],
    time_resolution          double precision,
    time_resolutionBounds    double precision[2],
    time_sampleSize          double precision,
    time_calibration         varchar(64),
-- alternate representation for optimized query execution or ADQL function implementation
--    _q_time_bounds           polygon,
--    _q_time_samples          polygon,
--    _q_time_exposureBounds   polygon,
--    _q_time_resolutionBounds polygon,
    
-- polarization
    polarization_states     text,
    polarization_dimension  integer,

-- custom axis
    custom_ctype            varchar(8),
    custom_bounds           double precision[2],
    custom_samples          double precision[],
    custom_dimension        bigint,
-- alternate representation for optimized query execution or ADQL function implementation
--    _q_custom_bounds        polygon,
--    _q_custom_samples       polygon,

-- visibility
    uv_distance                 double precision[2],
    uv_distributionEccentricity double precision,
    uv_distributionFill         double precision,
-- alternate representation for optimized query execution or ADQL function implementation
--    _q_uv_distance              polygon,

-- parent
    obsID uuid not null references <schema>.Observation (obsID),
-- entity
    planeID uuid not null  primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    metaProducer varchar(128),
-- caom entity
    maxLastModified timestamp not null,
    accMetaChecksum varchar(136) not null
)
;

-- this is for Observation join Plane
create index i_obsID on <schema>.Plane (obsID)
;

-- tag the clustering index
cluster i_obsID on <schema>.Plane
;

create unique index i_uri on <schema>.Plane(uri)
;

create unique index i_publisherID on <schema>.Plane(publisherID)
;

-- join table
create table <schema>.ProvenanceInput
(
    outputID uuid not null references <schema>.Plane (planeID),
    inputID varchar(512) not null
)
;

create unique index i_output2input on <schema>.ProvenanceInput (outputID,inputID)
;

create unique index i_input2output on <schema>.ProvenanceInput (inputID, outputID)
;

