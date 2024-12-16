
create table <schema>.Plane
(
    productID varchar(128) not null,
    publisherID varchar(512) not null,
    planeURI varchar(512) not null,
    creatorID varchar(512),

    metaRelease timestamp,
    dataRelease timestamp,
    dataProductType varchar(64),
    calibrationLevel integer,

    observable_ucd varchar(64),

-- provenance
    provenance_name varchar(64),
    provenance_reference text,
    provenance_version varchar(64),
    provenance_project varchar(256),
    provenance_producer varchar(256),
    provenance_runID varchar(256),
    provenance_lastExecuted timestamp,
    provenance_keywords citext,
    provenance_inputs text,

-- metrics
    metrics_sourceNumberDensity double precision,
    metrics_background double precision,
    metrics_backgroundStddev double precision,
    metrics_fluxDensityLimit double precision,
    metrics_magLimit double precision,
    metrics_sampleSNR double precision,

    quality_flag varchar(16),

-- position
    position_bounds          double precision[],
    position_bounds_spoly    spoly,
    position_bounds_samples  double precision[],
    position_bounds_center   spoint,
    position_bounds_area     double precision,
    position_bounds_size     double precision,
    position_dimension_naxis1 bigint,
    position_dimension_naxis2 bigint,
    position_resolution      double precision,
    position_resolutionBounds polygon,
    position_resolutionBounds_lower double precision,
    position_resolutionBounds_upper double precision,
    position_sampleSize      double precision,
    position_timeDependent   integer,

-- energy
    energy_energyBands       varchar(64),
    energy_bounds            polygon, 
    energy_bounds_lower      double precision,
    energy_bounds_upper      double precision,
    energy_bounds_width      double precision,
    energy_bounds_samples    polygon,
    energy_dimension         bigint,
    energy_resolvingPower    double precision,
    energy_resolvingPowerBounds polygon,
    energy_resolvingPowerBounds_lower double precision,
    energy_resolvingPowerBounds_upper double precision,
    energy_sampleSize        double precision,
    energy_bandpassName      varchar(64),
    energy_transition_species varchar(32),
    energy_transition_transition varchar(32),
    energy_freqWidth         double precision,
    energy_freqSampleSize    double precision,
    energy_restwav           double precision,

-- time
    time_bounds             polygon,
    time_bounds_lower       double precision,
    time_bounds_upper       double precision,
    time_bounds_width       double precision,
    time_bounds_samples     polygon,
    time_dimension          bigint,
    time_resolution         double precision,
    time_resolutionBounds   polygon,
    time_resolutionBounds_lower double precision,
    time_resolutionBounds_upper double precision,
    time_sampleSize         double precision,
    time_exposure           double precision,

-- polarization
    polarization_states     varchar(34),
-- this is long in the code but integer in db for historical reasons
    polarization_dimension  integer,

-- custom axis
    custom_ctype            varchar(8),
    custom_bounds           polygon,
    custom_bounds_lower     double precision,
    custom_bounds_upper     double precision,
    custom_bounds_width     double precision,
    custom_bounds_samples   polygon,
    custom_dimension        bigint,

    metaReadGroups text,
    dataReadGroups text,

-- optimisation
    dataReadAccessGroups tsvector default '',
    metaReadAccessGroups tsvector default '',

-- internal
    obsID uuid not null references <schema>.Observation (obsID),
    planeID uuid not null  primary key,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    metaChecksum varchar(136) not null,
    accMetaChecksum varchar(136) not null,
    metaProducer varchar(128)
)
;

-- this is for Observation join Plane
create index i_obsID on <schema>.Plane (obsID)
;

-- tag the clustering index
cluster i_obsID on <schema>.Plane
;

create unique index i_planeURI on <schema>.Plane(planeURI)
;

create unique index i_publisherID on <schema>.Plane(publisherID)
;

create unique index i_creatorID on <schema>.Plane(creatorID)
    where creatorID is not null
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

