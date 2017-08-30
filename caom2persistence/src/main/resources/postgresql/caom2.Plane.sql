
create table caom2.Plane
(
    productID varchar(64) not null,
    publisherID varchar(512) not null,
    planeURI varchar(512) not null,
    creatorID varchar(512),

    metaRelease timestamp,
    dataRelease timestamp,
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
    provenance_inputs text,

-- metrics
    metrics_sourceNumberDensity double precision,
    metrics_background double precision,
    metrics_backgroundStddev double precision,
    metrics_fluxDensityLimit double precision,
    metrics_magLimit double precision,

    quality_flag varchar(16),

-- position
    position_bounds          spoly,
    position_bounds_points   double precision[],
    position_bounds_center   spoint,
    position_bounds_area     double precision,
    position_bounds_size     double precision,
    position_bounds_samples  double precision[],
    position_dimension_naxis1 bigint,
    position_dimension_naxis2 bigint,
    position_resolution      double precision,
    position_sampleSize      double precision,
    position_timeDependent   integer,

-- energy
    energy_emband            varchar(32),
    energy_bounds            polygon, 
    energy_bounds_lower      double precision,
    energy_bounds_upper      double precision,
    energy_bounds_width      double precision,
    energy_bounds_integrated double precision,
    energy_bounds_samples    polygon,
    energy_dimension         bigint,
    energy_resolvingPower    double precision,
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
    time_bounds_integrated  double precision,
    time_bounds_samples     polygon,
    time_dimension          bigint,
    time_resolution         double precision,
    time_sampleSize         double precision,
    time_exposure           double precision,

-- polarization
    polarization_states     varchar(34),
-- this is long in the code but integer in db for historical reasons
    polarization_dimension  integer,

-- optimisation
    dataReadAccessGroups tsvector default '',
    metaReadAccessGroups tsvector default '',

-- internal
    obsID uuid not null references caom2.Observation (obsID),
    planeID uuid not null  primary key,
    lastModified timestamp not null,
    maxLastModified timestamp not null,
    stateCode int not null,
    metaChecksum varchar(136) not null,
    accMetaChecksum varchar(136) not null
)
;

-- this is for Observation join Plane
create index i_obsID on caom2.Plane (obsID)
;

-- tag the clustering index
cluster i_obsID on caom2.Plane
;

create table caom2.Plane_inputs
(
    outputID uuid not null references caom2.Plane (planeID),
    inputID uuid not null references caom2.Plane (planeID)
)
;

create unique index i_publisherID on caom2.Plane(publisherID)
;

create unique index i_creatorID on caom2.Plane(creatorID)
;

create unique index i_output2input on caom2.Plane_inputs (outputID,inputID)
;

create unique index i_input2output on caom2.Plane_inputs (inputID,outputID)
;

