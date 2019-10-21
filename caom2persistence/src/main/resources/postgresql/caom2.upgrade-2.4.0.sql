
alter table <schema>.Observation 
    add column target_id   varchar(128),
    add column metaProducer varchar(128);

drop index i_composite2simple;
drop index i_simple2composite;

alter table <schema>.ObservationMember
    rename column compositeID to parentID;

alter table <schema>.ObservationMember
    rename column simpleID to memberID;

create unique index i_parent2member on <schema>.ObservationMember (parentID,memberID);

create unique index i_member2parent on <schema>.ObservationMember (memberID,parentID);

alter table <schema>.Plane 
    add column observable_ucd varchar(64),
    add column metrics_sampleSNR double precision,
    add column position_resolutionBounds polygon,
    add column position_resolutionBounds_lower double precision,
    add column position_resolutionBounds_upper double precision,
    add column energy_resolvingPowerBounds polygon,
    add column energy_resolvingPowerBounds_lower double precision,
    add column energy_resolvingPowerBounds_upper double precision,
    add column time_resolutionBounds polygon,
    add column time_resolutionBounds_lower double precision,
    add column time_resolutionBounds_upper double precision,
    add column custom_ctype varchar(16),
    add column custom_bounds polygon,
    add column custom_bounds_lower double precision,
    add column custom_bounds_upper double precision,
    add column custom_bounds_width double precision,
    add column custom_bounds_samples polygon,
    add column custom_dimension bigint,
    add column metaProducer varchar(128);

-- rename to match model
alter table <schema>.Plane
    rename column energy_emBand to energy_energyBands;

-- make room for all 8 values plus 7 separators; round up
alter table <schema>.Plane
    alter column energy_energyBands set data type varchar(64);

alter table <schema>.Artifact
    add column contentRelease timestamp,
    add column contentReadGroups text,
    add column metaProducer varchar(128);

alter table <schema>.Part
    add column metaProducer varchar(128);

alter table <schema>.Chunk
    add column customAxis integer,
    add column custom_axis_axis_ctype varchar(16),
    add column custom_axis_axis_cunit varchar(16),
    add column custom_axis_error_syser double precision,
    add column custom_axis_error_rnder double precision,
    add column custom_axis_range_start_pix double precision,
    add column custom_axis_range_start_val double precision,
    add column custom_axis_range_end_pix double precision,
    add column custom_axis_range_end_val double precision,
    add column custom_axis_bounds text,
    add column custom_axis_function_naxis bigint,
    add column custom_axis_function_refCoord_pix double precision,
    add column custom_axis_function_refCoord_val double precision,
    add column custom_axis_function_delta double precision,
    add column metaProducer varchar(128);

drop index if exists Plane_i_emBand_dataProductType;

