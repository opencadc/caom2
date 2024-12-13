
-- manual revert from 2.4.0 to 2.3.33
-- note: you may have to manually change the schema

start transaction;

alter table caom2.Observation 
    drop column target_targetID, 
    drop column metaProducer;

drop index caom2.i_parent2member;
drop index caom2.i_member2parent;

alter table caom2.ObservationMember
    rename column parentID to compositeID;

alter table caom2.ObservationMember
    rename column memberID to simpleID;

create unique index i_composite2simple on caom2.ObservationMember (compositeID,simpleID);

create unique index i_simple2composite on caom2.ObservationMember (simpleID,compositeID);

alter table caom2.Plane 
    drop column observable_ucd,
    drop column metrics_sampleSNR,
    drop column position_resolutionBounds,
    drop column position_resolutionBounds_lower,
    drop column position_resolutionBounds_upper,
    drop column energy_resolvingPowerBounds,
    drop column energy_resolvingPowerBounds_lower,
    drop column energy_resolvingPowerBounds_upper,
    drop column time_resolutionBounds,
    drop column time_resolutionBounds_lower,
    drop column time_resolutionBounds_upper,
    drop column custom_ctype,
    drop column custom_bounds,
    drop column custom_bounds_lower,
    drop column custom_bounds_upper,
    drop column custom_bounds_width,
    drop column custom_bounds_samples,
    drop column custom_dimension,
    drop column metaProducer;

-- harmless to leave this change rather than shrink it back
--alter table caom2.Plane
--    alter column energy_energyBands set data type varchar(64);

-- rename to match model
alter table caom2.Plane
    rename column energy_energyBands to energy_emBand;

alter table caom2.Artifact
    drop column contentRelease,
    drop column contentReadGroups,
    drop column metaProducer;

alter table caom2.Part
    drop column metaProducer;

alter table caom2.Chunk
    drop column customAxis,
    drop column custom_axis_axis_ctype,
    drop column custom_axis_axis_cunit,
    drop column custom_axis_error_syser,
    drop column custom_axis_error_rnder,
    drop column custom_axis_range_start_pix,
    drop column custom_axis_range_start_val,
    drop column custom_axis_range_end_pix,
    drop column custom_axis_range_end_val,
    drop column custom_axis_bounds,
    drop column custom_axis_function_naxis,
    drop column custom_axis_function_refCoord_pix,
    drop column custom_axis_function_refCoord_val,
    drop column custom_axis_function_delta,
    drop column metaProducer;

update caom2.ModelVersion set version='2.3.33'
where model = 'CAOM';

commit;