
drop view if exists <schema>.ObsCore;

create view <schema>.ObsCore
(
    dataproduct_type,
    calib_level,
    obs_collection,
    facility_name,
    instrument_name,
    obs_id,
    obs_publisher_did,
    obs_release_date,

    access_url,
    access_format,
    access_estsize,

    target_name,

    s_ra,
    s_dec,
    s_fov,
    s_region,
    s_resolution,
    s_xel1, 
    s_xel2,

-- support CENTROID(s_region) function
    position_bounds_center,
-- support AREA(s_region) function
    position_bounds_area,

    t_min,
    t_max,
    t_exptime,
    t_resolution,
    t_xel,

    em_min,
    em_max,
    em_res_power,
    em_xel,
    em_ucd,

    pol_states,
    pol_xel,

    o_ucd,

-- primary key: this should be obs_id and obsID renamed to obs_creator_did
    core_id,

-- custom columns
    lastModified,

-- hidden columns (not in tap_schema)
    position_bounds_spoly,

-- for CAOM access control
    dataRelease, dataReadAccessGroups,
    planeID,
    metaRelease, metaReadAccessGroups
)
AS SELECT
    p.dataProductType,
    p.calibrationLevel,
    o.collection,
    o.telescope_name,
    o.instrument_name,
    o.observationID,
    p.publisherID,
    p.dataRelease,

-- access
    p.publisherID,
    CAST('application/x-votable+xml;content=datalink' AS varchar),
    CAST(NULL AS bigint),

    o.target_name,

-- spatial axis
    degrees(long(p.position_bounds_center)),
    degrees(lat(p.position_bounds_center)),
    p.position_bounds_size,
    p.position_bounds,
    p.position_resolution,
    p.position_dimension_naxis1,
    p.position_dimension_naxis2,
    p.position_bounds_center,
    p.position_bounds_area,

-- temporal axis
    p.time_bounds_lower,
    p.time_bounds_upper,
    p.time_exposure,
    p.time_resolution,
    p.time_dimension,

-- spectral axis
    p.energy_bounds_lower,
    p.energy_bounds_upper,
    p.energy_resolvingPower,
    p.energy_dimension,
-- em_ucd currently not known in caom2
    CAST(NULL AS varchar),

-- polarization axis
    p.polarization_states,
    p.polarization_dimension,

-- observable axis
    CAST('phot.count' AS varchar),

-- primary key
    p.planeID,

-- custom columns
    o.maxLastModified,

-- hidden columns    
    p.position_bounds_spoly,
    p.dataRelease, p.dataReadAccessGroups,
    p.planeID,
    p.metaRelease, p.metaReadAccessGroups

FROM <schema>.Observation o JOIN <schema>.Plane p ON o.obsID=p.obsID
WHERE o.intent = 'science' 
  AND p.calibrationLevel BETWEEN 0 AND 4
  AND p.dataProductType NOT LIKE 'http:%'
  AND p.dataProductType != 'eventlist'
;
