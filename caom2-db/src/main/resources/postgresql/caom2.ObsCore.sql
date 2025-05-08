
drop view if exists <schema>.ObsCore;

create view <schema>.ObsCore
(
-- observation
    obs_collection,
    facility_name,
    instrument_name,
    target_name,
    obs_id,
-- plane
    obs_creator_did,
    obs_publisher_did,
    obs_release_date,
    dataproduct_type,
    calib_level,

    access_url,
    access_format,
    access_estsize,

    s_ra,
    s_dec,
    s_fov,
    s_fov_min,
    s_region,
    s_resolution,
    s_resolution_min,
    s_resolution_max,
    s_xel1, 
    s_xel2,

    em_min,
    em_max,
    em_res_power,
    em_resolution,
    em_resolution_min,
    em_resolution_max,
    em_xel,
    em_ucd,

    t_min,
    t_max,
    t_exptime,
    t_exptime_min,
    t_exptime_max,
    t_resolution,
    t_xel,

    pol_states,
    pol_xel,

    o_ucd,

-- custom columns
    lastModified,

-- support spatial queries and functions
    _q_position_bounds,
    _q_position_bounds_area,
    _q_position_bounds_centroid,

-- caom2 access control
    dataRelease, dataReadGroups,
    planeID,
    metaRelease, metaReadGroups
)
AS SELECT
    o.collection,
    o.telescope_name,
    o.instrument_name,
    o.target_name,
    o.uri,

    p.uri,
    p.publisherID,
    p.dataRelease,
    p.dataProductType,
    p.calibrationLevel,

-- access
    p.publisherID,
    CAST('application/x-votable+xml;content=datalink' AS varchar),
    CAST(NULL AS bigint),

-- spatial axis
    degrees(long(p._q_position_bounds_centroid)),
    degrees(lat(p._q_position_bounds_centroid)),
    p._q_position_bounds_size,
    p._q_position_minBounds_size,
    p.position_bounds,
    p.position_resolution,
    p.position_resolutionBounds[1],
    p.position_resolutionBounds[2],
    p.position_dimension[1],
    p.position_dimension[2],

-- spectral axis
    p.energy_bounds[1],
    p.energy_bounds[2],
    p.energy_resolvingPower,
    p.energy_resolution,
    p.energy_resolutionBounds[1],
    p.energy_resolutionBounds[2],
    p.energy_dimension,
-- em_ucd currently not known in caom2
    CAST(NULL AS varchar),

-- temporal axis
    p.time_bounds[1],
    p.time_bounds[2],
    p.time_exposure,
    p.time_exposureBounds[1],
    p.time_exposureBounds[2],
    p.time_resolution,
    p.time_dimension,

-- polarization axis
    p.polarization_states,
    p.polarization_dimension,

-- observable UCD: default to photon counts
    COALESCE(p.observable_ucd, CAST('phot.count' AS varchar)),

-- custom columns
    o.maxLastModified,

-- query optimisation
    p._q_position_bounds,
    p._q_position_bounds_area,
    p._q_position_bounds_centroid,

-- hidden columns    
    p.dataRelease, p.dataReadGroups,
    p.planeID,
    p.metaRelease, p.metaReadGroups

FROM <schema>.Observation o JOIN <schema>.Plane p ON o.obsID=p.obsID
WHERE o.intent = 'science' 
  AND p.calibrationLevel BETWEEN 0 AND 4
;
