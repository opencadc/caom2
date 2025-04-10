
drop view if exists <schema>.ObsCore;

create view <schema>.ObsCore
(
    obs_collection,
    facility_name,
    instrument_name,
    target_name,
    obs_id,

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
    s_region,
    s_resolution,
    s_xel1, 
    s_xel2,

-- support spatial queries
    _q_position_bounds,
    _q_position_bounds_area,
    _q_position_bounds_centroid,

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

-- custom columns
    lastModified,

-- for CAOM access control
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
    p.position_bounds,
    p.position_resolution,
    p.position_dimension[1],
    p.position_dimension[2],
    p._q_position_bounds,
    p._q_position_bounds_area,
    p._q_position_bounds_centroid,

-- temporal axis
    p.time_bounds[1],
    p.time_bounds[2],
    p.time_exposure,
    p.time_resolution,
    p.time_dimension,

-- spectral axis
    p.energy_bounds[1],
    p.energy_bounds[2],
    p.energy_resolvingPower,
    p.energy_dimension,
-- em_ucd currently not known in caom2
    CAST(NULL AS varchar),

-- polarization axis
    p.polarization_states,
    p.polarization_dimension,

-- observable UCD: default to photon counts
    COALESCE(p.observable_ucd, CAST('phot.count' AS varchar)),

-- custom columns
    o.maxLastModified,

-- hidden columns    
    p.dataRelease, p.dataReadGroups,
    p.planeID,
    p.metaRelease, p.metaReadGroups

FROM <schema>.Observation o JOIN <schema>.Plane p ON o.obsID=p.obsID
WHERE o.intent = 'science' 
  AND p.calibrationLevel BETWEEN 0 AND 4
;
