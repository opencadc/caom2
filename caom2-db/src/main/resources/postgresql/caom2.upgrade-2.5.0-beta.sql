-- add internal query optimization columns

alter table <schema>.Observation add column _q_targetPosition_coordinates spoint;

alter table <schema>.Plane 
    add column _q_position_bounds              spoly,
    add column _q_position_bounds_centroid     spoint,
    add column _q_position_bounds_area         double precision,
    add column _q_position_bounds_size         double precision,
    add column _q_position_minBounds           spoly,
    add column _q_position_minBounds_centroid  spoint,
    add column _q_position_minBounds_area      double precision,
    add column _q_position_minBounds_size      double precision,
    add column _q_position_maxRecoverableScale polygon,
    add column _q_position_resolutionBounds    polygon,
    add column _q_energy_bounds                polygon,
    add column _q_energy_samples               polygon,
    add column _q_energy_resolvingPowerBounds  polygon,
    add column _q_energy_resolutionBounds      polygon,
    add column _q_time_bounds                  polygon,
    add column _q_time_samples                 polygon,
    add column _q_time_exposureBounds          polygon,
    add column _q_time_resolutionBounds        polygon,
    add column _q_custom_bounds                polygon,
    add column _q_custom_samples               polygon,
    add column _q_uv_distance                  polygon;