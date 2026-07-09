
-- core observation metadata
create index i_observation_collection
    on <schema>.Observation (collection)
;
create index i_observation_telescope
    on <schema>.Observation (telescope_name)
;
create index i_observation_instrument
    on <schema>.Observation (instrument_name)
;

-- position
create index i_plane_position
    on <schema>.Plane using gist (_q_position_bounds)
    where _q_position_bounds is not null
;

-- energy
create index i_plane_energy
    on <schema>.Plane using gist (_q_energy_bounds)
    where _q_energy_bounds is not null
;

-- time
create index i_plane_time
    on <schema>.Plane using gist (_q_time_bounds)
    where _q_time_bounds is not null
;

create index i_plane_time_exposure
    on <schema>.Plane (time_exposure)
    where time_exposure is not null
;

-- polarization
create index _plane_polarization_states_pattern
    on <schema>.Plane (polarization_states varchar_pattern_ops)
    where polarization_states is not null
;

