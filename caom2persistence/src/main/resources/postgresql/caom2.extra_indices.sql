
-- position
create index Plane_position_i1
    on <schema>.Plane using gist (position_bounds_spoly)
;

-- energy
create index Plane_energy_ib
    on <schema>.Plane using gist (energy_bounds)
;

create index Plane_energy_ib1
    on <schema>.Plane (energy_bounds_lower)
;
create index Plane_energy_ib2
    on <schema>.Plane (energy_bounds_upper)
;

create index Plane_energy_irw
    on <schema>.Plane (energy_restwav)
where energy_restwav is not null
;

-- time
create index Plane_time_ib
    on <schema>.Plane using gist (time_bounds)
;

create index Plane_time_ib1
    on <schema>.Plane (time_bounds_lower)
;
create index Plane_time_ib2
    on <schema>.Plane (time_bounds_upper)
;
create index Plane_time_ibw
    on <schema>.Plane (time_bounds_width)
;

create index plane_pol_states_pattern
    on <schema>.Plane (polarization_states varchar_pattern_ops)
    where polarization_states is not null
;

create index Observation_i_observationID_lower
    on <schema>.Observation ( lower(observationID) )
;
create index Observation_i_observationID_lower_pattern
    on <schema>.Observation ( lower(observationID)  varchar_pattern_ops)
;

create index Observation_i_targ_lower
    on <schema>.Observation ( lower(target_name) )
    where target_name is not null
;
create index Observation_i_targ_lower_pattern
    on <schema>.Observation ( lower(target_name) varchar_pattern_ops)
    where target_name is not null
;

create index Observation_i_proposal_id_lower
    on <schema>.Observation ( lower(proposal_id))
    where proposal_id is not null
;
create index Observation_i_proposal_id_lower_pattern
    on <schema>.Observation ( lower(proposal_id) varchar_pattern_ops)
    where proposal_id is not null
;

create index i_collection_instrument 
    on <schema>.Observation (collection,instrument_name)
;
create index i_telescope_instrument
    on <schema>.Observation (telescope_name,instrument_name)
;
create index i_instrument
    on <schema>.Observation (instrument_name)
;
create index i_instrument_pattern
    on <schema>.Observation (instrument_name varchar_pattern_ops)
;

create index i_telescope
    on <schema>.Observation (telescope_name)
;

create index i_bandpassName
    on <schema>.Plane (energy_bandpassName)
    where energy_bandpassName is not null
;
create index i_bandpassName_pattern
    on <schema>.Plane (energy_bandpassName varchar_pattern_ops)
    where energy_bandpassName is not null
;

create index i_provenance_runid
    on <schema>.Plane (provenance_runID)
    where provenance_runID is not null
;

create index Plane_i_dataRelease
    on <schema>.Plane ( dataRelease )
;

create index Artifact_i_uri_pattern
    on <schema>.Artifact (uri varchar_pattern_ops)
;
