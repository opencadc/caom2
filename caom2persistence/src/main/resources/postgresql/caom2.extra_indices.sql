
-- position
create index Plane_position_i1
    on <schema>.Plane using gist (position_bounds)
;

create index Plane_position_i2
    on <schema>.Plane using gist (position_bounds_center)
;
create index Plane_position_i3
    on <schema>.Plane (position_bounds_area)
;

-- energy
create index Plane_energy_ib
    on <schema>.Plane using gist (energy_bounds)
;
create index Plane_energy_ibw
    on <schema>.Plane (energy_bounds_width)
;

create index Plane_energy_ib1
    on <schema>.Plane (energy_bounds_lower)
;
create index Plane_energy_ib2
    on <schema>.Plane (energy_bounds_upper)
;

create index Plane_energy_iss
    on <schema>.Plane (energy_sampleSize)
;

create index Plane_energy_irw
    on <schema>.Plane (energy_restwav)
where energy_restwav is not null
;

-- time
create index Plane_time_ib
    on <schema>.Plane using gist (time_bounds)
;
create index Plane_time_ibw
    on <schema>.Plane (time_bounds_width)
;

create index Plane_time_ib1
    on <schema>.Plane (time_bounds_lower)
;
create index Plane_time_ib2
    on <schema>.Plane (time_bounds_upper)
;

create index Plane_polar_is
    on <schema>.Plane (polarization_states)
;

-- case-sensitive
create index i_collection_instrument
    on <schema>.Observation (collection, instrument_name)
;

-- hardware config: telescope,instrument
create index i_telescope_instrument
    on <schema>.Observation (telescope_name, instrument_name)
;

-- astronomical result search: emBand,dataProductType,calibrationLevel
create index Plane_i_emBand_dataProductType
    on <schema>.Plane (energy_emBand,dataProductType)
;

-- aka filter 
create index i_bandpassName
    on <schema>.Plane (energy_bandpassName)
;

create index i_provenance_runid
    on <schema>.Plane (provenance_runID)
;

-- case-insensitive

create index Observation_i_observationID_lower
    on <schema>.Observation ( lower(observationID) )
;

create index Observation_i_targ_lower
    on <schema>.Observation ( (lower(target_name)) )
;

create index Observation_i_proposal_id_lower
    on <schema>.Observation ( lower(proposal_id) )
;

-- support both case-insensitive and LIKE via special operator

create index Observation_i_observationID_lower_pattern
    on <schema>.Observation ( lower(observationID)  varchar_pattern_ops )
;

create index Observation_i_targ_lower_pattern
    on <schema>.Observation ( (lower(target_name))  varchar_pattern_ops )
;

create index Observation_i_proposal_id_lower_pattern
    on <schema>.Observation ( lower(proposal_id)  varchar_pattern_ops )
;

create index Plane_i_dataRelease
    on <schema>.Plane ( dataRelease )
;

-- support for SODA service and file authorization queries 
create index Artifact_i_uri
    on <schema>.Artifact ( uri )
;

--create index Observation_i_tel_kw
--    on <schema>.Observation (telescope_keywords)
--;

--create index Observation_i_instr_kw
--    on <schema>.Observation (instrument_keywords)
--;

--create index Observation_i_targ_kw
--    on <schema>.Observation (target_keywords)
--;

--create index Observation_i_prop_kw
--    on <schema>.Observation (proposal_keywords)
--;

--create index Plane_i_prov_kw
--    on <schema>.Plane (provenance_keywords)
--;