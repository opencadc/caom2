
-- position
create index Plane_position_i1
    on caom2.Plane using gist (position_bounds)
;

create index Plane_position_i2
    on caom2.Plane using gist (position_bounds_center)
;
create index Plane_position_i3
    on caom2.Plane (position_bounds_area)
;

-- energy
create index Plane_energy_ib
    on caom2.Plane using gist (energy_bounds)
;
create index Plane_energy_ibw
    on caom2.Plane (energy_bounds_width)
;

create index Plane_energy_ib1
    on caom2.Plane (energy_bounds_lower)
;
create index Plane_energy_ib2
    on caom2.Plane (energy_bounds_upper)
;

create index Plane_energy_iss
    on caom2.Plane (energy_sampleSize)
;

create index Plane_energy_irw
    on caom2.Plane (energy_restwav)
where energy_restwav is not null
;

-- time
create index Plane_time_ib
    on caom2.Plane using gist (time_bounds)
;
create index Plane_time_ibw
    on caom2.Plane (time_bounds_width)
;

create index Plane_time_ib1
    on caom2.Plane (time_bounds_lower)
;
create index Plane_time_ib2
    on caom2.Plane (time_bounds_upper)
;

create index Plane_polar_is
    on caom2.Plane (polarization_states)
;

-- case-sensitive
create index i_collection_instrument
    on caom2.Observation (collection, instrument_name)
;

-- hardware config: telescope,instrument
create index i_telescope_instrument
    on caom2.Observation (telescope_name, instrument_name)
;

-- astronomical result search: emBand,dataProductType,calibrationLevel
create index Plane_i_emBand_dataProductType
    on caom2.Plane (energy_emBand,dataProductType)
;

-- aka filter 
create index i_bandpassName
    on caom2.Plane (energy_bandpassName)
;

create index i_provenance_runid
    on caom2.Plane (provenance_runID)
;

-- case-insensitive

create index Observation_i_observationID_lower
    on caom2.Observation ( lower(observationID) )
;

create index Observation_i_targ_lower
    on caom2.Observation ( (lower(target_name)) )
;

create index Observation_i_proposal_id_lower
    on caom2.Observation ( lower(proposal_id) )
;

-- support both case-insensitive and LIKE via special operator

create index Observation_i_observationID_lower_pattern
    on caom2.Observation ( lower(observationID)  varchar_pattern_ops )
;

create index Observation_i_targ_lower_pattern
    on caom2.Observation ( (lower(target_name))  varchar_pattern_ops )
;

create index Observation_i_proposal_id_lower_pattern
    on caom2.Observation ( lower(proposal_id)  varchar_pattern_ops )
;

create index Plane_i_dataRelease
    on caom2.Plane ( dataRelease )
;

-- support for SODA service and file authorization queries 
create index Artifact_i_uri
    on caom2.Artifact ( uri )
;

--create index Observation_i_tel_kw
--    on caom2.Observation (telescope_keywords)
--;

--create index Observation_i_instr_kw
--    on caom2.Observation (instrument_keywords)
--;

--create index Observation_i_targ_kw
--    on caom2.Observation (target_keywords)
--;

--create index Observation_i_prop_kw
--    on caom2.Observation (proposal_keywords)
--;

--create index Plane_i_prov_kw
--    on caom2.Plane (provenance_keywords)
--;