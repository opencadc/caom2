
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
    on <schema>.Plane (polarization_states COLLATE "C")
    where polarization_states is not null
;

create index i_collection_instrument
    on <schema>.Observation (collection COLLATE "C", instrument_name COLLATE "C")
;

create index i_telescope_instrument
    on <schema>.Observation (telescope_name COLLATE "C", instrument_name COLLATE "C")
;

create index Plane_i_emBand_dataProductType
    on <schema>.Plane (energy_emBand COLLATE "C", dataProductType COLLATE "C")
;

create index i_bandpassName
    on <schema>.Plane (energy_bandpassName COLLATE "C")
    where energy_bandpassName is not null
;

create index i_provenance_runid
    on <schema>.Plane (provenance_runID COLLATE "C")
    where provenance_runID is not null
;

create index Observation_i_observationID_lower
    on <schema>.Observation ( lower(observationID)  COLLATE "C")
;

create index Observation_i_targ_lower
    on <schema>.Observation ( (lower(target_name))  COLLATE "C")
    where target_name is not null
;

create index Observation_i_proposal_id_lower
    on <schema>.Observation ( lower(proposal_id)  COLLATE "C")
    where proposal_id is not null
;

create index Artifact_i_uri
    on <schema>.Artifact ( uri  COLLATE "C" )
;

create index Plane_i_dataRelease
    on <schema>.Plane ( dataRelease )
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