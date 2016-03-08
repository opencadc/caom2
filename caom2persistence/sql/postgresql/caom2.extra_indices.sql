
-- position
create index Plane_position_i1
        on caom2.Plane using gist (position_bounds)
tablespace caom_index
;

create index Plane_position_i2
        on caom2.Plane using gist (position_bounds_center)
tablespace caom_index
;
create index Plane_position_i3
        on caom2.Plane (position_bounds_area)
tablespace caom_index
;

-- energy
create index Plane_energy_ib
      on caom2.Plane using gist (energy_bounds)
tablespace caom_index
;
create index Plane_energy_ibw
        on caom2.Plane (energy_bounds_width)
tablespace caom_index
;

create index Plane_energy_ib1
        on caom2.Plane (energy_bounds_cval1)
tablespace caom_index
;
create index Plane_energy_ib2
        on caom2.Plane (energy_bounds_cval2)
tablespace caom_index
;

create index Plane_energy_iss
        on caom2.Plane (energy_sampleSize)
tablespace caom_index
;

create index Plane_energy_irw
        on caom2.Plane (energy_restwav)
tablespace caom_index
where energy_restwav is not null
;

-- time
create index Plane_time_ib
      on caom2.Plane using gist (time_bounds)
tablespace caom_index
;
create index Plane_time_ibw
        on caom2.Plane (time_bounds_width)
tablespace caom_index
;

create index Plane_time_ib1
        on caom2.Plane (time_bounds_cval1)
tablespace caom_index
;
create index Plane_time_ib2
        on caom2.Plane (time_bounds_cval2)
tablespace caom_index
;

create index Plane_polar_is
        on caom2.Plane (polarization_states)
tablespace caom_index
;

-- case-sensitive
create index i_collection_instrument
    on caom2.Observation (uri_collection, instrument_name)
    tablespace caom_index
;

-- hardware config: telescope,instrument
create index i_telescope_instrument
    on caom2.Observation (telescope_name, instrument_name)
    tablespace caom_index
;

-- astronomical result search: emBand,dataProductType,calibrationLevel
create index Plane_i_emBand_dataProductType
    on caom2.Plane (energy_emBand,dataProductType)
    tablespace caom_index
;

-- aka filter 
create index i_bandpassName
    on caom2.Plane (energy_bandpassName)
    tablespace caom_index
;

create index i_provenance_runid
    on caom2.Plane (provenance_runID)
    tablespace caom_index
;

-- case-insensitive

create index Observation_i_collectionID_lower
    on caom2.Observation ( lower(uri_observationID) )
    tablespace caom_index
;

create index Observation_i_targ_lower
    on caom2.Observation ( (lower(target_name)) )
    tablespace caom_index
;

create index Observation_i_proposal_id_lower
    on caom2.Observation ( lower(proposal_id) )
    tablespace caom_index
;

-- support both case-insensitive and LIKE via special operator

create index Observation_i_observationID_lower_pattern
    on caom2.Observation ( lower(uri_observationID)  varchar_pattern_ops )
    tablespace caom_index
;

create index Observation_i_targ_lower_pattern
    on caom2.Observation ( (lower(target_name))  varchar_pattern_ops )
    tablespace caom_index
;

create index Observation_i_proposal_id_lower_pattern
    on caom2.Observation ( lower(proposal_id)  varchar_pattern_ops )
    tablespace caom_index
;

-- change: remove index(lower(telescope_keywords))
-- change: index(telescope_keywords) for tsvector @@ tsquery
create index Observation_i_tel_kw
    on caom2.Observation using GIN (telescope_keywords)
    tablespace caom_index
;

-- change: remove index(lower(instrument_keywords))
-- change: index(instrument_keywords) for tsvector @@ tsquery
create index Observation_i_instr_kw
    on caom2.Observation using GIN (instrument_keywords)
    tablespace caom_index
;

-- change: index(target_keywords) for tsvector @@ tsquery
create index Observation_i_targ_kw
    on caom2.Observation using GIN (target_keywords)
    tablespace caom_index
;

-- change: remove index(lower(proposal_title))

-- change: remove index(lower(proposal_keywords))
-- change: index(proposal_keywords) for tsvector @@ tsquery
create index Observation_i_prop_kw
    on caom2.Observation using GIN (proposal_keywords)
    tablespace caom_index
;

-- change: remove index(lower(provenance_keywords))
-- change: index(provenance_keywords) for tsvector @@ tsquery
create index Plane_i_prov_kw
    on caom2.Plane using GIN (provenance_keywords)
    tablespace caom_index
;

create index Plane_i_dataRelease
    on caom2.Plane ( dataRelease )
    tablespace caom_index
;

create index Artifact_i_uri
    on caom2.Artifact ( uri )
    tablespace caom_index
;
