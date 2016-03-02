
-- data discovery indices

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

create index Plane_energy_i1
      on caom2.Plane using gist (energy_bounds)
tablespace caom_index
;
create index Plane_energy_i2
        on caom2.Plane (energy_bounds_width)
tablespace caom_index
;

-- TODO: explain/analyze to see if tw-columns actually help
create index Plane_energy_i3
        on caom2.Plane (energy_bounds_cval1,energy_bounds_cval2) -- fix
tablespace caom_index
;
-- TODO: explain/analyze to see if we also need index(energy_bounds_cval2,energy_bounds_cval1)

create index Plane_energy_i4
        on caom2.Plane (energy_sampleSize)
tablespace caom_index
;

create index Plane_energy_i5
        on caom2.Plane (energy_restwav)
tablespace caom_index
where energy_restwav is not null
;

create index Plane_time_i1
      on caom2.Plane using gist (time_bounds)
tablespace caom_index
;
create index Plane_time_i2
        on caom2.Plane (time_bounds_width)
tablespace caom_index
;

-- TODO: explain/analyze as above
create index Plane_time_i3
        on caom2.Plane (time_bounds_cval1,time_bounds_cval2) -- fix
tablespace caom_index
;
create index Plane_time_i4
        on caom2.Plane (time_bounds_cval2,time_bounds_cval1) -- fix
tablespace caom_index
;

create index Plane_polar_i1
        on caom2.Plane (polarization_states)
tablespace caom_index
;

-- case-sensitive
create index i_collection_instrument
    on caom2.Observation (collection, instrument_name)
    tablespace caom_index
;

-- hardware config search: telescope,instrument,filter
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
    on caom2.Observation ( lower(observationID) )
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
    on caom2.Observation ( lower(observationID)  varchar_pattern_ops )
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
create index Observation_i_telescope_keywords_lower_pattern
    on caom2.Observation ( lower(telescope_keywords) varchar_pattern_ops )
    tablespace caom_index
;
-- change: remove index(telescope_keywords) for tsvector @@ tsquery
--create index Observation_i_telescope_keywords
--    on caom2.Observation using GIN (telescope_keywords)
--    tablespace caom_index
--;

-- change: remove index(lower(instrument_keywords))
create index Observation_i_instrument_keywords_lower_pattern
    on caom2.Observation ( lower(instrument_keywords) varchar_pattern_ops )
    tablespace caom_index
;
-- change: remove index(instrument_keywords) for tsvector @@ tsquery
--create index Observation_i_instrument_keywords
--    on caom2.Observation using GIN (instrument_keywords)
--    tablespace caom_index
--;
-- change: remove index(lower(proposal_title))
create index Observation_i_proposal_title_lower_pattern
    on caom2.Observation ( lower(proposal_title) varchar_pattern_ops )
    tablespace caom_index
;

-- change: remove index(lower(proposal_keywords))
create index Observation_i_proposal_keywords_lower_pattern
    on caom2.Observation ( lower(proposal_keywords) varchar_pattern_ops )
    tablespace caom_index
;
-- change: remove index(proposal_keywords) for tsvector @@ tsquery
--create index Observation_i_proposal_keywords
--    on caom2.Observation using GIN (proposal_keywords)
--    tablespace caom_index
--;

-- change: remove index(lower(provenance_keywords))
create index Plane_i_provenance_keywords_lower_pattern
    on caom2.Plane ( lower(provenance_keywords) varchar_pattern_ops )
    tablespace caom_index
;
-- change: remove index(provenance_keywords) for tsvector @@ tsquery
--create index Observation_i_provenance_keywords
--    on caom2.Observation using GIN (provenance_keywords)
--    tablespace caom_index
--;

create index Plane_i_dataRelease
    on caom2.Plane ( dataRelease )
    tablespace caom_index
;

create index Artifact_i_uri
    on caom2.Artifact ( uri )
    tablespace caom_index
;
