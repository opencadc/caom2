
-- case-sensitive
create index i_collection_instrument
    on caom2.Observation (collection, instrument_name)
    tablespace caom_index
;

create index i_telescope_instrument
    on caom2.Observation (telescope_name, instrument_name)
    tablespace caom_index
;

create index Plane_i_emBand_dataProductType
    on caom2.Plane (energy_emBand,dataProductType)
    tablespace caom_index
;

create index i_bandpassName
    on caom2.Plane (energy_bandpassName)
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
    where proposal_id is not null
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
    where proposal_id is not null
;

create index Observation_i_telescope_keywords_lower_pattern
    on caom2.Observation ( lower(telescope_keywords) varchar_pattern_ops )
    tablespace caom_index
    where telescope_keywords is not null
;

create index Observation_i_instrument_keywords_lower_pattern
    on caom2.Observation ( lower(instrument_keywords) varchar_pattern_ops )
    tablespace caom_index
    where instrument_keywords is not null
;

create index Observation_i_proposal_title_lower_pattern
    on caom2.Observation ( lower(proposal_title) varchar_pattern_ops )
    tablespace caom_index
    where proposal_title is not null
;

create index Observation_i_proposal_keywords_lower_pattern
    on caom2.Observation ( lower(proposal_keywords) varchar_pattern_ops )
    tablespace caom_index
    where proposal_keywords is not null
;

create index Plane_i_provenance_keywords_lower_pattern
    on caom2.Plane ( lower(provenance_keywords) varchar_pattern_ops )
    tablespace caom_index
    where provenance_keywords is not null
;

create index Artifact_i_uri
    on caom2.Artifact ( uri )
    tablespace caom_index
;
