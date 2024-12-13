
-- to modify this file with a specific <schema>, try:
-- cat caom2.upgrade-indices-manual-2.3.15.sql | sed 's/<schema>/caom2/g' > doit.sql

-- add new pattern matching indices and recreate some indices as partial

--drop index if exists <schema>.i_instrument;
create index i_instrument
    on <schema>.Observation (instrument_name)
;

--drop index if exists <schema>.i_instrument_pattern;
create index i_instrument_pattern
    on <schema>.Observation (instrument_name varchar_pattern_ops)
;

--drop index if exists <schema>.i_telescope;
create index i_telescope
    on <schema>.Observation (telescope_name)
;

--drop index if exists <schema>.i_telescope_pattern;
create index i_telescope_pattern
    on <schema>.Observation (telescope_name varchar_pattern_ops)
;

-- partial
drop index if exists <schema>.i_bandpassName;
create index i_bandpassName
    on <schema>.Plane (energy_bandpassName)
    where energy_bandpassName is not null
;

-- partial+pattern
drop index if exists <schema>.i_bandpassName_pattern;
create index i_bandpassName_pattern
    on <schema>.Plane (energy_bandpassName varchar_pattern_ops)
    where energy_bandpassName is not null
;

-- change name, partial+pattern
drop index if exists <schema>.Plane_polar_is;
create index plane_pol_states_pattern
    on <schema>.Plane (polarization_states varchar_pattern_ops)
    where polarization_states is not null
;

-- partial
drop index if exists <schema>.i_provenance_runid;
create index i_provenance_runid
    on <schema>.Plane (provenance_runID)
    where provenance_runID is not null
;

-- partial+pattern
drop index if exists <schema>.i_provenance_runid_pattern;
create index i_provenance_runid_pattern
    on <schema>.Plane (provenance_runID varchar_pattern_ops)
    where provenance_runID is not null
;

-- pattern
drop index if exists <schema>.Artifact_i_uri_pattern;
create index Artifact_i_uri_pattern
    on <schema>.Artifact (uri varchar_pattern_ops)
;
