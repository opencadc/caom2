
drop view if exists caom2.ObsPart;
drop view if exists caom2.ObsFile;
drop view if exists caom2.ObsCore;
drop view if exists caom2.SIAv1;

-- bug fix: correct handling of keywords
drop index if exists Observation_i_instr_kw;
drop index if exists Observation_i_prop_kw;
drop index if exists Observation_i_targ_kw;
drop index if exists Observation_i_tel_kw;

alter table caom2.Observation
    alter column instrument_keywords type citext
        using replace(replace(instrument_keywords::varchar,$$'$$,''),' ','|'),
    alter column proposal_keywords type citext
        using replace(replace(proposal_keywords::varchar,$$'$$,''),' ','|'),
    alter column target_keywords type citext
        using replace(replace(target_keywords::varchar,$$'$$,''),' ','|'),
    alter column telescope_keywords type citext
        using replace(replace(telescope_keywords::varchar,$$'$$,''),' ','|');

drop index if exists Plane_i_prov_kw;

alter table caom2.Plane
    alter column provenance_keywords type citext
        using replace(replace(provenance_keywords::varchar,$$'$$,''),' ','|');

alter table caom2.Plane
    rename column position_dimension1 to position_dimension_naxis1;
alter table caom2.Plane
    rename column position_dimension2 to position_dimension_naxis2;

-- bug fix: correct support for IVOA/DALI datatypes
alter table caom2.Plane
    add column position_bounds_points double precision[],
    add column position_bounds_samples double precision[],
    add column energy_bounds_samples polygon,
    add column time_bounds_samples polygon;

update caom2.Plane 
    set (energy_bounds_samples,time_bounds_samples) = (energy_bounds, time_bounds);

-- recreate indices on keywords? NO - need a large database before I can study
-- the possible query plans

