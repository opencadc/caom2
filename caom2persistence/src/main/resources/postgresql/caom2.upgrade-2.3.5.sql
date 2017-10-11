
drop view if exists <schema>.ObsPart;
drop view if exists <schema>.ObsFile;
drop view if exists <schema>.ObsCore;
drop view if exists <schema>.SIAv1;

-- bug fix: correct handling of keywords
drop index <schema>.Observation_i_instr_kw;
drop index <schema>.Observation_i_prop_kw;
drop index <schema>.Observation_i_targ_kw;
drop index <schema>.Observation_i_tel_kw;
drop index <schema>.Plane_i_prov_kw;

-- keyword handling change: tsvector to citext with | separator
alter table <schema>.Observation
    alter column instrument_keywords type citext
        using replace(replace(instrument_keywords::varchar,$$'$$,''),' ','|'),
    alter column proposal_keywords type citext
        using replace(replace(proposal_keywords::varchar,$$'$$,''),' ','|'),
    alter column target_keywords type citext
        using replace(replace(target_keywords::varchar,$$'$$,''),' ','|'),
    alter column telescope_keywords type citext
        using replace(replace(telescope_keywords::varchar,$$'$$,''),' ','|');

--\echo altering keywords column (plane)...
alter table <schema>.Plane
    alter column provenance_keywords type citext
        using replace(replace(provenance_keywords::varchar,$$'$$,''),' ','|');

-- bug fix: rename columns to match model
alter table <schema>.Plane
    rename column position_dimension1 to position_dimension_naxis1;
alter table <schema>.Plane
    rename column position_dimension2 to position_dimension_naxis2;

-- bug fix: correct support for IVOA/DALI datatypes and richer caom2 polygons
alter table <schema>.Plane
    add column position_bounds_points double precision[],
    add column position_bounds_samples double precision[],
    add column energy_bounds_samples polygon,
    add column time_bounds_samples polygon;

-- preserve bounds values we previously stored in the wrong column
update <schema>.Plane 
    set (energy_bounds_samples,time_bounds_samples) = (energy_bounds, time_bounds);
