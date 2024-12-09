-- remove unused columns
-- https://github.com/opencadc/caom2db/issues/154

alter table caom2.Plane
    drop column energy_bounds_integrated,
    drop column time_bounds_integrated;

