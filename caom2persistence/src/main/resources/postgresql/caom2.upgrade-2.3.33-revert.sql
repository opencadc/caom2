
-- for reference and manual revert of upgrade: 2.3.33 to 2.3.31

start transaction;

alter table caom2.Plane
    rename column position_bounds to position_bounds_points;

alter table caom2.Plane
    rename column position_bounds_spoly to position_bounds;

update caom2.ModelVersion set version='2.3.31'
where model = 'CAOM';

commit;
