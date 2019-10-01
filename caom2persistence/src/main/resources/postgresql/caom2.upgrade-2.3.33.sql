-- rename position.bounds columns
alter table caom2.Plane
    rename column position_bounds to position_bounds_spoly;

alter table caom2.Plane
    rename column position_bounds_points to position_bounds;




