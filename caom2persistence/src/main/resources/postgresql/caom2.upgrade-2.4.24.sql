
-- ADQL DISTANCE(POINT(s_ra, s_dec), ...) < r on ObsCore
-- InitDatabase runs this in a transaction, so the build blocks writes to Plane:
-- on a large table, build it beforehand with
--   create index concurrently Plane_position_center on <schema>.Plane using gist (position_bounds_center);
-- and this upgrade does nothing
create index if not exists Plane_position_center
    on <schema>.Plane using gist (position_bounds_center)
;
