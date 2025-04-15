
drop view if exists <schema>.ObsCore_radio;

create view <schema>.ObsCore_radio
(
    obs_publisher_did,
    s_largest_angular_scale_min,
    s_largest_angular_scale_max,
    uv_distance_min,
    uv_distance_max,
    uv_distribution_ecc,
    uv_distribution_fill
)
AS SELECT
    p.publisherID,
    p.position_maxRecoverableScale[1],
    p.position_maxRecoverableScale[2],
    p.uv_distance[1],
    p.uv_distance[2],
    p.uv_distributionEccentricity,
    p.uv_distributionFill
FROM <schema>.Plane as p
WHERE p.uv_distance IS NOT NULL
;

