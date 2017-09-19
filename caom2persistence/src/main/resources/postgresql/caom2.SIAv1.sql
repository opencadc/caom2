
create or replace view caom2.SIAv1
(
    collection, publisherDID, instrument_name,

    position_center_ra, position_center_dec,
    position_naxes, position_naxis, position_scale,
    position_bounds,

    energy_bounds_center, energy_bounds_cval1, energy_bounds_cval2,
    energy_units, energy_bandpassName,

    time_bounds_center, time_bounds_cval1, time_bounds_cval2,
    time_exposure,

-- temporarily support both, but only accessURL is in the tap_schema
    uri,
-- deprecated: AccessURLConverter changes accessURL -> uri
    accessURL,
    imageFormat,

-- CAOM read access
    metaRelease, dataRelease, metaReadAccessGroups,
    planeID
)
AS SELECT
o.collection, p.planeURI, instrument_name,

degrees(long(p.position_bounds_center)), degrees(lat(position_bounds_center)),
2::int,
ARRAY[position_dimension_naxis1,position_dimension_naxis2],
ARRAY[position_sampleSize,position_sampleSize],
p.position_bounds,

-- careful to avoid underflow when cval1 = 0 and cval2 = min denormalised double
(case when p.energy_bounds_lower  > 1.0e-300 then 0.5*(p.energy_bounds_lower + p.energy_bounds_upper) else NULL end),
p.energy_bounds_lower, p.energy_bounds_lower, CAST('m' as varchar),
p.energy_bandpassName,

0.5*(p.time_bounds_upper + p.time_bounds_lower),
p.time_bounds_lower, p.time_bounds_upper,
p.time_exposure,

a.uri,
a.uri,
a.contentType,

p.metaRelease, p.dataRelease, p.metaReadAccessGroups,
p.planeID

FROM caom2.Observation o JOIN caom2.Plane p ON o.obsID=p.obsID JOIN caom2.Artifact a on p.planeID=a.planeID
WHERE 
-- science data only as in ObsCore
  o.intent = 'science' 
  AND p.calibrationLevel IS NOT NULL
-- avoid nulls in arrays created in select
  AND p.position_dimension_naxis1 IS NOT NULL
  AND p.position_dimension_naxis2 IS NOT NULL
  AND p.position_sampleSize IS NOT NULL
-- SIAv1 specific filtering
  AND p.dataProductType = 'image'
  AND (a.productType = 'science'
    OR a.productType IS NULL) -- OR to allow HST science data through without joining to caom2.Part
  AND (o.collection != 'OMM' OR a.uri NOT LIKE '%prev') -- hack for OMM data engineering issue
;
