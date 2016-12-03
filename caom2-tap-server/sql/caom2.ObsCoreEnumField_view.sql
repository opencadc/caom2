
-- view of caom2.ObsCoreEnumField

-- drop old view 
drop view if exists caom2.ObsCoreEnumField;

-- create view
create view caom2.ObsCoreEnumField as
select count(*) as num_tuples, max(t_min) as max_t_min, 
    obs_collection, facility_name, instrument_name,
    dataproduct_type, calib_level
from caom2.ObsCore
where obs_collection != 'TEST'
group by obs_collection, facility_name, instrument_name, dataproduct_type, calib_level
;

grant select on caom2.ObsCoreEnumField to CVOPUB;

