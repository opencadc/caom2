
-- view of caom2.ObsCoreEnumField

-- drop old view 
drop view if exists caom2.ObsCoreEnumField_view;

start transaction;

-- create view
create view caom2.ObsCoreEnumField_view as
select count(*),max(t_min),obs_collection,facility_name,instrument_name,dataproduct_type,calib_level
from caom2.ObsCore
group by obs_collection,facility_name,instrument_name,dataproduct_type,calib_level
;

grant select on view caom2.ObsCoreEnumField_view to CVOPUB;

commit;
