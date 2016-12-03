
-- view of table caom2.EnumField

-- drop old view
drop view if exists caom2.EnumField_view;

start transaction;

-- create view
create view caom2.EnumField_view as 
select count(*),max(p.time_bounds_lower),o.collection,o.telescope_name,o.instrument_name,o.type,o.intent,
       p.dataProductType,p.calibrationLevel,p.energy_emband,p.energy_bandpassName
from caom2.Observation o join caom2.Plane p on o.obsID=p.obsID
group by o.collection,o.telescope_name,o.instrument_name,o.type,o.intent,
         p.dataProductType,p.calibrationLevel,p.energy_emband,p.energy_bandpassName
;

grant select on view caom2.EnumField_view to CVOPUB;

commit;
