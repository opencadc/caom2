
-- view of table caom2.EnumField

-- drop old view
drop view if exists caom2.EnumField;

-- create view
create view caom2.EnumField as 
select count(*) as num_tuples, max(p.time_bounds_lower) as max_time_bounds_cval1,  
    o.collection, o.telescope_name, o.instrument_name, o.type,o.intent,
    p.dataProductType, p.calibrationLevel, p.energy_energyBands, p.energy_bandpassName
from caom2.Observation o join caom2.Plane p on o.obsID=p.obsID
where o.collection != 'TEST'
group by o.collection, o.telescope_name, o.instrument_name, o.type, o.intent,
    p.dataProductType, p.calibrationLevel, p.energy_energyBands, p.energy_bandpassName
;

grant select on caom2.EnumField to public;

