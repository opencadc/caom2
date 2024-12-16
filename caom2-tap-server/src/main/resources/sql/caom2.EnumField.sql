
-- table with aggregate information for enumerated fields

-- drop old tmp table (unlikely: only if this script gets killed)
drop table if exists caom2.EnumField_tmp;

start transaction;

-- create tmp table
create table caom2.EnumField_tmp
(
-- number of occurances of each combination
    num_tuples             bigint,
-- max timestamp for data acquisition described by this combination
    max_time_bounds_cval1  double precision,

-- from caom.Observation
    collection             varchar(64),
    telescope_name         varchar(64),
    instrument_name        varchar(64),
    type                   varchar(32),
    intent                 varchar(32),

-- from caom.Plane
    dataProductType        varchar(16),
    calibrationLevel       integer,
    energy_energyBands     varchar(64),
    energy_bandpassName    varchar(64)
)
;

-- populate
insert into caom2.EnumField_tmp
(num_tuples,max_time_bounds_cval1,collection,telescope_name,instrument_name,type,intent,dataProductType,calibrationLevel,energy_energyBands,energy_bandpassName)
select count(*),max(p.time_bounds_lower),o.collection,o.telescope_name,o.instrument_name,o.type,o.intent,
       p.dataProductType,p.calibrationLevel,p.energy_energyBands,p.energy_bandpassName
from caom2.Observation o join caom2.Plane p on o.obsID=p.obsID
group by o.collection,o.telescope_name,o.instrument_name,o.type,o.intent,
         p.dataProductType,p.calibrationLevel,p.energy_energyBands,p.energy_bandpassName
;

-- drop the old table, swap in the new table, grant permissions

grant select on table caom2.EnumField_tmp to CVOPUB;

drop table if exists caom2.EnumField;
alter table caom2.EnumField_tmp rename to EnumField;

commit;
