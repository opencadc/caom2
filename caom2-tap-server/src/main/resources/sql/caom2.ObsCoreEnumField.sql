
-- table with aggregate information for enumerated fields

-- drop old tmp table (unlikely: only if this script gets killed)
drop table if exists caom2.ObsCoreEnumField_tmp;

start transaction;

-- create tmp table
create table caom2.ObsCoreEnumField_tmp
(
-- number of occurances of each combination
    num_tuples             bigint,
    max_t_min              double precision,

-- from caom.Observation
    obs_collection         varchar(64),
    facility_name          varchar(64),
    instrument_name        varchar(64),

    dataproduct_type       varchar(16),
    calib_level            integer
)
;

-- populate
insert into caom2.ObsCoreEnumField_tmp
(num_tuples,max_t_min,obs_collection,facility_name,instrument_name,dataproduct_type,calib_level)
select count(*),max(t_min),obs_collection,facility_name,instrument_name,dataproduct_type,calib_level
from caom2.ObsCore
group by obs_collection,facility_name,instrument_name,dataproduct_type,calib_level
;

-- drop the old table, swap in the new table, grant permissions

grant select on table caom2.ObsCoreEnumField_tmp to CVOPUB;

drop table if exists caom2.ObsCoreEnumField;
alter table caom2.ObsCoreEnumField_tmp rename to ObsCoreEnumField;

commit;
