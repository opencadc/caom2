
-- tables with aggregate information for text fields: list of distinct values

-- drop old tmp table (unlikely: only if this script gets killed)
drop table if exists caom2.distinct_proposal_id_tmp;

start transaction;

-- create tmp table
create table caom2.distinct_proposal_id_tmp
(
-- number of occurances of each combination
    num_tuples             bigint,

-- from caom.Observation
    proposal_id            citext
)
;

-- populate
insert into caom2.distinct_proposal_id_tmp
(num_tuples,proposal_id)
select count(*),o.proposal_id
from caom2.Observation o
where o.proposal_id is not null
group by o.proposal_id
;

create index distinct_proposal_id_i1_tmp
    on caom2.distinct_proposal_id_tmp ( proposal_id )
;

-- drop the old table, swap in the new table, grant permissions

grant select on table caom2.distinct_proposal_id_tmp to CVOPUB;

drop table if exists caom2.distinct_proposal_id;
alter table caom2.distinct_proposal_id_tmp rename to distinct_proposal_id;
alter index caom2.distinct_proposal_id_i1_tmp rename to distinct_proposal_id_i1;

commit;

