
-- tables with aggregate information for text fields: list of distinct values

-- drop old tmp table (unlikely: only if this script gets killed)
drop table if exists caom2.distinct_proposal_pi_tmp;

start transaction;

-- create tmp table
create table caom2.distinct_proposal_pi_tmp
(
-- number of occurances of each combination
    num_tuples             bigint,

-- from caom.Observation
    proposal_pi            citext
)
;

-- populate
insert into caom2.distinct_proposal_pi_tmp
(num_tuples,proposal_pi)
select count(*),o.proposal_pi
from caom2.Observation o
where o.proposal_pi is not null
group by o.proposal_pi
;

create index distinct_proposal_pi_i1_tmp
    on caom2.distinct_proposal_pi_tmp ( proposal_pi )
;

-- drop the old table, swap in the new table, grant permissions

grant select on table caom2.distinct_proposal_pi_tmp to CVOPUB;

drop table if exists caom2.distinct_proposal_pi;
alter table caom2.distinct_proposal_pi_tmp rename to distinct_proposal_pi;
alter index caom2.distinct_proposal_pi_i1_tmp rename to distinct_proposal_pi_i1;

commit;

