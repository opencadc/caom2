
-- tables with aggregate information for text fields: list of distinct values

-- drop old tmp table (unlikely: only if this script gets killed)
drop table if exists caom2.distinct_proposal_title_tmp;

start transaction;

-- create tmp table
create table caom2.distinct_proposal_title_tmp
(
-- number of occurances of each combination
    num_tuples             bigint,

-- from caom.Observation
    proposal_title         citext
)
;

-- populate
insert into caom2.distinct_proposal_title_tmp
(num_tuples,proposal_title)
select count(*),o.proposal_title
from caom2.Observation o
where o.proposal_title is not null
group by o.proposal_title
;

create index distinct_proposal_title_i1_tmp
    on caom2.distinct_proposal_title_tmp ( proposal_title )
;

-- drop the old table, swap in the new table, grant permissions

grant select on table caom2.distinct_proposal_title_tmp to CVOPUB;

drop table if exists caom2.distinct_proposal_title;
alter table caom2.distinct_proposal_title_tmp rename to distinct_proposal_title;
alter index caom2.distinct_proposal_title_i1_tmp rename to distinct_proposal_title_i1;

commit;

