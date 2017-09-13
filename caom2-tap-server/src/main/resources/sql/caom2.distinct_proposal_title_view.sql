
-- view of distinct_proposal_title

-- drop old view
drop view if exists caom2.distinct_proposal_title;

-- create view
create view caom2.distinct_proposal_title as
select count(*) as num_tuples, o.proposal_title
from caom2.Observation o
where o.proposal_title is not null and o.collection != 'TEST'
group by o.proposal_title
;

grant select on caom2.distinct_proposal_title to CVOPUB;

