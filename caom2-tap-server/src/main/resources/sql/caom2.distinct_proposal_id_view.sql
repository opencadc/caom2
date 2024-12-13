
-- view of caom2.distinct_proposal_id

-- drop old view
drop view if exists caom2.distinct_proposal_id;

-- create view
create view caom2.distinct_proposal_id as
select count(*) as num_tuples, o.proposal_id
from caom2.Observation o
where o.proposal_id is not null and o.collection != 'TEST'
group by o.proposal_id
;

grant select on caom2.distinct_proposal_id to CVOPUB;

