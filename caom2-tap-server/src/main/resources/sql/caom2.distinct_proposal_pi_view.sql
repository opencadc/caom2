
-- view of distinct_proposal_pi

-- drop old view
drop view if exists caom2.distinct_proposal_pi;

-- create view
create view caom2.distinct_proposal_pi as
select count(*) as num_tuples, o.proposal_pi
from caom2.Observation o
where o.proposal_pi is not null and o.collection != 'TEST'
group by o.proposal_pi
;

grant select on caom2.distinct_proposal_pi to CVOPUB;

