
-- view of distinct_proposal_pi

-- drop old view
drop view if exists caom2.distinct_proposal_pi_view;

start transaction;

-- create view
create table caom2.distinct_proposal_pi_view as
select count(*),o.proposal_pi
from caom2.Observation o
where o.proposal_pi is not null
group by o.proposal_pi
;

grant select on view caom2.distinct_proposal_pi_view to CVOPUB;

commit;
