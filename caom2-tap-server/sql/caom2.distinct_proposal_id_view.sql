
-- view of caom2.distinct_proposal_id

-- drop old view
drop view if exists caom2.distinct_proposal_id_view;

start transaction;

-- create view
create view caom2.distinct_proposal_id_view as
select count(*),o.proposal_id
from caom2.Observation o
where o.proposal_id is not null
group by o.proposal_id
;

grant select on view caom2.distinct_proposal_id_view to CVOPUB;

commit;
