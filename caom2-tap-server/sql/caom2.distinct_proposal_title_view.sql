
-- view of distinct_proposal_title

-- drop old view
drop view if exists caom2.distinct_proposal_title_view;

start transaction;

-- create view
create table caom2.distinct_proposal_title_view as
select count(*),o.proposal_title
from caom2.Observation o
where o.proposal_title is not null
group by o.proposal_title
;

grant select on view caom2.distinct_proposal_title_view to CVOPUB;

commit;
