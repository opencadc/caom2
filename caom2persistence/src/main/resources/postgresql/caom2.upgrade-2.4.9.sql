-- add bucket column to support parallel record processing by caom2-artifact-download
alter table <schema>.HarvestSkipURI
  add column bucket char(3);

-- populate with chars from uuid
update <schema>.HarvestSkipURI set bucket = substring(id::varchar, 1, 3);

-- add not-null constraint
alter table <schema>.HarvestSkipURI
  alter column bucket set NOT NULL;

-- create index
create index HarvestSkipURI_bucket
    on <schema>.HarvestSkipURI(bucket);
