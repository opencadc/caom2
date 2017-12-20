
-- update caom2.DeletedObservation table
delete from <schema>.DeletedObservation;

alter table <schema>.DeletedObservation 
    add column collection varchar(64) not null,
    add column observationID varchar(256) not null
;

-- update caom2.HarvestSkipURI table
alter table <schema>.HarvestSkipURI
    add column tryAfter timestamp;

update <schema>.HarvestSkipURI 
    set tryAfter = lastModified;

alter table <schema>.HarvestSkipURI
    alter column tryAfter set not null;

create index HarvestSkipURI_i2
    on <schema>.HarvestSkipURI ( source,cname,tryAfter )
;
