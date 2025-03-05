
create table <schema>.HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    uri             varchar(512) not null,
    uriBucket       char(3) not null,
    tryAfter        timestamp,
    errorMessage    varchar(1024),

    id uuid not null primary key,
    lastModified timestamp not null
)
;

create unique index HarvestSkip_i1
    on <schema>.HarvestSkip ( source,cname,uri )
;

create index HarvestSkip_bucket
    on <schema>.HarvestSkip(uriBucket);
