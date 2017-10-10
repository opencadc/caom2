
create table <schema>.HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256)  not null,
    skipID          uuid not null,
    errorMessage    varchar(1024),

    lastModified    timestamp not null,
    id              uuid primary key
)
;

create unique index HarvestSkip_i1
    on <schema>.HarvestSkip ( source,cname,skipID )
;

