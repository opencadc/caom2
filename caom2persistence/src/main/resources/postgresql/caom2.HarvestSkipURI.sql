
create table <schema>.HarvestSkipURI
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    skipID          varchar(512) not null,
    tryAfter      timestamp not null,
    errorMessage    varchar(1024),

    lastModified    timestamp not null,
    id              uuid primary key
)
;

create unique index HarvestSkipURI_i1
    on <schema>.HarvestSkipURI ( source,cname,skipID )
;

create index HarvestSkipURI_i2
    on <schema>.HarvestSkipURI ( source,cname,tryAfter )
;

