
create table caom2.HarvestSkipURI
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    skipID          varchar(512) not null,
    errorMessage    varchar(1024),

    lastModified    timestamp not null,
    id              uuid primary key
)
;

create unique index HarvestSkipURI_i1
    on caom2.HarvestSkipURI ( source,cname,skipID )
;

