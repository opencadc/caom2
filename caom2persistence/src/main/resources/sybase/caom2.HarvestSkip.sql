
create table caom2_HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    skipID          binary(16) not null,
    errorMessage    varchar(1024) null,

    lastModified    datetime not null,
    id              binary(16) not null primary key nonclustered
)
;

create unique index i_HarvestSkip
    on caom2_HarvestSkip ( source,cname,skipID )
;
