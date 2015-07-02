
create table caom2_HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    skipID          bigint not null,
    lastModified    datetime not null,
    id              bigint not null primary key nonclustered
)
;

create unique index i_HarvestSkip
    on caom2_HarvestState ( source,cname,skipID )
;
