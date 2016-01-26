
create table caom2_HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    skipID          binary(16) not null,
    lastModified    datetime not null,
    id              binary(16) primary key nonclustered nonclustered
)
;

create unique index i_HarvestSkip
    on caom2_HarvestState ( source,cname,skipID )
;
