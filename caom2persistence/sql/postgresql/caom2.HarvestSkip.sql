
create table caom2.HarvestSkip
(
    source          varchar(256) not null,
    cname           varchar(256)  not null,
    skipID          bigint not null,
    lastModified    timestamp not null,
    id              bigint primary key using index tablespace caom_index
)
tablespace caom_data
;

create unique index HarvestSkip_i1
    on caom2.HarvestSkip ( source,cname,skipID )
tablespace caom_index
;

