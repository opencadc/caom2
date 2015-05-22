
create table caom2_HarvestState
(
    source          varchar(256) not null,
    code            integer not null,
    cname           varchar(256)  not null,
    curLastModified datetime null,
    curID           binary(16) null,

    lastModified    datetime not null,
    stateID         bigint not null primary key nonclustered
)
;

create unique index i_HarvestState
    on caom2_HarvestState ( source,cname )
;
