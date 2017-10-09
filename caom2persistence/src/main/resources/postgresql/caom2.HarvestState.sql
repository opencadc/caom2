
create table <schema>.HarvestState
(
    source          varchar(256) not null,
    cname           varchar(256)  not null,
    curLastModified timestamp,
    curID           uuid,

    lastModified    timestamp not null,
    stateID         uuid primary key
)
;

create unique index HarvestState_i1
    on <schema>.HarvestState ( source,cname )
;
