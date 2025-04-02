
create table <schema>.HarvestState
(
    source          varchar(256) not null,
    cname           varchar(256) not null,
    curLastModified timestamp,
    curID           uuid,

    id uuid not null primary key,
    lastModified timestamp not null
)
;

create unique index HarvestState_i1
    on <schema>.HarvestState ( source,cname )
;
