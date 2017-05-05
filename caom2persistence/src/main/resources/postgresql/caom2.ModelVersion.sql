
create table caom2.ModelVersion
(
    model varchar(16) not null primary key,
    version varchar(16) not null,
    lastModified timestamp not null
)
;

