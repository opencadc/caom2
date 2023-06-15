
-- a sample table with currently support PostgreSQL datatype 
-- the table has one row with valid values for each type
-- the table has one row with NULL values for each type

drop table tap_test.AllDataTypes;

create table tap_test.AllDataTypes
(
    t_char                char(1),
    t_clob                text,
    t_char_n              char(8),
    t_varchar_n           varchar(64),
    t_varchar             varchar,
    
    t_byte                bytea,
    t_blob                bytea,
    t_binary_n            bytea,
    t_varbinary_n         bytea,
    t_varbinary           bytea,
    
    t_short               smallint,
    t_int                 integer,
    t_long                bigint,
    t_float               real,
    t_double              double precision,
    t_timestamp           timestamp,
    
    t_stc_point           spoint,
    t_stc_polygon         spoly
)
tablespace tap_data
;

