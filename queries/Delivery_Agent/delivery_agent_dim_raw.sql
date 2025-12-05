/*
Defines raw.delivery_agent table for ingesting source data as-is with audit metadata.
Creates append-only stream raw.delivery_agent_stream to capture delta changes.
*/
use role data_engineer;
use database foodpanda_db;
use schema foodpanda_db.raw;

create or replace table raw.delivery_agent (
    deliveryagentid text comment 'Primary Key (Source System)',         -- primary key as text
    name text,           
    phone text,            
    vehicletype text,             
    locationid text,              
    status text,                  
    gender text,                  
    rating text,                  
    createddate text,             
    modifieddate text,            

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'Raw delivery_agent table storing source data as-is. All columns are text except audit fields.';

create or replace stream raw.delivery_agent_stream on table raw.delivery_agent
APPEND_ONLY = true
comment = 'This is the append-only stream object on delivery agent table that only gets delta data';


copy into raw.delivery_agent (deliveryagentid, name, phone, vehicletype, locationid, 
                         status, gender, rating, createddate, modifieddate,
                         _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as deliveryagentid,
        t.$2::text as name,
        t.$3::text as phone,
        t.$4::text as vehicletype,
        t.$5::text as locationid,
        t.$6::text as status,
        t.$7::text as gender,
        t.$8::text as rating,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @raw.csv_stg/initial/Delivery_Agent t
)
file_format = (format_name = 'raw.csv_file_format')
on_error = abort_statement;

select count(*) from raw.delivery_agent;
select * from raw.delivery_agent_stream;
