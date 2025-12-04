/*
Defines the raw.menu table and append-only stream for ingesting menu data from staged CSVs.
Stores all source fields as text with audit metadata for traceability.
*/
use role data_engineer;
use database foodpanda_db.raw;
use warehouse foodpanda_wh;

create or replace table raw.menu (
    menuid text comment 'Primary Key (Source System)',                   
    restaurantid text comment 'Restaurant FK(Source System)',             
    itemname text,                 
    description text,              
    price text,                    
    category text,                 
    availability text,             
    itemtype text,                 
    createddate text,              
    modifieddate text,             

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
COMMENT = 'Raw menu table storing source data as-is from stage. All columns are text except audit fields for traceability.';

-- Stream object to capture the changes. 
create or replace stream raw.menu_stm 
on table raw.menu
append_only = true
comment = 'This is the append-only stream object on menu entity that only gets delta data';

list @raw.csv_stg/initial/Menu;

copy into raw.menu (menuid, restaurantid, itemname, description, price, category, 
                availability, itemtype, createddate, modifieddate,
                _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as menuid,
        t.$2::text as restaurantid,
        t.$3::text as itemname,
        t.$4::text as description,
        t.$5::text as price,
        t.$6::text as category,
        t.$7::text as availability,
        t.$8::text as itemtype,
        t.$9::text as createddate,
        t.$10::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @raw.csv_stg/initial/Menu t
)
file_format = (format_name = 'raw.csv_file_format')
on_error = abort_statement;

select * from raw.menu limit 10;
