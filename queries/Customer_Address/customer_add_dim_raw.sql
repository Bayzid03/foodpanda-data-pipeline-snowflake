/*
This script defines the raw.customeraddress table, its stream, and loads staged CSV data
into the table with audit metadata for traceability.
*/
use role data_engineer;
use schema foodpanda_db.raw;
use warehouse foodpanda_wh;

-- Create raw customer address table
create or replace table raw.customeraddress (
    addressid text,                                   -- primary key as text
    customerid text comment 'Customer FK (Source Data)', -- foreign key reference as text (no constraint in Snowflake)
    flatno text,                                      -- flat number
    houseno text,                                     -- house number
    floor text,                                       -- floor
    building text,                                    -- building name
    landmark text,                                    -- landmark
    locality text,                                    -- locality
    city text,                                        -- city
    state text,                                       -- state/division
    pincode text,                                     -- postal code
    coordinates text,                                 -- coordinates
    primaryflag text,                                 -- primary flag
    addresstype text,                                 -- address type
    createddate text,                                 -- created date
    modifieddate text,                                -- modified date

    -- audit columns
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'Raw customer address table populated from staged CSV files with audit metadata.';

-- Create stream on raw table
create or replace stream raw.customeraddress_stream 
on table raw.customeraddress
append = true
comment = 'Stream to track inserts/updates in raw.customeraddress for downstream processing.';

-- Copy data from staged CSV into raw.customeraddress
copy into raw.customeraddress (
    addressid, customerid, flatno, houseno, floor, building, 
    landmark, locality, city, state, pincode, coordinates, primaryflag, addresstype, 
    createddate, modifieddate, 
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text  as addressid,
        t.$2::text  as customerid,
        t.$3::text  as flatno,
        t.$4::text  as houseno,
        t.$5::text  as floor,
        t.$6::text  as building,
        t.$7::text  as landmark,
        t.$8::text  as locality,
        t.$9::text  as city,
        t.$10::text as state,
        t.$11::text as pincode,
        t.$12::text as coordinates,
        t.$13::text as primaryflag,
        t.$14::text as addresstype,
        t.$15::text as createddate,
        t.$16::text as modifieddate,
        metadata$filename          as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key   as _stg_file_md5,
        current_timestamp           as _copy_data_ts
    from @raw.csv_stg/initial/Customer_Address t
)
file_format = (format_name = 'raw.csv_file_format')
on_error = abort_statement;
