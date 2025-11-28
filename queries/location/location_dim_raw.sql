/*
Docstring:
This script creates the raw Location dimension table in Snowflake, sets up a stream for change tracking,
and loads data from the internal stage using COPY INTO with audit columns for traceability.
*/

-- Use the data engineering role and switch to raw schema
use role data_engineer;
use schema foodpanda_db.raw;

-- Create raw Location table with audit columns
create or replace table raw.Location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'Raw Location table populated from staged CSV files. All business columns are text; audit columns provide traceability.';

select * from raw.Location;

-- Create stream on the raw table to capture incremental changes
create or replace stream raw.Location_stream 
on table raw.Location
append_only = true
comment = 'Stream to track inserts/updates in raw.Location for downstream processing.';

-- Copy data from internal stage into raw.Location
copy into raw.Location (
    locationid, city, state, zipcode, activeflag, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text as locationid,
        t.$2::text as city,
        t.$3::text as state,
        t.$4::text as zipcode,
        t.$5::text as activeflag,
        t.$6::text as createddate,
        t.$7::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @raw.csv_stage/initial/Location.csv (file_format => 'raw.csv_format') t
    on error = abort_statement
)
comment = 'COPY INTO command to load staged Location.csv into raw.Location with audit metadata.';

-- Check copy history for recent loads
select *
from table(information_schema.copy_history(
    table_name=>'LOCATION', 
    start_time=> dateadd(hours, -1, current_timestamp())
));

-- Preview loaded data
select * from raw.Location;
select * from raw.Location_stream;
