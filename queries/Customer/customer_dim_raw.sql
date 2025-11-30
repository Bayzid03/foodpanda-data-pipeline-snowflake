/*
Defines the raw.customer table, its stream, and COPY INTO command to load staged CSV data.
Stores source data as-is with audit metadata for traceability, applies PII tags for governance,
and enables downstream curated/enriched layers via the stream. Includes validation queries.
*/

use role data_engineer;
use schema foodpanda_db.raw;
use warehouse foodpanda_wh;

-- Create raw customer table
create or replace table raw.customer (
    customerid text,                                        -- Primary key (source system)
    name text,                                              -- Customer name
    mobile text WITH TAG (common.pii_policy_tag = 'PII'),   -- Mobile number (PII tagged)
    email text WITH TAG (common.pii_policy_tag = 'EMAIL'),  -- Email address (PII tagged)
    loginbyusing text,                                      -- Login method
    gender text WITH TAG (common.pii_policy_tag = 'PII'),   -- Gender (PII tagged)
    dob text WITH TAG (common.pii_policy_tag = 'PII'),      -- Date of birth (PII tagged)
    anniversary text,                                       -- Anniversary date
    preferences text,                                       -- Customer preferences
    createddate text,                                       -- Record creation date
    modifieddate text,                                      -- Record modification date

    -- Audit columns
    _stg_file_name text,                                    -- Source file name
    _stg_file_load_ts timestamp,                            -- File load timestamp
    _stg_file_md5 text,                                     -- File checksum
    _copy_data_ts timestamp default current_timestamp       -- Copy/load timestamp
)
comment = 'Raw customer table populated from staged CSV files with audit metadata. All columns are text except audit fields.';

-- Create stream on raw.customer for change tracking
create or replace stream raw.customer_stream on table raw.customer
append_only = true
comment = 'Stream to track inserts/updates in raw.customer for downstream processing.';

-- Load data from staged CSV into raw.customer
copy into raw.customer (
    customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, preferences,
    createddate, modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
from (
    select 
        t.$1::text  as customerid,
        t.$2::text  as name,
        t.$3::text  as mobile,
        t.$4::text  as email,
        t.$5::text  as loginbyusing,
        t.$6::text  as gender,
        t.$7::text  as dob,
        t.$8::text  as anniversary,
        t.$9::text  as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename           as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key   as _stg_file_md5,
        current_timestamp           as _copy_data_ts
    from @raw.csv_stg/initial/Customer t
)
file_format = (format_name = 'raw.csv_file_format')
on_error = abort_statement;

-- Validation queries
select * from raw.customer limit 10;        -- Preview sample records
select count(*) from raw.customer;          -- Count total records
select count(*) from raw.customer_stream;   -- Count stream records
