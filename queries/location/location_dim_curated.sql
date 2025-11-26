use role data_engineer
use schema foodpanda_db.curated 

create or replace table curated.restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'This is the curated restaurant_location table where data will be copied from raw.Location table using copy command. This is as-is data representation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream curated.restaurant_location_stream on table curated.restaurant_location;
comment = 'This is the stream table that will be used to track changes in the curated.restaurant_location table.';
