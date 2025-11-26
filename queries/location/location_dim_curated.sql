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

/*
Docstring:
This MERGE statement loads data from raw.Location_stream into curated.restaurant_location,
adding state codes, union territory flags, capital city flags, and city tiers for Bangladesh.
*/

-- Merge data from raw.Location_stream to curated.restaurant_location
MERGE INTO curated.restaurant_location AS target
USING (
    SELECT 
        CAST(LocationID AS NUMBER) AS Location_ID,
        CAST(City AS STRING) AS City,
        CAST(State AS STRING) AS State,

        -- State/Division Code Mapping (Bangladesh)
        CASE 
            WHEN State = 'Dhaka' THEN 'DH'
            WHEN State = 'Chattogram' THEN 'CTG'
            WHEN State = 'Sylhet' THEN 'SYL'
            WHEN State = 'Khulna' THEN 'KHU'
            WHEN State = 'Rajshahi' THEN 'RAJ'
            ELSE NULL
        END AS state_code,

        -- Bangladesh has no union territories, so always 'N'
        'N' AS is_union_territory,

        -- Capital city flag
        CASE 
            WHEN (State = 'Dhaka' AND City = 'Dhaka') THEN TRUE
            ELSE FALSE
        END AS capital_city_flag,

        -- City tier classification (example logic for Bangladesh)
        CASE 
            WHEN City IN ('Dhaka', 'Chattogram') THEN 'Tier-1'
            WHEN City IN ('Khulna', 'Rajshahi', 'Sylhet') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,

        CAST(ZipCode AS STRING) AS Zip_Code,
        CAST(ActiveFlag AS STRING) AS Active_Flag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM raw.Location_stream
) AS source
ON target.Location_ID = source.Location_ID

-- Update if any field has changed
WHEN MATCHED AND (
    target.City != source.City OR
    target.State != source.State OR
    target.state_code != source.state_code OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.Zip_Code != source.Zip_Code OR
    target.Active_Flag != source.Active_Flag OR
    target.modified_ts != source.modified_ts
) THEN 
    UPDATE SET 
        target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.Zip_Code = source.Zip_Code,
        target.Active_Flag = source.Active_Flag,
        target.modified_ts = source.modified_ts,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts

-- Insert new records if not matched
WHEN NOT MATCHED THEN
    INSERT (
        Location_ID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
