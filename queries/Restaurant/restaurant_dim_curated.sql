/*
Defines the curated.restaurant table, its stream, and logic to load and merge data from raw.restaurant_stream.
Cleans and casts source fields into proper datatypes, applies PII tagging, and adds audit metadata.
The stream tracks changes for downstream enrichment and SCD2 processing.
*/
USE ROLE data_engineer;
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.restaurant (
    restaurant_sk NUMBER AUTOINCREMENT PRIMARY KEY,              -- surrogate key
    restaurant_id NUMBER UNIQUE,                                 -- business key from source
    name STRING(100) NOT NULL,                                   -- restaurant name
    cuisine_type STRING,                                         -- cuisine type
    pricing_for_two NUMBER(10, 2),                               -- pricing for two people
    restaurant_phone STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),  -- phone number (PII tagged)
    operating_hours STRING(100),                                 -- operating hours
    location_id_fk NUMBER,                                       -- foreign key to location
    active_flag STRING(10),                                      -- active status
    open_status STRING(10),                                      -- open status
    locality STRING(100),                                        -- locality
    restaurant_address STRING,                                   -- full address
    latitude NUMBER(9, 6),                                       -- latitude precision
    longitude NUMBER(9, 6),                                      -- longitude precision
    created_dt TIMESTAMP_TZ,                                     -- record creation date
    modified_dt TIMESTAMP_TZ,                                    -- last modified date

    -- audit columns
    _stg_file_name STRING,                                       -- source file name
    _stg_file_load_ts TIMESTAMP_TZ,                              -- file load timestamp
    _stg_file_md5 STRING,                                        -- file checksum
    _copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP         -- copy timestamp
)
COMMENT = 'Curated restaurant table with standardized datatypes, audit metadata, and PII tagging.';

-- Stream to track changes for enrichment
CREATE OR REPLACE STREAM curated.restaurant_stream ON TABLE curated.restaurant
COMMENT = 'Tracks inserts and updates in curated.restaurant for downstream enrichment.';

-- Initial load from raw stream
INSERT INTO curated.restaurant (
    restaurant_id,
    name,
    cuisine_type,
    pricing_for_two,
    restaurant_phone,
    operating_hours,
    location_id_fk,
    active_flag,
    open_status,
    locality,
    restaurant_address,
    latitude,
    longitude,
    created_dt,
    modified_dt,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
SELECT 
    TRY_CAST(restaurantid AS NUMBER) AS restaurant_id,
    name::STRING AS name,
    cuisinetype::STRING AS cuisine_type,
    TRY_CAST(pricing_for_2 AS NUMBER(10,2)) AS pricing_for_two,
    restaurant_phone::STRING AS restaurant_phone,
    operatinghours::STRING AS operating_hours,
    TRY_CAST(locationid AS NUMBER) AS location_id_fk,
    activeflag::STRING AS active_flag,
    openstatus::STRING AS open_status,
    locality::STRING AS locality,
    restaurant_address::STRING AS restaurant_address,
    TRY_CAST(latitude AS NUMBER(9,6)) AS latitude,
    TRY_CAST(longitude AS NUMBER(9,6)) AS longitude,
    TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
    TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts
FROM raw.restaurant_stream;

-- Merge logic for updates/inserts
MERGE INTO curated.restaurant AS target
USING (
    SELECT 
        TRY_CAST(restaurantid AS NUMBER) AS restaurant_id,
        name::STRING AS name,
        cuisinetype::STRING AS cuisine_type,
        TRY_CAST(pricing_for_2 AS NUMBER(10,2)) AS pricing_for_two,
        restaurant_phone::STRING AS restaurant_phone,
        operatinghours::STRING AS operating_hours,
        TRY_CAST(locationid AS NUMBER) AS location_id_fk,
        activeflag::STRING AS active_flag,
        openstatus::STRING AS open_status,
        locality::STRING AS locality,
        restaurant_address::STRING AS restaurant_address,
        TRY_CAST(latitude AS NUMBER(9,6)) AS latitude,
        TRY_CAST(longitude AS NUMBER(9,6)) AS longitude,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM raw.restaurant_stream
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN 
    INSERT (
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        latitude,
        longitude,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.latitude,
        source.longitude,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

