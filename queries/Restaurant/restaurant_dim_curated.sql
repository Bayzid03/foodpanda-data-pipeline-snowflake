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
    _copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP          -- copy timestamp
)
COMMENT = 'Curated restaurant table with standardized datatypes, audit metadata, and PII tagging.';

-- Stream to track changes for enrichment
CREATE OR REPLACE STREAM curated.restaurant_stream ON TABLE curated.restaurant
COMMENT = 'Tracks inserts and updates in curated.restaurant for downstream enrichment.';
