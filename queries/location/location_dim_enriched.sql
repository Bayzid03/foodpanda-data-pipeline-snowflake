/*
This script creates the enriched restaurant_location_dim table with Slowly Changing Dimension Type 2 (SCD2) logic.
It merges data from curated.restaurant_location_stream into the dimension table, maintaining historical records
with effective start/end dates, current record flags, and a surrogate hash key for uniqueness.
*/

use role data_engineer;
use schema foodpanda_db.enriched;

-- Create enriched dimension table with SCD2 support
create or replace table enriched.restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,             -- surrogate hash key for uniqueness
    location_id number(38,0) not null,                     -- business key from source
    city varchar(100) not null,                            -- city name
    state varchar(100) not null,                           -- state/division name
    state_code varchar(2) not null,                        -- state/division code
    is_union_territory boolean not null default false,     -- union territory flag (always false for Bangladesh)
    capital_city_flag boolean not null default false,      -- flag for capital city
    city_tier varchar(6),                                  -- city tier classification
    zip_code varchar(10) not null,                         -- postal code
    active_flag varchar(10) not null,                      -- active/inactive indicator
    eff_start_dt timestamp_tz(9) not null,                 -- effective start date for SCD2
    eff_end_dt timestamp_tz(9),                            -- effective end date for SCD2
    current_flag boolean not null default true             -- flag for current active record
)
comment = 'Enriched dimension table for restaurant location with SCD2 enabled and surrogate hash key.';

-- Merge curated stream data into enriched dimension table
MERGE INTO enriched.restaurant_location_dim AS target
USING curated.restaurant_location_stream AS source
ON target.LOCATION_ID = source.LOCATION_ID
AND target.ACTIVE_FLAG = source.ACTIVE_FLAG

-- Handle deletes: mark record as expired
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    UPDATE SET 
        target.EFF_END_DT = CURRENT_TIMESTAMP(),
        target.CURRENT_FLAG = false

-- Handle inserts with updates: add new record with current effective start date
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )

-- Handle inserts without updates: add new record with current effective start date
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN
    INSERT (
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );
