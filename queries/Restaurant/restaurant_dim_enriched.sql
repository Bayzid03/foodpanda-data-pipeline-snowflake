/*
Defines the enriched.restaurant_dim table with SCD2 logic.
Tracks historical changes from curated.restaurant_stream using surrogate hash keys, effective dates, and current flags.
*/

USE ROLE data_engineer;
USE SCHEMA foodpanda_db.enriched;

-- Restaurant Dimension with SCD2
CREATE OR REPLACE TABLE enriched.restaurant_dim (
    restaurant_hk NUMBER PRIMARY KEY,       -- Surrogate hash key
    restaurant_id NUMBER,                   -- Business key
    name STRING(100),
    cuisine_type STRING,
    pricing_for_two NUMBER(10,2),
    restaurant_phone STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),
    operating_hours STRING(100),
    location_id_fk NUMBER,
    active_flag STRING(10),
    open_status STRING(10),
    locality STRING(100),
    restaurant_address STRING,
    latitude NUMBER(9,6),
    longitude NUMBER(9,6),
    eff_start_date TIMESTAMP_TZ,
    eff_end_date TIMESTAMP_TZ,
    is_current BOOLEAN
);

MERGE INTO enriched.restaurant_dim AS target
USING curated.restaurant_stream AS source
ON target.restaurant_id = source.restaurant_id

-- Expire old record when update occurs
WHEN MATCHED 
    AND source.METADATA$ACTION = 'UPDATE' 
    AND source.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        target.eff_end_date = CURRENT_TIMESTAMP(),
        target.is_current = FALSE

-- Insert new record when attributes change or new restaurant arrives
WHEN NOT MATCHED THEN
    INSERT (
        restaurant_hk,
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
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            COALESCE(source.restaurant_id,''), COALESCE(source.name,''), COALESCE(source.cuisine_type,''), 
            COALESCE(source.pricing_for_two,''), COALESCE(source.restaurant_phone,''), COALESCE(source.operating_hours,''), 
            COALESCE(source.location_id_fk,''), COALESCE(source.active_flag,''), COALESCE(source.open_status,''), 
            COALESCE(source.locality,''), COALESCE(source.restaurant_address,''), COALESCE(source.latitude,''), 
            COALESCE(source.longitude,'')
        ))),
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
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );
