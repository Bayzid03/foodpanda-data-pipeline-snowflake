/*
This script defines the enriched.customer_address_dim table with Slowly Changing Dimension Type 2 (SCD2).
It merges data from curated.customeraddress_stream, maintaining historical records with effective dates,
current flags, and a surrogate hash key for uniqueness.
*/

use schema foodpanda_db.enriched;

-- Create enriched customer address dimension with SCD2
CREATE OR REPLACE TABLE enriched.customer_address_dim (
    customer_address_hk NUMBER PRIMARY KEY comment 'Surrogate hash key for uniqueness',
    address_id INT comment 'Business key (Source System)',
    customer_id_fk STRING comment 'Customer foreign key (Source System)',
    flat_no STRING,
    house_no STRING,
    floor STRING,
    building STRING,
    landmark STRING,
    locality STRING,
    city STRING,
    state STRING,
    pincode STRING,
    coordinates STRING,
    primary_flag STRING,
    address_type STRING,

    -- SCD2 columns
    eff_start_date TIMESTAMP_TZ,
    eff_end_date TIMESTAMP_TZ,
    is_current BOOLEAN
)
comment = 'Enriched customer address dimension with SCD2 logic and surrogate hash key.';

-- Merge curated stream data into enriched dimension
MERGE INTO enriched.customer_address_dim AS target
USING curated.customeraddress_stream AS source
ON target.address_id = source.address_id
AND target.customer_id_fk = source.customer_id_fk

-- Handle deletes: expire current record
WHEN MATCHED
    AND source.METADATA$ACTION = 'DELETE' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN 
    UPDATE SET
        target.eff_end_date = CURRENT_TIMESTAMP,
        target.is_current = false

-- Handle inserts with updates: add new record
WHEN NOT MATCHED
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN 
    INSERT (
        customer_address_hk,
        address_id,
        customer_id_fk,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag,
        address_type,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_hex(CONCAT(
            COALESCE(source.address_id,''), COALESCE(source.customer_id_fk,''), 
            COALESCE(source.flat_no,''), COALESCE(source.house_no,''), 
            COALESCE(source.floor,''), COALESCE(source.building,''), 
            COALESCE(source.landmark,''), COALESCE(source.locality,''), 
            COALESCE(source.city,''), COALESCE(source.state,''), 
            COALESCE(source.pincode,''), COALESCE(source.coordinates,''), 
            COALESCE(source.primary_flag,''), COALESCE(source.address_type,'')
        ))),
        source.address_id,
        source.customer_id_fk,
        source.flat_no,
        source.house_no,
        source.floor,
        source.building,
        source.landmark,
        source.locality,
        source.city,
        source.state,
        source.pincode,
        source.coordinates,
        source.primary_flag,
        source.address_type,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    )

-- Handle inserts without updates: add new record
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN 
    INSERT (
        customer_address_hk,
        address_id,
        customer_id_fk,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag,
        address_type,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        hash(SHA1_hex(CONCAT(
            COALESCE(source.address_id,''), COALESCE(source.customer_id_fk,''), 
            COALESCE(source.flat_no,''), COALESCE(source.house_no,''), 
            COALESCE(source.floor,''), COALESCE(source.building,''), 
            COALESCE(source.landmark,''), COALESCE(source.locality,''), 
            COALESCE(source.city,''), COALESCE(source.state,''), 
            COALESCE(source.pincode,''), COALESCE(source.coordinates,''), 
            COALESCE(source.primary_flag,''), COALESCE(source.address_type,'')
        ))),
        source.address_id,
        source.customer_id_fk,
        source.flat_no,
        source.house_no,
        source.floor,
        source.building,
        source.landmark,
        source.locality,
        source.city,
        source.state,
        source.pincode,
        source.coordinates,
        source.primary_flag,
        source.address_type,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    );
