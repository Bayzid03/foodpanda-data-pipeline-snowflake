/*
Defines enriched.delivery_agent_dim table with SCD Type 2 support.
Tracks historical changes using hash key, effective dates, and current flag.
Populates from curated.delivery_agent_stream via MERGE; old records expired, new versions inserted.
*/
USE SCHEMA foodpanda_db.enriched;

CREATE OR REPLACE TABLE enriched.delivery_agent_dim(
    delivery_agent_hk NUMBER PRIMARY KEY COMMENT 'Surrogate hash key (EDW)',
    delivery_agent_id NUMBER NOT NULL COMMENT 'Business key from source system',
    name STRING NOT NULL COMMENT 'Delivery agent name',
    phone STRING UNIQUE COMMENT 'Unique phone number',
    vehicle_type STRING COMMENT 'Type of vehicle',
    location_id_fk NUMBER NOT NULL COMMENT 'Location foreign key',
    status STRING COMMENT 'Current status',
    gender STRING COMMENT 'Gender',
    rating NUMBER(4,2) COMMENT 'Agent rating',
    eff_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Effective start date',
    eff_end_date TIMESTAMP COMMENT 'Effective end date (NULL for current record)',
    is_current BOOLEAN DEFAULT TRUE COMMENT 'Flag for current record'
)
COMMENT = 'Enriched delivery_agent_dim table with SCD2 history tracking.';

MERGE INTO enriched.delivery_agent_dim AS target
USING curated.delivery_agent_stream AS source
ON target.delivery_agent_id = source.delivery_agent_id

-- Expire old record when attributes change
WHEN MATCHED AND source.METADATA$ACTION = 'UPDATE' AND source.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET 
        target.eff_end_date = CURRENT_TIMESTAMP,
        target.is_current = FALSE

-- Insert new record for new or changed attributes
WHEN NOT MATCHED THEN
    INSERT(
        delivery_agent_hk,
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id_fk,
        status,
        gender,
        rating,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES(
        HASH(SHA1_HEX(CONCAT(
            COALESCE(source.delivery_agent_id,''), COALESCE(source.name,''), COALESCE(source.phone,''), 
            COALESCE(source.vehicle_type,''), COALESCE(source.location_id_fk,''), COALESCE(source.status,''), 
            COALESCE(source.gender,''), COALESCE(source.rating,'')
        ))),
        source.delivery_agent_id,
        source.name,
        source.phone,
        source.vehicle_type,
        source.location_id_fk,
        source.status,
        source.gender,
        source.rating,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    );
