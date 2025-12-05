/*
Defines curated.delivery_agent table with standardized datatypes and audit metadata.
Creates stream to track inserts, updates, and deletes.
Populates data from raw.delivery_agent_stream using MERGE;
*/
USE ROLE data_engineer;
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.delivery_agent (
    delivery_agent_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_agent_id INT NOT NULL UNIQUE comment 'Primary Key (Source System)',               -- Delivery agent ID as integer
    name STRING NOT NULL,                
    phone STRING NOT NULL,                 
    vehicle_type STRING NOT NULL,                 
    location_id_fk INT comment 'Location FK(Source System)',                     
    status STRING,                       
    gender STRING,                       
    rating number(4,2),                        
    created_dt TIMESTAMP_NTZ,          
    modified_dt TIMESTAMP_NTZ,         

    -- Audit columns with appropriate data types
    _stg_file_name STRING,               
    _stg_file_load_ts TIMESTAMP,         
    _stg_file_md5 STRING,                
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
)
COMMENT = 'Curated delivery_agent table with standardized datatypes and audit metadata. Populated via MERGE from raw stream; SCD2 not applied.';

create or replace stream curated.delivery_agent_stream
on table curated.delivery_agent
COMMENT = 'Stream on curated.delivery_agent to capture inserts, updates, and deletes.';

MERGE INTO curated.delivery_agent AS target
USING raw.delivery_agent_stream AS source
ON target.delivery_agent_id = source.deliveryagentid
WHEN MATCHED THEN
    UPDATE SET
        target.phone = source.phone,
        target.vehicle_type = source.vehicletype,
        target.location_id_fk = TRY_TO_NUMBER(source.locationid),
        target.status = source.status,
        target.gender = source.gender,
        target.rating = TRY_TO_DECIMAL(source.rating,4,2),
        target.created_dt = TRY_TO_TIMESTAMP(source.createddate),
        target.modified_dt = TRY_TO_TIMESTAMP(source.modifieddate),
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        delivery_agent_id,
        name,
        phone,
        vehicle_type,
        location_id_fk,
        status,
        gender,
        rating,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TRY_TO_NUMBER(source.deliveryagentid),
        source.name,
        source.phone,
        source.vehicletype,
        TRY_TO_NUMBER(source.locationid),
        source.status,
        source.gender,
        TRY_TO_NUMBER(source.rating),
        TRY_TO_TIMESTAMP(source.createddate),
        TRY_TO_TIMESTAMP(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP()
    );

select * from curated.delivery_agent_stream;
