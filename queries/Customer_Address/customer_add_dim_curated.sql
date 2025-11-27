/*
Docstring:
This script defines the curated.customer_address table and stream, and merges data
from raw.customeraddress_stream into the curated layer with proper datatypes and audit metadata.
*/

use schema foodpanda_db.curated;

-- Create curated customer address table
CREATE OR REPLACE TABLE curated.customer_address (
    customer_address_sk NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (Auto-increment)',
    address_id INT comment 'Primary Key (Source Data)',
    customer_id_fk INT comment 'Customer Foreign Key (Source Data)',
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
    created_date TIMESTAMP_TZ,
    modified_date TIMESTAMP_TZ,

    -- Audit columns
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
comment = 'Curated customer address table with standardized datatypes and audit metadata.';

-- Create stream on curated table
create or replace stream curated.customeraddress_stream 
on table curated.customer_address
append = true
comment = 'Stream to track inserts/updates in curated.customer_address for enrichment.';

-- Merge raw stream data into curated table
MERGE INTO curated.customer_address AS target
USING (
    SELECT 
        CAST(addressid AS INT) AS address_id,
        CAST(customerid AS INT) AS customer_id_fk,
        flatno AS flat_no,
        houseno AS house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primaryflag AS primary_flag,
        addresstype AS address_type,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM raw.customeraddress_stream 
) AS source
ON target.address_id = source.address_id

WHEN NOT MATCHED THEN 
    INSERT (
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
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
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
        source.created_date,
        source.modified_date,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    )

WHEN MATCHED THEN 
    UPDATE SET 
        target.flat_no = source.flat_no,
        target.house_no = source.house_no,
        target.floor = source.floor,
        target.building = source.building,
        target.landmark = source.landmark,
        target.locality = source.locality,
        target.city = source.city,
        target.state = source.state,
        target.pincode = source.pincode,
        target.coordinates = source.coordinates,
        target.primary_flag = source.primary_flag,
        target.address_type = source.address_type,
        target.created_date = source.created_date,
        target.modified_date = source.modified_date,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts;
