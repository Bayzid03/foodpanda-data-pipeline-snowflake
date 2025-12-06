/*
Defines curated.delivery fact table with standardized datatypes and audit metadata.
Creates stream curated.delivery_stm to capture inserts, updates, and deletes.
Populates data from raw.delivery_stm using MERGE; SCD2 not applied.
*/
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.delivery (
    delivery_sk INT AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate Key (EDW)',
    delivery_id INT NOT NULL COMMENT 'Primary Key (Source System)',
    order_id_fk NUMBER NOT NULL COMMENT 'Order FK (Source System)',
    delivery_agent_id_fk NUMBER NOT NULL COMMENT 'Delivery Agent FK (Source System)',
    delivery_status STRING COMMENT 'Delivery status',
    estimated_time STRING COMMENT 'Estimated delivery time',
    customer_address_id_fk NUMBER NOT NULL COMMENT 'Customer Address FK (Source System)',
    delivery_date TIMESTAMP COMMENT 'Delivery date',
    created_date TIMESTAMP COMMENT 'Record creation date',
    modified_date TIMESTAMP COMMENT 'Last modified date',

    -- audit columns
    _stg_file_name STRING COMMENT 'Source file name',
    _stg_file_load_ts TIMESTAMP COMMENT 'File load timestamp',
    _stg_file_md5 STRING COMMENT 'File content hash',
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Copy timestamp'
)
COMMENT = 'Curated delivery fact table with standardized datatypes and audit metadata. Populated via MERGE from raw stream; SCD2 not applied.';

CREATE OR REPLACE STREAM curated.delivery_stm
ON TABLE curated.delivery
COMMENT = 'Stream on curated.delivery to capture inserts, updates, and deletes.';

MERGE INTO curated.delivery AS target
USING raw.delivery_stm AS source
ON target.delivery_id = TO_NUMBER(source.deliveryid)
   AND target.order_id_fk = TO_NUMBER(source.orderid)
   AND target.delivery_agent_id_fk = TO_NUMBER(source.deliveryagentid)
WHEN MATCHED THEN
    UPDATE SET
        delivery_status = source.deliverystatus,
        estimated_time = source.estimatedtime,
        customer_address_id_fk = TO_NUMBER(source.addressid),
        delivery_date = TO_TIMESTAMP(source.deliverydate),
        created_date = TO_TIMESTAMP(source.createddate),
        modified_date = TO_TIMESTAMP(source.modifieddate),
        _stg_file_name = source._stg_file_name,
        _stg_file_load_ts = source._stg_file_load_ts,
        _stg_file_md5 = source._stg_file_md5,
        _copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        delivery_id,
        order_id_fk,
        delivery_agent_id_fk,
        delivery_status,
        estimated_time,
        customer_address_id_fk,
        delivery_date,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TO_NUMBER(source.deliveryid),
        TO_NUMBER(source.orderid),
        TO_NUMBER(source.deliveryagentid),
        source.deliverystatus,
        source.estimatedtime,
        TO_NUMBER(source.addressid),
        TO_TIMESTAMP(source.deliverydate),
        TO_TIMESTAMP(source.createddate),
        TO_TIMESTAMP(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
