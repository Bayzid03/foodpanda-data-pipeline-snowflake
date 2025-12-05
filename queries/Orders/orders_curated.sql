/*
Defines curated.orders fact table with standardized datatypes and audit metadata.
Creates stream curated.orders_stm to capture inserts, updates, and deletes.
Populates data from raw.orders_stm using MERGE; SCD2 not applied.
*/
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.orders (
    order_sk NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate Key (EDW)',
    order_id BIGINT UNIQUE COMMENT 'Primary Key (Source System)',
    customer_id_fk BIGINT COMMENT 'Customer FK (Source System)',
    restaurant_id_fk BIGINT COMMENT 'Restaurant FK (Source System)',
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status STRING,
    payment_method STRING,
    created_dt TIMESTAMP_TZ COMMENT 'Record creation date',
    modified_dt TIMESTAMP_TZ COMMENT 'Last modified date',

    -- audit columns
    _stg_file_name STRING COMMENT 'Source file name',
    _stg_file_load_ts TIMESTAMP_NTZ COMMENT 'File load timestamp',
    _stg_file_md5 STRING COMMENT 'File content hash',
    _copy_data_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP COMMENT 'Copy timestamp'
)
COMMENT = 'Curated orders fact table with standardized datatypes and audit metadata. Populated via MERGE from raw stream; SCD2 not applied.';

CREATE OR REPLACE STREAM curated.orders_stm
ON TABLE curated.orders
COMMENT = 'Stream on curated.orders to capture inserts, updates, and deletes.';

MERGE INTO curated.orders AS target
USING raw.orders_stm AS source
ON target.order_id = TRY_TO_NUMBER(source.orderid)
WHEN MATCHED THEN
    UPDATE SET
        total_amount = TRY_TO_DECIMAL(source.totalamount),
        status = source.status,
        payment_method = source.paymentmethod,
        modified_dt = TRY_TO_TIMESTAMP_TZ(source.modifieddate),
        _stg_file_name = source._stg_file_name,
        _stg_file_load_ts = source._stg_file_load_ts,
        _stg_file_md5 = source._stg_file_md5,
        _copy_data_ts = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id_fk,
        restaurant_id_fk,
        order_date,
        total_amount,
        status,
        payment_method,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TRY_TO_NUMBER(source.orderid),
        TRY_TO_NUMBER(source.customerid),
        TRY_TO_NUMBER(source.restaurantid),
        TRY_TO_TIMESTAMP(source.orderdate),
        TRY_TO_DECIMAL(source.totalamount),
        source.status,
        source.paymentmethod,
        TRY_TO_TIMESTAMP_TZ(source.createddate),
        TRY_TO_TIMESTAMP_TZ(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );
