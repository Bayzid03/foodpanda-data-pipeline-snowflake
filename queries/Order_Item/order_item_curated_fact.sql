/*
Defines curated.order_item fact table with standardized datatypes and audit metadata.
Creates stream curated.order_item_stm to capture inserts, updates, and deletes.
Populates data from raw.orderitem_stm using MERGE; SCD2 not applied.
*/
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.order_item (
    order_item_sk NUMBER AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate Key (EDW)',
    order_item_id NUMBER NOT NULL UNIQUE COMMENT 'Primary Key (Source System)',
    order_id_fk NUMBER NOT NULL COMMENT 'Order FK (Source System)',
    menu_id_fk NUMBER NOT NULL COMMENT 'Menu FK (Source System)',
    quantity NUMBER(10,2) COMMENT 'Quantity',
    price NUMBER(10,2) COMMENT 'Unit price',
    subtotal NUMBER(10,2) COMMENT 'Line subtotal',
    created_dt TIMESTAMP COMMENT 'Record creation date',
    modified_dt TIMESTAMP COMMENT 'Last modified date',

    -- audit columns
    _stg_file_name STRING COMMENT 'Source file name',
    _stg_file_load_ts TIMESTAMP COMMENT 'File load timestamp',
    _stg_file_md5 STRING COMMENT 'File content hash',
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Copy timestamp'
)
COMMENT = 'Curated order_item fact table with standardized datatypes and audit metadata. Populated via MERGE from raw stream; SCD2 not applied.';

CREATE OR REPLACE STREAM curated.order_item_stm
ON TABLE curated.order_item
COMMENT = 'Stream on curated.order_item to capture inserts, updates, and deletes.';

MERGE INTO curated.order_item AS target
USING raw.orderitem_stm AS source
ON target.order_item_id = TRY_TO_NUMBER(source.orderitemid)
   AND target.order_id_fk = TRY_TO_NUMBER(source.orderid)
   AND target.menu_id_fk = TRY_TO_NUMBER(source.menuid)
WHEN MATCHED THEN
    UPDATE SET 
        target.quantity = TRY_TO_DECIMAL(source.quantity,10,2),
        target.price = TRY_TO_DECIMAL(source.price,10,2),
        target.subtotal = TRY_TO_DECIMAL(source.subtotal,10,2),
        target.created_dt = TRY_TO_TIMESTAMP(source.createddate),
        target.modified_dt = TRY_TO_TIMESTAMP(source.modifieddate),
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        order_item_id,
        order_id_fk,
        menu_id_fk,
        quantity,
        price,
        subtotal,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TRY_TO_NUMBER(source.orderitemid),
        TRY_TO_NUMBER(source.orderid),
        TRY_TO_NUMBER(source.menuid),
        TRY_TO_DECIMAL(source.quantity,10,2),
        TRY_TO_DECIMAL(source.price,10,2),
        TRY_TO_DECIMAL(source.subtotal,10,2),
        TRY_TO_TIMESTAMP(source.createddate),
        TRY_TO_TIMESTAMP(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );
