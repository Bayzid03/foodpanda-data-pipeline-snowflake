/*
Defines raw.orderitem fact table for ingesting order item data as-is with audit metadata.
Creates append-only stream raw.orderitem_stm to capture delta changes from staged CSV files.
*/
USE ROLE data_engineer;
USE DATABASE foodpanda_db.raw;
USE WAREHOUSE foodpanda_wh;

CREATE OR REPLACE TABLE raw.orderitem (
    orderitemid TEXT COMMENT 'Primary Key (Source System)',
    orderid TEXT COMMENT 'Order FK (Source System)',
    menuid TEXT COMMENT 'Menu FK (Source System)',
    quantity TEXT COMMENT 'Quantity as text',
    price TEXT COMMENT 'Price as text',
    subtotal TEXT COMMENT 'Subtotal as text',
    createddate TEXT COMMENT 'Record creation date',
    modifieddate TEXT COMMENT 'Last modified date',

    -- audit columns
    _stg_file_name TEXT COMMENT 'Source file name',
    _stg_file_load_ts TIMESTAMP COMMENT 'File load timestamp',
    _stg_file_md5 TEXT COMMENT 'File content hash',
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Copy timestamp'
)
COMMENT = 'Raw orderitem fact table storing source data as-is. All columns are text except audit fields.';

CREATE OR REPLACE STREAM raw.orderitem_stm
ON TABLE raw.orderitem
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream on orderitem table capturing delta data.';

LIST @raw.csv_stg/initial/Order_Item.csv;

COPY INTO raw.orderitem (
    orderitemid, orderid, menuid, quantity, price,
    subtotal, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT 
        t.$1::TEXT AS orderitemid,
        t.$2::TEXT AS orderid,
        t.$3::TEXT AS menuid,
        t.$4::TEXT AS quantity,
        t.$5::TEXT AS price,
        t.$6::TEXT AS subtotal,
        t.$7::TEXT AS createddate,
        t.$8::TEXT AS modifieddate,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM @raw.csv_stg/initial/Order_Item.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'raw.csv_file_format')
ON_ERROR = ABORT_STATEMENT;
