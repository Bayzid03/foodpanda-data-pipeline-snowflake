/*
Defines raw.delivery fact table for ingesting delivery data as-is with audit metadata.
Creates append-only stream raw.delivery_stm to capture delta changes from staged CSV files.
*/
USE ROLE data_engineer;
USE SCHEMA foodpanda_db.raw;
USE WAREHOUSE foodpanda_wh;

CREATE OR REPLACE TABLE raw.delivery (
    deliveryid TEXT COMMENT 'Primary Key (Source System)',
    orderid TEXT COMMENT 'Order FK (Source System)',
    deliveryagentid TEXT COMMENT 'Delivery Agent FK (Source System)',
    deliverystatus TEXT COMMENT 'Delivery status',
    estimatedtime TEXT COMMENT 'Estimated delivery time',
    addressid TEXT COMMENT 'Customer Address FK (Source System)',
    deliverydate TEXT COMMENT 'Delivery date',
    createddate TEXT COMMENT 'Record creation date',
    modifieddate TEXT COMMENT 'Last modified date',

    -- audit columns
    _stg_file_name TEXT COMMENT 'Source file name',
    _stg_file_load_ts TIMESTAMP COMMENT 'File load timestamp',
    _stg_file_md5 TEXT COMMENT 'File content hash',
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Copy timestamp'
)
COMMENT = 'Raw delivery fact table storing source data as-is. All columns are text except audit fields.';

CREATE OR REPLACE STREAM raw.delivery_stm
ON TABLE raw.delivery
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream on delivery table capturing delta data.';

COPY INTO raw.delivery (
    deliveryid, orderid, deliveryagentid, deliverystatus,
    estimatedtime, addressid, deliverydate, createddate,
    modifieddate, _stg_file_name, _stg_file_load_ts,
    _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT 
        t.$1::TEXT AS deliveryid,
        t.$2::TEXT AS orderid,
        t.$3::TEXT AS deliveryagentid,
        t.$4::TEXT AS deliverystatus,
        t.$5::TEXT AS estimatedtime,
        t.$6::TEXT AS addressid,
        t.$7::TEXT AS deliverydate,
        t.$8::TEXT AS createddate,
        t.$9::TEXT AS modifieddate,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM @raw.csv_stg/initial/Delivery.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'raw.csv_file_format')
ON_ERROR = ABORT_STATEMENT;
