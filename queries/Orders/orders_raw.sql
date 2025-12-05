/*
Defines raw.orders table for ingesting order data as-is with audit metadata.
Creates append-only stream raw.orders_stm to capture delta changes from staged CSV files.
*/

USE ROLE data_engineer;
USE DATABASE foodpanda_db.raw;

CREATE OR REPLACE TABLE raw.orders (
    orderid TEXT COMMENT 'Primary Key (Source System)',
    customerid TEXT COMMENT 'Customer FK (Source System)',
    restaurantid TEXT COMMENT 'Restaurant FK (Source System)',
    orderdate TEXT COMMENT 'Order date as text',
    totalamount TEXT COMMENT 'Total amount as text',
    status TEXT COMMENT 'Order status',
    paymentmethod TEXT COMMENT 'Payment method',
    createddate TEXT COMMENT 'Created date as text',
    modifieddate TEXT COMMENT 'Modified date as text',

    -- audit columns
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 TEXT,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Raw orders table storing source data as-is. All columns are text except audit fields.';

CREATE OR REPLACE STREAM raw.orders_stm 
ON TABLE raw.orders
APPEND_ONLY = TRUE
COMMENT = 'Append-only stream on orders table capturing delta data';

LIST @raw.csv_stg/initial/Orders.csv;

COPY INTO raw.orders (
    orderid, customerid, restaurantid, orderdate, totalamount, 
    status, paymentmethod, createddate, modifieddate,
    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts
)
FROM (
    SELECT 
        t.$1::TEXT AS orderid,
        t.$2::TEXT AS customerid,
        t.$3::TEXT AS restaurantid,
        t.$4::TEXT AS orderdate,
        t.$5::TEXT AS totalamount,
        t.$6::TEXT AS status,
        t.$7::TEXT AS paymentmethod,
        t.$8::TEXT AS createddate,
        t.$9::TEXT AS modifieddate,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM @raw.csv_stg/initial/Orders.csv t
)
FILE_FORMAT = (FORMAT_NAME = 'raw.csv_file_format')
ON_ERROR = ABORT_STATEMENT;
