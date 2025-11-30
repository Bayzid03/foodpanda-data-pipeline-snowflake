/*
Defines the curated.customer table, its stream, and logic to load and merge data from raw.customer_stream.
Casts source text fields into proper datatypes (timestamps, dates, strings) and applies audit metadata.
The stream enables downstream enrichment by tracking changes in curated.customer.
*/

use schema foodpanda_db.curated;

-- Create curated customer table
CREATE OR REPLACE TABLE curated.customer (
    customer_sk NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',
    customer_id INT comment 'Primary Key (Source Data)',
    name STRING,
    mobile STRING,
    email STRING,
    login_by_using STRING,
    gender STRING,
    dob TIMESTAMP_TZ,
    anniversary TIMESTAMP_TZ,
    preferences STRING,
    created_date TIMESTAMP_TZ,
    modified_date TIMESTAMP_TZ,
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
comment = 'Curated customer table with standardized datatypes and audit metadata.';

-- Create stream on curated.customer
CREATE OR REPLACE STREAM curated.customer_stream 
ON TABLE curated.customer
APPEND_ONLY = TRUE
COMMENT = 'Stream to track inserts/updates in curated.customer for enrichment.';

-- Initial load from raw stream
INSERT INTO curated.customer (
    customer_id,
    name,
    mobile,
    email,
    login_by_using,
    gender,
    dob,
    anniversary,
    preferences,
    created_date,
    modified_date,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
)
SELECT 
    TRY_CAST(customerid AS INT) AS customer_id,
    name::STRING,
    mobile::STRING,
    email::STRING,
    loginbyusing::STRING AS login_by_using,
    gender::STRING,
    TRY_TO_DATE(dob, 'YYYY-MM-DD') AS dob,
    TRY_TO_DATE(anniversary, 'YYYY-MM-DD') AS anniversary,
    preferences::STRING,
    TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
    TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
FROM raw.customer_stream;

-- Merge logic for updates/inserts
MERGE INTO curated.customer AS target
USING (
    SELECT 
        TRY_CAST(customerid AS INT) AS customer_id,
        name::STRING AS name,
        mobile::STRING AS mobile,
        email::STRING AS email,
        loginbyusing::STRING AS login_by_using,
        gender::STRING AS gender,
        TRY_TO_DATE(dob, 'YYYY-MM-DD') AS dob,
        TRY_TO_DATE(anniversary, 'YYYY-MM-DD') AS anniversary,
        preferences::STRING AS preferences,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM raw.customer_stream
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.name = source.name,
        target.mobile = source.mobile,
        target.email = source.email,
        target.login_by_using = source.login_by_using,
        target.gender = source.gender,
        target.dob = source.dob,
        target.anniversary = source.anniversary,
        target.preferences = source.preferences,
        target.created_date = source.created_date,
        target.modified_date = source.modified_date,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN 
    INSERT (
        customer_id,
        name,
        mobile,
        email,
        login_by_using,
        gender,
        dob,
        anniversary,
        preferences,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.customer_id,
        source.name,
        source.mobile,
        source.email,
        source.login_by_using,
        source.gender,
        source.dob,
        source.anniversary,
        source.preferences,
        source.created_date,
        source.modified_date,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
