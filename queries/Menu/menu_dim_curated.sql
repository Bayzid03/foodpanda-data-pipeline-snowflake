/*
Defines the curated.menu table with standardized datatypes and audit metadata.
Populates data from raw.menu_stm using MERGE to handle inserts and updates; SCD2 not applied.
*/

USE ROLE data_engineer;
USE SCHEMA foodpanda_db.curated;

CREATE OR REPLACE TABLE curated.menu (
    Menu_SK INT AUTOINCREMENT PRIMARY KEY COMMENT 'Surrogate Key (EDW)',  
    Menu_ID INT NOT NULL UNIQUE COMMENT 'Primary Key (Source System)',  
    Restaurant_ID_FK INT COMMENT 'Restaurant FK (Source System)',  
    Item_Name STRING NOT NULL,                        
    Description STRING NOT NULL,                     
    Price DECIMAL(10, 2) NOT NULL,                   
    Category STRING,                        
    Availability BOOLEAN,                   
    Item_Type STRING,                        
    Created_dt TIMESTAMP_TZ,               
    Modified_dt TIMESTAMP_TZ,              

    -- Audit columns
    _STG_FILE_NAME STRING,                  
    _STG_FILE_LOAD_TS TIMESTAMP_TZ,        
    _STG_FILE_MD5 STRING,                   
    _COPY_DATA_TS TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP
)
COMMENT = 'Menu entity under curated schema with standardized datatypes. Populated via MERGE from raw.menu_stm. This table does not support SCD2.';

CREATE OR REPLACE STREAM curated.menu_stm 
ON TABLE curated.menu
COMMENT = 'Stream object on curated.menu to track inserts, updates, and deletes.';

MERGE INTO curated.menu AS target
USING (
    SELECT 
        TRY_CAST(menuid AS INT) AS Menu_ID,
        TRY_CAST(restaurantid AS INT) AS Restaurant_ID_FK,
        TRIM(itemname) AS Item_Name,
        TRIM(description) AS Description,
        TRY_CAST(price AS DECIMAL(10, 2)) AS Price,
        TRIM(category) AS Category,
        CASE 
            WHEN LOWER(availability) = 'true' THEN TRUE
            WHEN LOWER(availability) = 'false' THEN FALSE
            ELSE NULL
        END AS Availability,
        TRIM(itemtype) AS Item_Type,
        TRY_CAST(createddate AS TIMESTAMP_TZ) AS Created_dt,
        TRY_CAST(modifieddate AS TIMESTAMP_TZ) AS Modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM raw.menu_stm
) AS source
ON target.Menu_ID = source.Menu_ID
WHEN MATCHED THEN
    UPDATE SET
        Restaurant_ID_FK = source.Restaurant_ID_FK,
        Item_Name = source.Item_Name,
        Description = source.Description,
        Price = source.Price,
        Category = source.Category,
        Availability = source.Availability,
        Item_Type = source.Item_Type,
        Created_dt = source.Created_dt,  
        Modified_dt = source.Modified_dt,  
        _STG_FILE_NAME = source._stg_file_name,
        _STG_FILE_LOAD_TS = source._stg_file_load_ts,
        _STG_FILE_MD5 = source._stg_file_md5,
        _COPY_DATA_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        Created_dt, 
        Modified_dt,  
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        source.Created_dt,  
        source.Modified_dt,  
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        CURRENT_TIMESTAMP
    );
