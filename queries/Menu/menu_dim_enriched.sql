/*
Defines the enriched.menu_dim table with SCD Type 2 logic for menu items.
Tracks historical changes from curated stream using hash keys, effective start/end dates, and current flags.
*/
use schema foodpanda_db.enriched;

CREATE OR REPLACE TABLE menu_dim (
    Menu_Dim_HK NUMBER primary key comment 'Menu Dim HK (EDW)',                       -- Hash key generated for Menu Dim table
    Menu_ID INT NOT NULL comment 'Primary Key (Source System)',                       -- Unique and non-null Menu_ID
    Restaurant_ID_FK INT NOT NULL comment 'Restaurant FK (Source System)',            -- Identifier for the restaurant
    Item_Name STRING,                           
    Description STRING,                         
    Price DECIMAL(10, 2),                       
    Category STRING,                           
    Availability BOOLEAN,                       
    Item_Type STRING,                           
    EFF_START_DATE TIMESTAMP_NTZ,               -- Effective start date of the record
    EFF_END_DATE TIMESTAMP_NTZ,                 -- Effective end date of the record
    IS_CURRENT BOOLEAN                         -- Flag to indicate if the record is current (True/False)
)
COMMENT = 'Menu dimension with SCD2 history. Tracks changes using effective dates, current flag, and hash key from Menu_ID and Restaurant_ID.';

MERGE INTO 
    menu_dim AS target
USING 
    menu_stm AS source
ON 
    target.Menu_ID = source.Menu_ID AND
    target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
    target.Item_Name = source.Item_Name AND
    target.Description = source.Description AND
    target.Price = source.Price AND
    target.Category = source.Category AND
    target.Availability = source.Availability AND
    target.Item_Type = source.Item_Type
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
            source.Item_Name, source.Description, source.Price, 
            source.Category, source.Availability, source.Item_Type))),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' 
    AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        Menu_Dim_HK,               -- Hash key
        Menu_ID,
        Restaurant_ID_FK,
        Item_Name,
        Description,
        Price,
        Category,
        Availability,
        Item_Type,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
            source.Item_Name, source.Description, source.Price, 
            source.Category, source.Availability, source.Item_Type))),  -- Hash key
        source.Menu_ID,
        source.Restaurant_ID_FK,
        source.Item_Name,
        source.Description,
        source.Price,
        source.Category,
        source.Availability,
        source.Item_Type,
        CURRENT_TIMESTAMP(),       -- Effective start date
        NULL,                      -- Effective end date (NULL for current record)
        TRUE                       -- IS_CURRENT = TRUE for new record
    );
