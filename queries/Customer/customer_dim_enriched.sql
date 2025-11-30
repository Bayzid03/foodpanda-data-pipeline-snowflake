/*
Implements Slowly Changing Dimension Type 2 (SCD2) for the customer_dim table.
Merges data from curated.customer_stream, expiring old records when attributes change
and inserting new records with updated values. Surrogate hash key ensures uniqueness,
while effective dates and current flag track historical versions.
*/

USE SCHEMA foodpanda_db.enriched;

MERGE INTO enriched.customer_dim AS target
USING curated.customer_stream AS source
ON target.customer_id = source.customer_id

-- Expire old record when an update occurs
WHEN MATCHED 
    AND source.METADATA$ACTION = 'UPDATE' 
    AND source.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        target.eff_end_date = CURRENT_TIMESTAMP,
        target.is_current = FALSE

-- Insert new record when attributes change or new customer arrives
WHEN NOT MATCHED THEN
    INSERT (
        customer_hk,
        customer_id,
        name,
        mobile,
        email,
        login_by_using,
        gender,
        dob,
        anniversary,
        preferences,
        eff_start_date,
        eff_end_date,
        is_current
    )
    VALUES (
        HASH(SHA1_HEX(CONCAT(
            COALESCE(source.customer_id,''), COALESCE(source.name,''), COALESCE(source.mobile,''), 
            COALESCE(source.email,''), COALESCE(source.login_by_using,''), COALESCE(source.gender,''), 
            COALESCE(source.dob,''), COALESCE(source.anniversary,''), COALESCE(source.preferences,'')
        ))),
        source.customer_id,
        source.name,
        source.mobile,
        source.email,
        source.login_by_using,
        source.gender,
        source.dob,
        source.anniversary,
        source.preferences,
        CURRENT_TIMESTAMP,
        NULL,
        TRUE
    );
