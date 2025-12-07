/*
Docstring:
Defines enriched.date_dim table as a reusable calendar dimension with surrogate hash key.
Populates dates from current date back to earliest order date using recursive CTE.
Provides attributes for year, quarter, month, week, day of year, day of week, day of month, and day name.
*/
USE ROLE data_engineer;
USE WAREHOUSE foodpanda_wh;
USE DATABASE foodpanda_db.enriched;

CREATE OR REPLACE TABLE enriched.date_dim (
    date_dim_hk NUMBER PRIMARY KEY COMMENT 'Date Dim HK (EDW)',
    calendar_date DATE UNIQUE COMMENT 'Calendar date',
    year NUMBER COMMENT 'Year',
    quarter NUMBER COMMENT 'Quarter (1-4)',
    month NUMBER COMMENT 'Month (1-12)',
    week NUMBER COMMENT 'Week of the year',
    day_of_year NUMBER COMMENT 'Day of the year (1-365/366)',
    day_of_week NUMBER COMMENT 'Day of the week (1-7)',
    day_of_the_month NUMBER COMMENT 'Day of the month (1-31)',
    day_name STRING COMMENT 'Name of the day (e.g., Monday)'
)
COMMENT = 'Date dimension table populated from current date down to earliest order date.';

INSERT INTO enriched.date_dim
WITH RECURSIVE my_date_dim_cte AS (
    -- anchor clause: start from today
    SELECT 
        CURRENT_DATE() AS calendar_date,
        YEAR(CURRENT_DATE()) AS year,
        QUARTER(CURRENT_DATE()) AS quarter,
        MONTH(CURRENT_DATE()) AS month,
        WEEK(CURRENT_DATE()) AS week,
        DAYOFYEAR(CURRENT_DATE()) AS day_of_year,
        DAYOFWEEK(CURRENT_DATE()) AS day_of_week,
        DAY(CURRENT_DATE()) AS day_of_the_month,
        DAYNAME(CURRENT_DATE()) AS day_name

    UNION ALL

    -- recursive clause: go backwards one day at a time
    SELECT 
        DATEADD('day', -1, calendar_date) AS calendar_date,
        YEAR(DATEADD('day', -1, calendar_date)) AS year,
        QUARTER(DATEADD('day', -1, calendar_date)) AS quarter,
        MONTH(DATEADD('day', -1, calendar_date)) AS month,
        WEEK(DATEADD('day', -1, calendar_date)) AS week,
        DAYOFYEAR(DATEADD('day', -1, calendar_date)) AS day_of_year,
        DAYOFWEEK(DATEADD('day', -1, calendar_date)) AS day_of_week,
        DAY(DATEADD('day', -1, calendar_date)) AS day_of_the_month,
        DAYNAME(DATEADD('day', -1, calendar_date)) AS day_name
    FROM my_date_dim_cte
    WHERE DATEADD('day', -1, calendar_date) >= (SELECT MIN(order_date) FROM curated.Orders)
)
SELECT 
    HASH(SHA1_HEX(calendar_date)) AS date_dim_hk,
    calendar_date,
    year,
    quarter,
    month,
    week,
    day_of_year,
    day_of_week,
    day_of_the_month,
    day_name
FROM my_date_dim_cte;
