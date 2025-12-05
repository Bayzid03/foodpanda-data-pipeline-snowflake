-- KPI 1: Current Menu Availability Rate
-- Chart Used: Pie chart
SELECT 
    Availability,
    COUNT(*) AS item_count
FROM enriched.menu_dim
WHERE Is_Current = TRUE
GROUP BY Availability;

-- KPI 2: Average Price by Category
-- Chart Used: Bar chart
SELECT 
    Category,
    ROUND(AVG(Price), 2) AS avg_price
FROM enriched.menu_dim
WHERE Is_Current = TRUE
GROUP BY Category;

-- KPI 3: Price Change Frequency per Item
-- Chart Used: Line chart
SELECT 
    Menu_ID,
    COUNT(*) AS total_versions
FROM enriched.menu_dim
GROUP BY Menu_ID;

-- Overall average price change frequency
SELECT 
    ROUND(AVG(total_versions), 2) AS avg_changes_per_item
FROM (
    SELECT Menu_ID, COUNT(*) AS total_versions
    FROM enriched.menu_dim
    GROUP BY Menu_ID
);

-- KPI 4: Average Lifecycle Duration of Menu Items
-- Chart Used: Bar chart
SELECT 
    Menu_ID,
    ROUND(AVG(DATEDIFF(day, Eff_Start_Date, Eff_End_Date)), 2) AS avg_days_active
FROM enriched.menu_dim
WHERE Eff_End_Date IS NOT NULL
GROUP BY Menu_ID;

-- KPI 5: Current vs Historical Item Mix
-- Chart Used: Pie chart
SELECT 
    Is_Current,
    COUNT(*) AS item_count
FROM enriched.menu_dim
GROUP BY Is_Current;

-- KPI 6: Top Items by Price Growth
-- Chart Used: KPI card or Bar chart
SELECT 
    Menu_ID,
    MIN(Price) AS initial_price,
    MAX(Price) AS latest_price,
    (MAX(Price) - MIN(Price)) AS price_growth
FROM enriched.menu_dim
GROUP BY Menu_ID
ORDER BY price_growth DESC
LIMIT 10;
