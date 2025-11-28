-- KPI 1: Active vs Inactive Locations
-- Chart Used: Bar chart or Pie chart
SELECT 
    active_flag,
    COUNT(*) AS location_count
FROM enriched.restaurant_location_dim
WHERE current_flag = TRUE
GROUP BY active_flag;

-- KPI 2: Union Territory vs State Locations
-- Chart Used: Bar chart
SELECT 
    is_union_territory,
    COUNT(*) AS location_count
FROM enriched.restaurant_location_dim
WHERE current_flag = TRUE
GROUP BY is_union_territory;

-- KPI 3: Capital City Coverage
-- Chart Used: KPI card
SELECT 
    COUNT(*) AS capital_city_locations
FROM enriched.restaurant_location_dim
WHERE current_flag = TRUE
  AND capital_city_flag = TRUE;

-- KPI 4: City Tier Distribution
-- Chart Used: Pie chart
SELECT 
    city_tier,
    COUNT(*) AS location_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM enriched.restaurant_location_dim
WHERE current_flag = TRUE
GROUP BY city_tier;

-- KPI 5: Location Change Frequency
-- Chart Used: Line chart
SELECT 
    location_id,
    COUNT(*) AS total_versions
FROM enriched.restaurant_location_dim
GROUP BY location_id;

-- Overall average change frequency
SELECT 
    ROUND(AVG(total_versions), 2) AS avg_changes_per_location
FROM (
    SELECT location_id, COUNT(*) AS total_versions
    FROM enriched.restaurant_location_dim
    GROUP BY location_id
);
