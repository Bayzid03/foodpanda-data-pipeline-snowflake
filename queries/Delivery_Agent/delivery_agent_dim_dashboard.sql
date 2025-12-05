-- KPI 1: Active vs Inactive Delivery Agents
-- Chart Used: Pie chart
SELECT 
    status,
    COUNT(*) AS agent_count
FROM enriched.delivery_agent_dim
WHERE is_current = TRUE
GROUP BY status;

-- KPI 2: Average Rating by Vehicle Type
-- Chart Used: Bar chart
SELECT 
    vehicle_type,
    ROUND(AVG(rating), 2) AS avg_rating
FROM enriched.delivery_agent_dim
WHERE is_current = TRUE
GROUP BY vehicle_type;

-- KPI 3: Gender Distribution of Current Agents
-- Chart Used: Pie chart
SELECT 
    gender,
    COUNT(*) AS agent_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM enriched.delivery_agent_dim
WHERE is_current = TRUE
GROUP BY gender;

-- KPI 4: Rating Change Frequency per Agent
-- Chart Used: Line chart
SELECT 
    delivery_agent_id,
    COUNT(*) AS total_versions
FROM enriched.delivery_agent_dim
GROUP BY delivery_agent_id;

-- Overall average rating change frequency
SELECT 
    ROUND(AVG(total_versions), 2) AS avg_changes_per_agent
FROM (
    SELECT delivery_agent_id, COUNT(*) AS total_versions
    FROM enriched.delivery_agent_dim
    GROUP BY delivery_agent_id
);

-- KPI 5: Average Lifecycle Duration of Agent Records
-- Chart Used: Bar chart
SELECT 
    delivery_agent_id,
    ROUND(AVG(DATEDIFF(day, eff_start_date, eff_end_date)), 2) AS avg_days_active
FROM enriched.delivery_agent_dim
WHERE eff_end_date IS NOT NULL
GROUP BY delivery_agent_id;

-- KPI 6: Top Agents by Rating Growth
-- Chart Used: KPI card or Bar chart
SELECT 
    delivery_agent_id,
    MIN(rating) AS initial_rating,
    MAX(rating) AS latest_rating,
    (MAX(rating) - MIN(rating)) AS rating_growth
FROM enriched.delivery_agent_dim
GROUP BY delivery_agent_id
ORDER BY rating_growth DESC
LIMIT 10;
