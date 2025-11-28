-- KPI 1: Active vs. Expired Addresses
-- Chart Used: Pie chart (to compare active vs expired counts)
SELECT 
    is_current,
    COUNT(*) AS address_count
FROM enriched.customer_address_dim
GROUP BY is_current;


-- KPI 2: Primary Address Coverage (%)
-- Chart Used: Single KPI card (to show % coverage clearly)
SELECT 
    ROUND(
        (COUNT(DISTINCT CASE WHEN primary_flag = 'Y' AND is_current = TRUE THEN customer_id_fk END) 
         / COUNT(DISTINCT customer_id_fk)) * 100, 2
    ) AS primary_address_coverage_pct
FROM enriched.customer_address_dim;


-- KPI 3: Address Type Distribution
-- Chart Used: Pie chart (to show share of Home vs Office vs Other)
SELECT 
    address_type,
    COUNT(*) AS address_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM enriched.customer_address_dim
WHERE is_current = TRUE
GROUP BY address_type;


-- KPI 4: Address Change Frequency
-- Chart Used: Line chart (to show distribution of changes per customer)
SELECT 
    customer_id_fk,
    COUNT(*) AS total_address_versions
FROM enriched.customer_address_dim
GROUP BY customer_id_fk;

-- Overall average change frequency
SELECT 
    ROUND(AVG(total_address_versions), 2) AS avg_address_changes_per_customer
FROM (
    SELECT customer_id_fk, COUNT(*) AS total_address_versions
    FROM enriched.customer_address_dim
    GROUP BY customer_id_fk
);


-- KPI 5: City/State Coverage Ratio
-- Chart Used: Map visualization (best for geographic coverage) and Bar chart (unique counts)
SELECT 
    COUNT(DISTINCT city) AS unique_cities,
    COUNT(DISTINCT state) AS unique_states
FROM enriched.customer_address_dim
WHERE is_current = TRUE;
