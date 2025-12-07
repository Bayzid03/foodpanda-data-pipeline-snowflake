/*
Defines analytic KPI views on top of enriched.order_item_fact joined with date_dim and restaurant_dim.
Provides yearly, monthly, daily, weekday, and restaurant-level revenue KPIs for delivered orders.
Enhancements include revenue growth %, top restaurants, and weekday vs weekend splits.
These views support BI dashboards and recruiter-facing portfolio demonstrations.
*/
USE ROLE data_engineer;
USE DATABASE foodpanda_db.enriched;

-- =========================
-- Yearly Revenue KPIs
-- =========================
CREATE OR REPLACE VIEW enriched.vw_yearly_revenue_kpis AS
SELECT
    d.year,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value,
    ROUND((SUM(fact.subtotal) - LAG(SUM(fact.subtotal)) OVER (ORDER BY d.year)) 
          / NULLIF(LAG(SUM(fact.subtotal)) OVER (ORDER BY d.year),0) * 100,2) AS revenue_growth_pct
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY d.year
ORDER BY d.year;

-- =========================
-- Monthly Revenue KPIs
-- =========================
CREATE OR REPLACE VIEW enriched.vw_monthly_revenue_kpis AS
SELECT
    d.year,
    d.month,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value,
    ROUND((SUM(fact.subtotal) - LAG(SUM(fact.subtotal)) OVER (PARTITION BY d.year ORDER BY d.month)) 
          / NULLIF(LAG(SUM(fact.subtotal)) OVER (PARTITION BY d.year ORDER BY d.month),0) * 100,2) AS revenue_growth_pct
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- =========================
-- Daily Revenue KPIs
-- =========================
CREATE OR REPLACE VIEW enriched.vw_daily_revenue_kpis AS
SELECT
    d.year,
    d.month,
    d.day_of_the_month AS day,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY d.year, d.month, d.day_of_the_month
ORDER BY d.year, d.month, d.day_of_the_month;

-- =========================
-- Day-of-Week Revenue KPIs
-- =========================
CREATE OR REPLACE VIEW enriched.vw_day_revenue_kpis AS
SELECT
    d.year,
    d.month,
    d.day_name,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY d.year, d.month, d.day_name
ORDER BY d.year, d.month, d.day_name;

-- =========================
-- Monthly Revenue by Restaurant
-- =========================
CREATE OR REPLACE VIEW enriched.vw_monthly_revenue_by_restaurant AS
SELECT
    d.year,
    d.month,
    r.name AS restaurant_name,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
JOIN enriched.restaurant_dim r
  ON fact.restaurant_dim_key = r.restaurant_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY d.year, d.month, r.name
ORDER BY d.year, d.month;

-- =========================
-- Weekday vs Weekend Split
-- =========================
CREATE OR REPLACE VIEW enriched.vw_weekday_vs_weekend_revenue AS
SELECT
    CASE WHEN d.day_name IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item
FROM enriched.order_item_fact fact
JOIN enriched.date_dim d
  ON fact.order_date_dim_key = d.date_dim_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY day_type
ORDER BY day_type;

-- =========================
-- Top 5 Restaurants by Revenue
-- =========================
CREATE OR REPLACE VIEW enriched.vw_top5_restaurants_revenue AS
SELECT
    r.name AS restaurant_name,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order
FROM enriched.order_item_fact fact
JOIN enriched.restaurant_dim r
  ON fact.restaurant_dim_key = r.restaurant_hk
WHERE fact.delivery_status = 'Delivered'
GROUP BY r.name
ORDER BY total_revenue DESC
LIMIT 5;
