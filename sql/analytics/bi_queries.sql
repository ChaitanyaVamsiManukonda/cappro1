-- E-Commerce BI Analytical Queries
-- Designed for Snowflake Data Warehouse

-- 1. Most Viewed Products
SELECT 
    p.product_id,
    p.product_name,
    COUNT(*) as view_count
FROM fact_user_activity f
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.action = 'view'
GROUP BY p.product_id, p.product_name
ORDER BY view_count DESC
LIMIT 10;

-- 2. Conversion Rates (Sessions with purchase / Total engagement sessions)
SELECT
    COUNT(CASE WHEN total_purchases > 0 THEN 1 END) as purchasing_sessions,
    COUNT(*) as total_sessions,
    ROUND(COUNT(CASE WHEN total_purchases > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as conversion_rate_percent
FROM agg_session_metrics
WHERE total_actions > 0;

-- 3. Average Session Duration
SELECT
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(duration_seconds)/60.0, 2) as avg_duration_minutes
FROM agg_session_metrics
WHERE duration_seconds > 0;

-- 4. Top Users by Total Purchases
SELECT 
    u.user_id,
    u.user_name,
    u.email,
    SUM(m.total_purchases) as total_items_purchased
FROM agg_session_metrics m
JOIN dim_user u ON m.user_id = u.user_id
WHERE m.total_purchases > 0
GROUP BY u.user_id, u.user_name, u.email
ORDER BY total_items_purchased DESC
LIMIT 10;

-- 5. Abandoned Cart Rate per Day
SELECT
    DATE(session_start) as session_date,
    COUNT(CASE WHEN is_abandoned_cart = 1 THEN 1 END) as abandoned_carts,
    COUNT(*) as total_sessions,
    ROUND(COUNT(CASE WHEN is_abandoned_cart = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as abandonment_rate_percent
FROM agg_session_metrics
GROUP BY DATE(session_start)
ORDER BY session_date DESC;

-- 6. Product Conversion Rate (Purchases / Views)
WITH product_stats AS (
    SELECT 
        product_id,
        SUM(CASE WHEN action = 'view' THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) as total_purchases
    FROM fact_user_activity
    GROUP BY product_id
)
SELECT 
    p.product_name,
    s.total_views,
    s.total_purchases,
    ROUND(s.total_purchases * 100.0 / NULLIF(s.total_views, 0), 2) as product_conversion_rate
FROM product_stats s
JOIN dim_product p ON s.product_id = p.product_id
ORDER BY product_conversion_rate DESC
LIMIT 10;

-- 7. Average Number of Actions per Session
SELECT
    ROUND(AVG(total_actions), 2) as avg_actions_per_session
FROM agg_session_metrics;

-- 8. Peak Activity Hours on the Site
SELECT 
    EXTRACT(HOUR FROM timestamp) as activity_hour,
    COUNT(*) as total_actions
FROM fact_user_activity
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY total_actions DESC;

-- 9. Sessions with Unusually Long Durations (Potential Bots or idle tabs)
SELECT 
    session_id,
    user_id,
    duration_seconds,
    total_actions
FROM agg_session_metrics
WHERE duration_seconds > 3600 -- Longer than 1 hour
ORDER BY duration_seconds DESC
LIMIT 20;

-- 10. Cohort Analysis: Users by Signup Month vs Purchases
SELECT 
    DATE_TRUNC('month', u.signup_date) as signup_cohort,
    COUNT(DISTINCT u.user_id) as total_users_in_cohort,
    SUM(m.total_purchases) as cohort_total_purchases,
    ROUND(SUM(m.total_purchases) * 1.0 / NULLIF(COUNT(DISTINCT u.user_id), 0), 2) as purchases_per_user
FROM dim_user u
LEFT JOIN agg_session_metrics m ON u.user_id = m.user_id
GROUP BY DATE_TRUNC('month', u.signup_date)
ORDER BY signup_cohort DESC;
