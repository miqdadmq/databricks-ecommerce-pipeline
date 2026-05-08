-- ============================================================
-- Databricks SQL Dashboard Queries
-- Portfolio Project: E-Commerce Sales Pipeline
-- Copy each query into a separate Databricks SQL widget
-- ============================================================


-- ── Widget 1: Total Revenue (KPI Card) ──────────────────────
SELECT
    CONCAT('€ ', FORMAT_NUMBER(SUM(total_revenue), 0)) AS total_revenue
FROM monthly_revenue_by_category;


-- ── Widget 2: Total Orders (KPI Card) ───────────────────────
SELECT
    FORMAT_NUMBER(SUM(total_orders), 0) AS total_orders
FROM monthly_revenue_by_category;


-- ── Widget 3: Unique Customers (KPI Card) ───────────────────
SELECT
    FORMAT_NUMBER(COUNT(DISTINCT customer_id), 0) AS total_customers
FROM customer_segments;


-- ── Widget 4: Revenue Trend by Month (Line Chart) ───────────
SELECT
    year_month,
    SUM(total_revenue) AS revenue
FROM monthly_revenue_by_category
GROUP BY year_month
ORDER BY year_month;


-- ── Widget 5: Revenue by Category (Bar Chart) ───────────────
SELECT
    category,
    SUM(total_revenue)    AS total_revenue,
    SUM(total_orders)     AS total_orders,
    SUM(unique_customers) AS unique_customers
FROM monthly_revenue_by_category
GROUP BY category
ORDER BY total_revenue DESC;


-- ── Widget 6: Top 10 Products (Table) ───────────────────────
SELECT
    product_id,
    category,
    total_revenue,
    total_units_sold,
    total_orders,
    avg_unit_price
FROM top_products
ORDER BY total_revenue DESC
LIMIT 10;


-- ── Widget 7: Customer Segments (Pie / Donut Chart) ─────────
SELECT
    segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM customer_segments
GROUP BY segment
ORDER BY customer_count DESC;


-- ── Widget 8: Avg Order Value by Category (Bar Chart) ───────
SELECT
    category,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM monthly_revenue_by_category
GROUP BY category
ORDER BY avg_order_value DESC;


-- ── Widget 9: Weekend vs Weekday Orders (for silver layer) ───
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(order_id)               AS total_orders,
    ROUND(SUM(total_amount), 2)   AS total_revenue
FROM silver_orders
WHERE status = 'completed'
GROUP BY is_weekend;
