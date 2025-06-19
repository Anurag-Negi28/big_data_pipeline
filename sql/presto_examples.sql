-- Presto/Trino Examples for Distributed Query Processing
-- These queries demonstrate Presto's federation and performance capabilities

-- 1. Cross-Database Federation Query
SELECT 
  o.order_id,
  o.customer_id,
  o.product_id,
  o.quantity * o.price as order_value,
  c.customer_name,
  c.customer_email,
  p.product_name,
  p.category,
  p.supplier_name
FROM hive.sales.orders o
JOIN postgresql.crm.customers c ON o.customer_id = c.customer_id
JOIN mysql.inventory.products p ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY order_value DESC
LIMIT 1000;

-- 2. Advanced Analytics with Array and Map Functions
WITH customer_product_history AS (
  SELECT 
    customer_id,
    ARRAY_AGG(DISTINCT product_id) as purchased_products,
    ARRAY_AGG(DISTINCT category) as purchased_categories,
    MAP_AGG(product_id, quantity * price) as product_revenues,
    ARRAY_AGG(order_date ORDER BY order_date) as order_timeline
  FROM orders
  GROUP BY customer_id
)
SELECT 
  customer_id,
  CARDINALITY(purchased_products) as unique_products_count,
  CARDINALITY(purchased_categories) as unique_categories_count,
  -- Find most valuable product for each customer
  (SELECT key FROM (SELECT key, value FROM UNNEST(product_revenues) AS t(key, value) 
                   ORDER BY value DESC LIMIT 1)) as top_product_id,
  -- Calculate days between first and last order
  DATE_DIFF('day', 
           CAST(order_timeline[1] AS DATE), 
           CAST(order_timeline[CARDINALITY(order_timeline)] AS DATE)) as customer_lifespan_days,
  -- Get purchase frequency
  CARDINALITY(order_timeline) as total_orders
FROM customer_product_history
WHERE CARDINALITY(purchased_products) > 1;

-- 3. Approximate Algorithms for Large Scale Analytics
SELECT 
  category,
  -- Exact counts
  COUNT(*) as exact_order_count,
  COUNT(DISTINCT customer_id) as exact_customer_count,
  
  -- Approximate counts (faster for very large datasets)
  APPROX_DISTINCT(customer_id) as approx_customer_count,
  APPROX_DISTINCT(product_id) as approx_product_count,
  
  -- Approximate percentiles
  APPROX_PERCENTILE(quantity * price, 0.5) as median_order_value,
  APPROX_PERCENTILE(quantity * price, 0.95) as p95_order_value,
  APPROX_PERCENTILE(quantity * price, 0.99) as p99_order_value,
  
  -- Statistical aggregates
  AVG(quantity * price) as avg_order_value,
  STDDEV(quantity * price) as stddev_order_value,
  
  -- Approximate most frequent values
  APPROX_TOP_K(product_id, 5) as top_5_products
FROM orders
GROUP BY category
ORDER BY exact_order_count DESC;

-- 4. Complex Time Series Analysis with Custom Functions
WITH daily_metrics AS (
  SELECT 
    order_date,
    COUNT(*) as daily_orders,
    SUM(quantity * price) as daily_revenue,
    COUNT(DISTINCT customer_id) as daily_unique_customers,
    AVG(quantity * price) as daily_avg_order_value
  FROM orders
  GROUP BY order_date
),
enhanced_metrics AS (
  SELECT *,
    -- Moving averages
    AVG(daily_revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7_revenue,
    AVG(daily_orders) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7_orders,
    
    -- Growth rates
    LAG(daily_revenue, 1) OVER (ORDER BY order_date) as prev_day_revenue,
    LAG(daily_revenue, 7) OVER (ORDER BY order_date) as prev_week_revenue,
    
    -- Seasonal comparisons
    LAG(daily_revenue, 28) OVER (ORDER BY order_date) as same_day_4_weeks_ago,
    
    -- Cumulative metrics
    SUM(daily_revenue) OVER (ORDER BY order_date) as cumulative_revenue,
    ROW_NUMBER() OVER (ORDER BY order_date) as day_number
  FROM daily_metrics
)
SELECT 
  order_date,
  daily_revenue,
  ma_7_revenue,
  -- Day-over-day growth
  CASE WHEN prev_day_revenue > 0 
       THEN (daily_revenue - prev_day_revenue) / prev_day_revenue * 100.0 
       ELSE NULL END as dod_growth_pct,
  -- Week-over-week growth
  CASE WHEN prev_week_revenue > 0 
       THEN (daily_revenue - prev_week_revenue) / prev_week_revenue * 100.0 
       ELSE NULL END as wow_growth_pct,
  -- Month-over-month comparison
  CASE WHEN same_day_4_weeks_ago > 0 
       THEN (daily_revenue - same_day_4_weeks_ago) / same_day_4_weeks_ago * 100.0 
       ELSE NULL END as mom_growth_pct,
  -- Running total
  cumulative_revenue,
  -- Revenue contribution to total
  daily_revenue / MAX(cumulative_revenue) OVER() * 100.0 as pct_of_total_revenue
FROM enhanced_metrics
ORDER BY order_date;