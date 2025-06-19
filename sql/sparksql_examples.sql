-- SparkSQL Examples for Distributed Data Processing
-- These queries demonstrate Spark's distributed SQL capabilities

-- 1. Complex Aggregation with Multiple Dimensions
SELECT 
  category,
  CASE 
    WHEN customer_age < 25 THEN 'Young'
    WHEN customer_age < 35 THEN 'Adult'
    ELSE 'Senior'
  END as age_group,
  customer_gender,
  COUNT(*) as order_count,
  SUM(quantity * price) as total_revenue,
  AVG(quantity * price) as avg_order_value,
  STDDEV(quantity * price) as revenue_stddev,
  MIN(order_date) as first_order,
  MAX(order_date) as last_order
FROM orders
GROUP BY CUBE(category, 
              CASE WHEN customer_age < 25 THEN 'Young'
                   WHEN customer_age < 35 THEN 'Adult'
                   ELSE 'Senior' END,
              customer_gender)
ORDER BY category, age_group, customer_gender;

-- 2. Advanced Window Functions for Ranking and Analytics
WITH order_analytics AS (
  SELECT 
    order_id,
    customer_id,
    product_id,
    quantity * price as order_value,
    order_date,
    -- Customer-level rankings
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as customer_order_rank,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY quantity * price DESC) as customer_value_rank,
    -- Product-level analytics
    COUNT(*) OVER (PARTITION BY product_id) as product_total_orders,
    AVG(quantity * price) OVER (PARTITION BY product_id) as product_avg_value,
    -- Time-based analytics
    LAG(quantity * price) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_value,
    LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as next_order_date
  FROM orders
)
SELECT 
  customer_id,
  COUNT(*) as total_orders,
  SUM(order_value) as total_spent,
  AVG(order_value) as avg_order_value,
  MAX(CASE WHEN customer_order_rank = 1 THEN order_value END) as latest_order_value,
  MAX(CASE WHEN customer_value_rank = 1 THEN order_value END) as highest_order_value,
  DATEDIFF(MAX(order_date), MIN(order_date)) as customer_lifetime_days
FROM order_analytics
GROUP BY customer_id
HAVING COUNT(*) > 1  -- Only customers with multiple orders
ORDER BY total_spent DESC;

-- 3. Cohort Analysis
WITH first_orders AS (
  SELECT 
    customer_id,
    MIN(order_date) as first_order_date,
    DATE_FORMAT(MIN(order_date), 'yyyy-MM') as cohort_month
  FROM orders
  GROUP BY customer_id
),
order_periods AS (
  SELECT 
    o.customer_id,
    fo.cohort_month,
    DATE_FORMAT(o.order_date, 'yyyy-MM') as order_month,
    MONTHS_BETWEEN(o.order_date, fo.first_order_date) as period_number,
    SUM(o.quantity * o.price) as revenue
  FROM orders o
  JOIN first_orders fo ON o.customer_id = fo.customer_id
  GROUP BY o.customer_id, fo.cohort_month, DATE_FORMAT(o.order_date, 'yyyy-MM')
)
SELECT 
  cohort_month,
  period_number,
  COUNT(DISTINCT customer_id) as customers,
  SUM(revenue) as total_revenue,
  AVG(revenue) as avg_revenue_per_customer
FROM order_periods
WHERE period_number <= 12  -- First 12 months
GROUP BY cohort_month, period_number
ORDER BY cohort_month, period_number;

-- 4. Machine Learning Feature Engineering
SELECT 
  customer_id,
  -- Recency features
  DATEDIFF(CURRENT_DATE(), MAX(order_date)) as days_since_last_order,
  
  -- Frequency features
  COUNT(*) as total_orders,
  COUNT(*) / DATEDIFF(MAX(order_date), MIN(order_date)) * 365 as orders_per_year,
  
  -- Monetary features
  SUM(quantity * price) as total_spent,
  AVG(quantity * price) as avg_order_value,
  STDDEV(quantity * price) as order_value_stddev,
  MAX(quantity * price) as max_order_value,
  
  -- Product diversity
  COUNT(DISTINCT product_id) as unique_products,
  COUNT(DISTINCT category) as unique_categories,
  
  -- Behavioral features
  AVG(quantity) as avg_quantity_per_order,
  COUNT(*) / COUNT(DISTINCT DATE_FORMAT(order_date, 'yyyy-MM')) as orders_per_active_month,
  
  -- Temporal features
  MONTH(MIN(order_date)) as first_order_month,
  COUNT(DISTINCT DATE_FORMAT(order_date, 'yyyy-MM')) as active_months
  
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 2;  -- At least 2 orders for meaningful features