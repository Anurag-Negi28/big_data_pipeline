-- BigQuery Examples for Distributed Data Processing
-- These queries demonstrate BigQuery's capabilities for big data analytics

-- 1. Customer Segmentation Analysis
WITH customer_metrics AS (
  SELECT 
    customer_id,
    customer_age,
    customer_gender,
    COUNT(order_id) as total_orders,
    SUM(quantity * price) as total_spent,
    AVG(quantity * price) as avg_order_value,
    MAX(order_date) as last_order_date,
    COUNT(DISTINCT product_id) as unique_products_purchased
  FROM `project.dataset.orders`
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY customer_id, customer_age, customer_gender
),
customer_segments AS (
  SELECT *,
    CASE 
      WHEN total_spent > 1000 THEN 'High Value'
      WHEN total_spent > 500 THEN 'Medium Value'
      ELSE 'Low Value'
    END as customer_segment,
    NTILE(4) OVER (ORDER BY total_spent DESC) as spending_quartile
  FROM customer_metrics
)
SELECT 
  customer_segment,
  COUNT(*) as customer_count,
  AVG(total_spent) as avg_customer_value,
  AVG(total_orders) as avg_orders_per_customer
FROM customer_segments
GROUP BY customer_segment
ORDER BY avg_customer_value DESC;

-- 2. Time Series Analysis with Window Functions
SELECT 
  order_date,
  COUNT(*) as daily_orders,
  SUM(quantity * price) as daily_revenue,
  AVG(SUM(quantity * price)) OVER (
    ORDER BY order_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as revenue_7day_ma,
  LAG(SUM(quantity * price), 7) OVER (ORDER BY order_date) as revenue_prev_week,
  ((SUM(quantity * price) - LAG(SUM(quantity * price), 7) OVER (ORDER BY order_date)) 
   / LAG(SUM(quantity * price), 7) OVER (ORDER BY order_date)) * 100 as wow_growth_pct
FROM `project.dataset.orders`
GROUP BY order_date
ORDER BY order_date;

-- 3. Product Performance Analysis
SELECT 
  category,
  product_name,
  COUNT(*) as total_orders,
  SUM(quantity) as total_quantity_sold,
  SUM(quantity * price) as total_revenue,
  AVG(price) as avg_price,
  COUNT(DISTINCT customer_id) as unique_customers,
  RANK() OVER (PARTITION BY category ORDER BY SUM(quantity * price) DESC) as category_rank,
  PERCENT_RANK() OVER (ORDER BY SUM(quantity * price) DESC) as overall_percentile
FROM `project.dataset.orders`
GROUP BY category, product_name
HAVING COUNT(*) >= 5  -- Filter for products with at least 5 orders
ORDER BY total_revenue DESC;

-- 4. Geographic Analysis (if location data available)
SELECT 
  customer_region,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(order_id) as total_orders,
  SUM(quantity * price) as total_revenue,
  AVG(quantity * price) as avg_order_value,
  -- Calculate market share
  SUM(quantity * price) / SUM(SUM(quantity * price)) OVER() * 100 as market_share_pct
FROM `project.dataset.orders` o
JOIN `project.dataset.customers` c ON o.customer_id = c.customer_id
GROUP BY customer_region
ORDER BY total_revenue DESC;