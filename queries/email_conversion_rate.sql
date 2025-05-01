WITH first_orders AS (
  SELECT
    user_id,
    MIN(DATE(created_at)) AS first_order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  GROUP BY user_id
)
SELECT
  ROUND(
    100 * SAFE_DIVIDE(
      COUNT(*),    
      10000   
    ), 
    2
  ) AS conversion_rate_percent
FROM first_orders
WHERE first_order_date
      BETWEEN '2022-08-01' AND '2022-08-31';