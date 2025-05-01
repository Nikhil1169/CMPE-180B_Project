WITH first_orders AS (
  SELECT
    user_id,
    MIN(DATE(created_at)) AS first_order_date
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  GROUP BY user_id
)
SELECT
  COUNT(*)                           AS new_customers_acquired,
  500                                AS campaign_cost_usd,    
  ROUND(500.0 / COUNT(*), 2)         AS cac_usd
FROM first_orders
WHERE first_order_date
      BETWEEN '2022-08-01' AND '2022-08-31'