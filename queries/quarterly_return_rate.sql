WITH quarterly_data AS (
  SELECT
    FORMAT_DATE('%Y-Q%Q', DATE(order_items.created_at)) AS quarter,
    COUNT(order_items.id) AS total_orders, 
    COUNT(order_items.returned_at) AS total_returns 
  FROM
    `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
  GROUP BY quarter
)
SELECT
  quarter,
  total_orders,
  total_returns,
  ROUND(
    (total_returns * 100.0 / total_orders), 
    2
  ) AS return_rate
FROM quarterly_data
ORDER BY quarter;