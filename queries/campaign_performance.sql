SELECT
  CASE
    WHEN DATE(created_at) BETWEEN '2022-07-01' AND '2022-07-31' THEN 'Before Campaign'
    WHEN DATE(created_at) BETWEEN '2022-08-01' AND '2022-08-31' THEN 'After Campaign'
  END AS campaign_period,
  COUNT(*)                           AS total_orders,
  SUM(num_of_item)                   AS total_items_sold,
  COUNT(DISTINCT user_id)            AS unique_customers,
  ROUND(SUM(num_of_item) * 20, 2)    AS estimated_revenue_usd
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) BETWEEN '2022-07-01' AND '2022-08-31'
GROUP BY campaign_period
ORDER BY campaign_period;