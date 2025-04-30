WITH user_metrics AS (
  SELECT
    u.id                         AS user_id,
    u.gender,
    u.state,                            
    COUNT(DISTINCT o.order_id)   AS total_orders,
    SUM(oi.sale_price)           AS total_spend,
    MAX(DATE(oi.created_at))     AS last_purchase_date
  FROM
    `bigquery-public-data.thelook_ecommerce.users`       AS u
  JOIN
    `bigquery-public-data.thelook_ecommerce.orders`      AS o
    ON u.id = o.user_id
  JOIN
    `bigquery-public-data.thelook_ecommerce.order_items` AS oi
    ON o.order_id = oi.order_id
  WHERE
    oi.status = 'Complete'
    AND DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY
    u.id, u.gender, u.state
),
quartiles AS (
  SELECT
    APPROX_QUANTILES(total_spend, 4)[OFFSET(1)] AS spend_q1,
    APPROX_QUANTILES(total_spend, 4)[OFFSET(2)] AS spend_q2,
    APPROX_QUANTILES(total_spend, 4)[OFFSET(3)] AS spend_q3,
    APPROX_QUANTILES(total_orders, 4)[OFFSET(1)] AS orders_q1,
    APPROX_QUANTILES(total_orders, 4)[OFFSET(2)] AS orders_q2,
    APPROX_QUANTILES(total_orders, 4)[OFFSET(3)] AS orders_q3
  FROM user_metrics
)
SELECT
  um.*,
  CASE
    WHEN um.total_spend  >= q.spend_q3  THEN 'High'
    WHEN um.total_spend  >= q.spend_q2  THEN 'Medium'
    ELSE 'Low'
  END AS spend_segment,
  CASE
    WHEN um.total_orders >= q.orders_q3 THEN 'Frequent'
    WHEN um.total_orders >= q.orders_q2 THEN 'Occasional'
    ELSE 'Infrequent'
  END AS frequency_segment,
  CASE
    WHEN um.last_purchase_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'Recent'
    WHEN um.last_purchase_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN 'Dormant'
    ELSE 'Churn Risk'
  END AS recency_segment
FROM
  user_metrics um
CROSS JOIN
  quartiles q
LIMIT 100;