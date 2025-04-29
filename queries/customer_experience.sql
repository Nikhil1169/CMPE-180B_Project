SELECT
  e.user_id,
  COUNTIF(e.event_type = 'view_item')        AS views,
  COUNTIF(e.event_type = 'add_to_cart')      AS adds_to_cart,
  COUNTIF(e.event_type = 'remove_from_cart') AS removes_from_cart,
  COUNTIF(e.event_type = 'purchase')         AS purchases,
  SAFE_DIVIDE(
    COUNTIF(e.event_type = 'add_to_cart'),
    NULLIF(COUNTIF(e.event_type = 'view_item'), 0)
  )                                          AS add_to_cart_rate,
  SAFE_DIVIDE(
    COUNTIF(e.event_type = 'purchase'),
    NULLIF(COUNTIF(e.event_type = 'view_item'), 0)
  )                                          AS conversion_rate
FROM
  `bigquery-public-data.thelook_ecommerce.events` AS e
WHERE
  DATE(e.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
GROUP BY
  e.user_id
ORDER BY
  conversion_rate DESC
LIMIT
  100;