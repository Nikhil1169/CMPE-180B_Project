WITH
  customer_spend AS (
    SELECT
      o.user_id,
      SUM(oi.sale_price) AS total_spend
    FROM
      `bigquery-public-data.thelook_ecommerce.orders`      AS o
    JOIN
      `bigquery-public-data.thelook_ecommerce.order_items` AS oi
      ON o.order_id = oi.order_id
    WHERE
      oi.status = 'Complete'
      AND DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    GROUP BY
      o.user_id
  ),
  spend_threshold AS (
    SELECT
      APPROX_QUANTILES(total_spend, 100)[OFFSET(90)] AS p90_spend
    FROM
      customer_spend
  ),
  high_value AS (
    SELECT
      cs.user_id,
      cs.total_spend
    FROM
      customer_spend AS cs,
      spend_threshold    AS st
    WHERE
      cs.total_spend >= st.p90_spend
  ),
  category_counts AS (
  
    SELECT
      hv.user_id,
      p.category,
      COUNT(*) AS cnt
    FROM
      high_value                                   AS hv
    JOIN
      `bigquery-public-data.thelook_ecommerce.orders`      AS o
      ON hv.user_id = o.user_id
    JOIN
      `bigquery-public-data.thelook_ecommerce.order_items` AS oi
      ON o.order_id = oi.order_id
    JOIN
      `bigquery-public-data.thelook_ecommerce.products`    AS p
      ON oi.product_id = p.id
    WHERE
      oi.status = 'Complete'
      AND DATE(oi.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    GROUP BY
      hv.user_id,
      p.category
  )
SELECT
  hv.user_id,
  hv.total_spend,
  ARRAY_AGG(cc.category
            ORDER BY cc.cnt DESC
            LIMIT 3
           ) AS top_categories
FROM
  category_counts AS cc
JOIN
  high_value AS hv
  ON cc.user_id = hv.user_id
GROUP BY
  hv.user_id,
  hv.total_spend
ORDER BY
  hv.total_spend DESC;


  