SELECT
  FORMAT_DATE('%Y-Q%Q', DATE(oi.created_at)) AS order_quarter,
  p.name AS product_name,
  COUNT(oi.id) AS total_quantity_sold,
  SUM(oi.sale_price) AS total_sales
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN
  `bigquery-public-data.thelook_ecommerce.products` p
ON
  oi.product_id = p.id
WHERE
  oi.status = 'Complete'
GROUP BY
  order_quarter, product_name
ORDER BY
  order_quarter, total_sales DESC

