SELECT
  products.category AS category,
  products.id AS product_id,
  products.name AS product_name,
  COUNT(order_items.returned_at) AS total_returns,
  ROUND( 
    (COUNT(order_items.returned_at) * 100.0 / COUNT(order_items.id)), 
    2
  ) AS return_rate
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
JOIN
  `bigquery-public-data.thelook_ecommerce.products` AS products
  ON order_items.product_id = products.id 
GROUP BY
  category, product_id, product_name
)

SELECT
  category,
  product_id,
  product_name,
  total_returns,
  return_rate
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER ( 
      PARTITION BY category 
      ORDER BY return_rate DESC
    ) AS rank
  FROM product_returns
) 
WHERE rank <= 3 
ORDER BY category, return_rate DESC;