SELECT
  products.category AS category,
  SUM(order_items.sale_price) AS total_loss 
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
JOIN
  `bigquery-public-data.thelook_ecommerce.products` AS products
  ON order_items.product_id = products.id 
WHERE
  order_items.returned_at IS NOT NULL 
GROUP BY category
ORDER BY total_loss DESC;