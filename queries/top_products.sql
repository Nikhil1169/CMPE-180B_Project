WITH product_sales AS (
  SELECT
    p.category,
    p.id AS product_id,
    p.name AS product_name,
    COUNT(oi.id) AS total_quantity_sold,
    SUM(oi.sale_price) AS total_sales
  FROM
    `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN
    `bigquery-public-data.thelook_ecommerce.products` p
      ON oi.product_id = p.id
  WHERE
    oi.status = 'Complete'
  GROUP BY
    p.category, product_id, product_name
),
ranked_products AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_sales DESC) AS sales_rank
  FROM
    product_sales
)
SELECT
  category,
  product_id,
  product_name,
  total_quantity_sold,
  total_sales
FROM
  ranked_products
WHERE
  sales_rank <= 3  
ORDER BY
  category,
  sales_rank;
