WITH inventory_status AS (
  SELECT
    product_category AS category,
    COUNTIF(sold_at IS NULL) AS unsold_inventory,
    COUNTIF(sold_at IS NOT NULL) AS sold_inventory
  FROM
    `bigquery-public-data.thelook_ecommerce.inventory_items`
  GROUP BY
    category
),
sales_status AS (
  SELECT
    p.category,
    COUNT(oi.id) AS total_units_sold,
    COALESCE(SUM(oi.sale_price), 0) AS total_sales
  FROM
    `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN
    `bigquery-public-data.thelook_ecommerce.products` p
      ON oi.product_id = p.id
  WHERE
    oi.status = 'Complete'
  GROUP BY
    p.category
)
SELECT
  inv.category,
  inv.unsold_inventory,
  inv.sold_inventory,
  COALESCE(s.total_units_sold, 0) AS total_units_sold,
  COALESCE(s.total_sales, 0) AS total_sales
FROM
  inventory_status inv
LEFT JOIN
  sales_status s
    ON inv.category = s.category
WHERE
  COALESCE(inv.unsold_inventory, 0) > 0
ORDER BY
  inv.unsold_inventory DESC,
  s.total_units_sold ASC
LIMIT 50;
