-- Validation queries run after each layer.
-- Each query should return 0 rows (or the expected count noted above it).

-- 1. Bronze vs silver vs quarantine: every bronze customer is accounted for
--    (bronze rows - duplicates) = silver rows + quarantined rows
SELECT
  (SELECT COUNT(*) FROM delta.`/Volumes/ecommerce/bronze/bronzevolume/customers/data`) AS bronze_rows,
  (SELECT COUNT(*) FROM ecommerce.silver.silver_customers)                             AS silver_rows,
  (SELECT COUNT(*) FROM ecommerce.silver.quarantine_customers)                         AS quarantined_rows;

-- 2. No duplicate business keys in silver
SELECT customer_id, COUNT(*) AS n
FROM ecommerce.silver.silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 3. Referential integrity: every fact row has a matching date
SELECT r.*
FROM ecommerce.gold.fact_returns r
LEFT ANTI JOIN ecommerce.gold.dim_date d ON r.date_sk = d.date_sk;

SELECT s.*
FROM ecommerce.gold.fact_sales s
LEFT ANTI JOIN ecommerce.gold.dim_date d ON s.date_sk = d.date_sk;

-- 4. No orders from the future (injected issue must be filtered out in silver)
SELECT *
FROM ecommerce.gold.fact_sales
WHERE order_timestamp > current_timestamp();

-- 5. Seasonality sanity check: November should be the peak, February the trough
SELECT month(order_timestamp) AS month,
       COUNT(DISTINCT order_id) AS orders,
       ROUND(SUM(net_revenue), 2) AS revenue
FROM ecommerce.gold.fact_sales
WHERE year(order_timestamp) = 2025
GROUP BY month(order_timestamp)
ORDER BY month;

-- 6. Batch and stream orders both reach silver; streamed lines all have an order
SELECT source_system, COUNT(*) AS orders
FROM ecommerce.silver.silver_orders
GROUP BY source_system;

SELECT i.order_id
FROM ecommerce.silver.silver_order_items i
LEFT ANTI JOIN delta.`/Volumes/ecommerce/bronze/bronzevolume/orders_stream/data` s
  ON i.order_id = s.order_id
WHERE i.source_system = 'stream';
