-- Calculate the average cost of orders
WITH done AS (
              SELECT oi.order_id
                   , SUM(p.price) AS total_sum
              FROM order_items oi
              JOIN orders o ON oi.order_id = o.order_id
              LEFT JOIN products p on oi.product_id = p.product_id
              WHERE status = 'done'
              GROUP BY oi.order_id
             )
SELECT ROUND(AVG(total_sum), 1)
FROM done;

-- Find the category with the highest sales
SELECT p.category
     , COUNT(DISTINCT oi.order_id) AS order_count
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY 2 DESC;