SELECT l_partkey, SUM(l_quantity) AS total_qty
FROM lineitem
GROUP BY l_partkey
ORDER BY total_qty DESC
LIMIT 10