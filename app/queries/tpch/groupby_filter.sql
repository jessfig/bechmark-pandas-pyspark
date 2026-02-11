SELECT l_suppkey, SUM(l_extendedprice)
FROM lineitem
WHERE l_discount < 0.05
GROUP BY l_suppkey