SELECT c.c_nationkey, COUNT(*)
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
WHERE o.o_orderdate >= '1996-01-01'
GROUP BY c.c_nationkey