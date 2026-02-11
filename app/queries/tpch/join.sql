SELECT o.o_orderkey, o.o_orderdate, c.c_name
FROM orders o
JOIN customer c
  ON o.o_custkey = c.c_custkey