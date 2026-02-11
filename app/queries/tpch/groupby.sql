SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity)
FROM lineitem
GROUP BY l_returnflag, l_linestatus