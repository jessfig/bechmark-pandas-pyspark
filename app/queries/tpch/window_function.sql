SELECT *,
       RANK() OVER (PARTITION BY l_partkey ORDER BY l_extendedprice DESC) AS rnk
FROM lineitem