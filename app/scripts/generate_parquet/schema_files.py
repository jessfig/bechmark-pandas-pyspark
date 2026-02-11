from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType, DateType


class SchemaFiles:
    def get_customer_schema(self):
        customer_schema = StructType([
            StructField("c_custkey", LongType(), False),
            StructField("c_name", StringType(), False),
            StructField("c_address", StringType(), False),
            StructField("c_nationkey", LongType(), False),
            StructField("c_phone", StringType(), False),
            StructField("c_acctbal", DoubleType(), False),
            StructField("c_mktsegment", StringType(), False),
            StructField("c_comment", StringType(), False)
        ])
        return customer_schema

    def get_orders_schema(self):
        orders_schema = StructType([
            StructField("o_orderkey", LongType(), False),
            StructField("o_custkey", LongType(), False),
            StructField("o_orderstatus", StringType(), False),
            StructField("o_totalprice", DoubleType(), False),
            StructField("o_orderdate", DateType(), False),
            StructField("o_orderpriority", StringType(), False),
            StructField("o_clerk", StringType(), False),
            StructField("o_shippriority", IntegerType(), False),
            StructField("o_comment", StringType(), False)
        ])
        return orders_schema

    def get_nation_schema(self):
        nation_schema = StructType([
            StructField("n_nationkey", LongType(), False),
            StructField("n_name", StringType(), False),
            StructField("n_regionkey", LongType(), False),
            StructField("n_comment", StringType(), False)
        ])
        return nation_schema

    def get_lineitem_schema(self):
        lineitem_schema = StructType([
            StructField("l_orderkey", LongType()),
            StructField("l_partkey", LongType()),
            StructField("l_suppkey", LongType()),
            StructField("l_linenumber", IntegerType()),
            StructField("l_quantity", DoubleType()),
            StructField("l_extendedprice", DoubleType()),
            StructField("l_discount", DoubleType()),
            StructField("l_tax", DoubleType()),
            StructField("l_returnflag", StringType()),
            StructField("l_linestatus", StringType()),
            StructField("l_shipdate", DateType()),
            StructField("l_commitdate", DateType()),
            StructField("l_receiptdate", DateType()),
            StructField("l_shipinstruct", StringType()),
            StructField("l_shipmode", StringType()),
            StructField("l_comment", StringType())
        ])
        return lineitem_schema