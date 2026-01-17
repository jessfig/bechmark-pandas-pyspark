from pyspark.sql import SparkSession


class SparkUtils:
    def read_csv_file(self, spark, path, schema):
        df = spark.read.option("delimiter", "|").option("dateFormat", "yyyy-MM-dd").schema(schema).csv(path)
        return df

    def read_parquet_file(self, spark, path):
        df = spark.read.parquet(path)
        return df

    def write_parquet_file(self, df, path):
        df.write.mode("overwrite").parquet(path)

    def get_spark_session(self):
        return (
            SparkSession.builder
            .appName("tpch-benchmark")
            .master("local[8]")
            # CPU
            .config("spark.executor.cores", "8")
            # Memory
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            # Shuffle
            .config("spark.sql.shuffle.partitions", "8")
            # Benchmark friendly
            .config("spark.sql.adaptive.enabled", "false")
            # GC - Garbage collector
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
            .getOrCreate()
        )
