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

            # CPU
            .master("local[8]")
            .config("spark.executor.cores", "8")

            # Memória
            .config("spark.executor.memory", "14g")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memoryOverhead", "2g")

            # Shuffle / SQL
            .config("spark.sql.shuffle.partitions", "24")
            .config("spark.sql.adaptive.enabled", "false")  # benchmark estável
            .config("spark.sql.broadcastTimeout", "1200")

            # GC - Garbage collector (estável para workload grande)
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

            # Debug / estabilidade
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.driver.maxResultSize", "2g")
            .getOrCreate()
        )
