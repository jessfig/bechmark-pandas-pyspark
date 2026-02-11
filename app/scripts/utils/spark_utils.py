from pyspark.sql import SparkSession


class SparkUtils:
    def __init__(self):
        self.__spark = self.__start_spark_session()

    def read_csv_file(self, path, schema):
        df = self.__spark.read.option("delimiter", "|").option("dateFormat", "yyyy-MM-dd").schema(schema).csv(path)
        return df

    def read_parquet_file(self, path):
        df = self.__spark.read.parquet(path)
        return df

    def write_parquet_file(self, df, path):
        df.write.mode("overwrite").parquet(path)

    def get_spark_session(self):
        return self.__spark

    def stop_spark_session(self):
        self.__spark.stop()

    def limpa_cache_spark(self):
        # Cache Spark
        self.__spark.catalog.clearCache()
        # Limpa memória JVM
        self.__spark.sparkContext._jsc.sc().env().blockManager().memoryStore().clear()

    def __start_spark_session(self):
        return (
            SparkSession.builder
            .appName("tpch-benchmark")

            # Local mode
            .master("local[8]")

            # Memória (container 16g)
            .config("spark.driver.memory", "26g")
            .config("spark.driver.memoryOverhead", "4g")

            # CPU / paralelismo
            .config("spark.default.parallelism", "8")

            # Shuffle / SQL
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.broadcastTimeout", "1200")

            # GC - Garbage collector (estável para workload grande)
            .config(
                "spark.driver.extraJavaOptions",
                "-XX:+UseG1GC "
                "-XX:InitiatingHeapOccupancyPercent=35 "
                "-XX:+ExplicitGCInvokesConcurrent"
            )

            # Evita cache acidental
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true")

            .getOrCreate()
        )
