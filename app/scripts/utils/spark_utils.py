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

            # CPU
            .master("local[8]")
            .config("spark.executor.cores", "8")

            # Memória
            .config("spark.executor.memory", "16g")
            .config("spark.driver.memory", "16g")
            .config("spark.executor.memoryOverhead", "8g")

            # Shuffle / SQL
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.adaptive.enabled", "false")  # benchmark estável
            .config("spark.sql.broadcastTimeout", "1200")

            # GC - Garbage collector (estável para workload grande)
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
            .getOrCreate()
        )
