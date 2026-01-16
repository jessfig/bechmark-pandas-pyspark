class SparkUtils:
    def __init__(self, spark):
        self.spark = spark

    def read_csv_file(self, path, schema):
        df = self.spark.read.option("delimiter", "|").option("dateFormat", "yyyy-MM-dd").schema(schema).csv(path)
        return df

    def read_parquet_file(self, path):
        df = self.spark.read.parquet(path)
        return df

    def write_parquet_file(self, df, path):
        df.write.mode("overwrite").parquet(path)
