from pyspark.sql import SparkSession
from app.scripts.enums.enum_tpch_tables import TablesTPCH
from app.scripts.enums.enum_tpch_scale_factor import ScaleFactorTPCH


class ConvertToParquet:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ConvertToParquet").getOrCreate()
        self.input_path = '/data/tpch'
        self.output_path = '/data/tpch/parquet'

    def convert_files(self, scale_factor: float):
        for table in TablesTPCH:
            print(f"Iniciando conversão da tabela {table} para parquet!")
            df = self.__read_file(table.value, scale_factor)
            self.__write_file(df, scale_factor)
            print(f"Finalizando conversão da tabela {table} para parquet!")

    def __read_file(self, table: str, scale_factor: float):
        input_file = f"{self.input_path}/sf{scale_factor}/{table}.tbl"
        df = self.spark.read.option("sep", "|").option("inferSchema", "true").csv(input_file)
        # Deletando ultima coluna que é nula
        df = df.drop(df.columns[-1])
        return df

    def __write_file(self, df, scale_factor: float):
        df.write.mode("overwrite").parquet(f"{self.output_path}/sf{scale_factor}")


if __name__ == "__main__":
    convert = ConvertToParquet()
    for scale in ScaleFactorTPCH:
            convert.convert_files(scale_factor=scale.value)
