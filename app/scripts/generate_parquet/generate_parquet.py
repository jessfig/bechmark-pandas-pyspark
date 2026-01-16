from pathlib import Path
from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from schema_files import SchemaFiles


class ConvertToParquet:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.spark = SparkSession.builder.appName("ConvertToParquet").getOrCreate()
        self.schemas = SchemaFiles()
        self.input_path = '/data/tpch'
        self.output_path = '/data/tpch/tpch_parquet'

    def convert_files(self, scale_factor: float):
        for table in TablesTPCH:
            self.time_utils.inicio_contador_tempo()
            print(f"Iniciando conversão da tabela {table.value} para parquet!")
            df = self.__read_file(table.value, scale_factor)
            self.__write_file(df, scale_factor)
            self.time_utils.fim_contador_tempo()
            print(
                f'Finalizando a conversão da tabela: {table.value}, scale factor: {scale_factor} para parquet, '
                f'tempo de processamento em segundos: {self.time_utils.tempo_processamento_segundos()}!'
            )

    def __read_file(self, table: str, scale_factor: float):
        input_file = f"{self.input_path}/sf{scale_factor}/{table}.tbl"
        schema = self.__get_schema(table)
        df = self.spark.read.option("delimiter", "|").option("dateFormat", "yyyy-MM-dd").schema(schema).csv(input_file)
        return df

    def __write_file(self, df, scale_factor: float):
        path = f"{self.output_path}/sf{scale_factor}"
        output_path = Path(path)
        output_path.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(path)

    def __get_schema(self, table:str):
        strategies = {
            TablesTPCH.ITENS_PEDIDOS.value: self.schemas.get_lineitem_schema,
            TablesTPCH.CLIENTES.value: self.schemas.get_customer_schema,
            TablesTPCH.PAISES.value: self.schemas.get_nation_schema,
            TablesTPCH.PEDIDOS.value: self.schemas.get_orders_schema
        }
        strategie = strategies.get(table)
        return strategie()


if __name__ == "__main__":
    convert = ConvertToParquet()
    for scale in ScaleFactorTPCH:
            convert.convert_files(scale_factor=scale.value)
