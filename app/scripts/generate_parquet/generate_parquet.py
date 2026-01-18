from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from utils.spark_utils import SparkUtils
from utils.logger_utils import setup_logger
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from schema_files import SchemaFiles


class ConvertToParquet:
    def __init__(self):
        self.log = setup_logger(self.__class__.__name__)
        self.time_utils = TimeUtils()
        self.spark_utils = SparkUtils()
        self.spark = self.spark_utils.get_spark_session()
        self.schemas = SchemaFiles()
        self.input_path = '/data/tpch'
        self.output_path = '/data/tpch_parquet'

    def convert_files(self, scale_factor: float):
        for table in TablesTPCH:
            self.log.info(f"Parquet - Iniciando conversão da tabela {table.value}!")
            self.time_utils.inicio_contador_tempo()

            input_path = f"{self.input_path}/sf{scale_factor}/{table.value}.tbl"
            schema = self.__get_schema(table.value)
            df = self.spark_utils.read_csv_file(self.spark, input_path, schema)

            output_path = f"{self.output_path}/sf{scale_factor}/{table.value}"
            self.spark_utils.write_parquet_file(df, output_path)

            self.time_utils.fim_contador_tempo()
            self.log.info(
                f'Parquet - Finalizando a conversão da tabela: {table.value}, '
                f'scale factor: {scale_factor}, '
                f'tempo: {self.time_utils.tempo_processamento_segundos()}!'
            )

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
