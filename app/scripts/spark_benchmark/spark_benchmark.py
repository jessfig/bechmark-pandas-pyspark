from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from utils.spark_utils import SparkUtils
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from enums.enum_queries import Queries


class PysparkBenckmarck:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.spark = SparkSession.builder.appName("tpch").getOrCreate()
        self.spark_utils = SparkUtils(self.spark)
        self.file_utils = FileUtils()

    def run_benchmark(self, scale_factor: float):
        self.time_utils.inicio_contador_tempo()
        self.__ler_tabelas_tpch(scale_factor)

        for querie in Queries:
            df = self.__run_spark_sql_querie(querie.value)
            path = f'/data/results_parquet/sf{scale_factor}_{querie}'
            self.spark_utils.write_parquet_file(df, path)

        self.time_utils.fim_contador_tempo()
        self.file_utils.gravar_arquivo(
            nome_teste='spark_tpch',
            scale_factor=scale_factor,
            tempo=self.time_utils.tempo_processamento_segundos()
        )

    def __ler_tabelas_tpch(self, scale_factor: float):
        for table in TablesTPCH:
            path = f'/data/tpch_parquet/sf{scale_factor}_{table.value}'
            df = self.spark_utils.read_parquet_file(path)
            df.createOrReplaceTempView(table.value)

    def __run_spark_sql_querie(self, querie):
        path_sql = f'queries/{querie}.sql'
        querie = self.file_utils.ler_arquivo(path_sql)
        df = self.spark.sql(querie)
        return df


if __name__ == "__main__":
    pyspark_benchmark = PysparkBenckmarck()
    for scale in ScaleFactorTPCH:
        pyspark_benchmark.run_benchmark(scale_factor=scale.value)
