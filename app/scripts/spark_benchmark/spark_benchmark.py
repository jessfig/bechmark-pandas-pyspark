import os
from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH


class PysparkBenckmarck:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.spark = SparkSession.builder.appName("tpch").getOrCreate()
        self.file_utils = FileUtils()

    def run_benchmark(self, scale_factor: float):
        self.time_utils.inicio_contador_tempo()
        self.__ler_tabelas_tpch(scale_factor)
        self.__run_spark_sql_querie('groupby')
        self.__run_spark_sql_querie('groupby_filter')
        self.__run_spark_sql_querie('groupby_sortby_limit')
        self.__run_spark_sql_querie('join')
        self.__run_spark_sql_querie('join_three_tables')
        self.__run_spark_sql_querie('scan_filter')
        self.__run_spark_sql_querie('window_function')
        self.time_utils.fim_contador_tempo()
        self.__gravar_resultados(scale_factor)

    def __ler_tabelas_tpch(self, scale_factor: float):
        for table in TablesTPCH:
            path = f'data/tpch/tpch_parquet/sf{scale_factor}/{table}.parquet'
            df = self.__read_table(path)
            df.createOrReplaceTempView(table.value)

    def __read_table(self, path: str):
        df = self.spark.read.parquet(path)
        return df

    def __run_spark_sql_querie(self, querie):
        path_sql = f'queries/{querie}.sql'
        querie = self.file_utils.ler_arquivo(path_sql)
        self.spark.sql(querie)

    def __gravar_resultados(self, scale_factor: float):
        self.file_utils.gravar_arquivo(
            nome_teste = 'spark_tpch',
            scale_factor = scale_factor,
            tempo = self.time_utils.tempo_processamento_segundos()
        )


if __name__ == "__main__":
    pyspark_benchmark = PysparkBenckmarck()
    for scale in ScaleFactorTPCH:
        pyspark_benchmark.run_benchmark(scale_factor=scale.value)
