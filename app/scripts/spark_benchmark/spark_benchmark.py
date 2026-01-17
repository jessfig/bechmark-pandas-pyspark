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
        self.timestamp = self.time_utils.timestamp()
        self.spark_utils = SparkUtils()
        self.spark = self.spark_utils.get_spark_session()
        self.file_utils = FileUtils()

    def run_benchmark(self, scale_factor: float):
        self.__ler_tabelas_tpch(scale_factor)

        for query in Queries:
            self.time_utils.inicio_contador_tempo()
            df = self.__run_spark_sql_query(query.value)
            df.foreachPartition(lambda _: None) # forçando execução query
            self.time_utils.fim_contador_tempo()

            self.file_utils.gravar_arquivo(
                path_name = f'/data/results/spark/{self.timestamp}/sf{scale_factor}/',
                query = query.value,
                scale_factor=scale_factor,
                time=self.time_utils.tempo_processamento_segundos()
            )

    def __ler_tabelas_tpch(self, scale_factor: float):
        for table in TablesTPCH:
            path = f'/data/tpch_parquet/sf{scale_factor}/{table.value}'
            df = self.spark_utils.read_parquet_file(self.spark, path)
            df.createOrReplaceTempView(table.value)

    def __run_spark_sql_query(self, query):
        path_sql = f'queries/{query}.sql'
        query = self.file_utils.ler_arquivo(path_sql)
        df = self.spark.sql(query)
        return df


if __name__ == "__main__":
    pyspark_benchmark = PysparkBenckmarck()
    # Warm Up
    pyspark_benchmark.run_benchmark(scale_factor=ScaleFactorTPCH.DEZ_MB.value)

    for scale in ScaleFactorTPCH:
        pyspark_benchmark.run_benchmark(scale_factor=scale.value)
