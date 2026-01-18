from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from utils.spark_utils import SparkUtils
from utils.logger_utils import setup_logger
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from enums.enum_queries import Queries


class PysparkBenckmarck:
    def __init__(self, spark_utils):
        self.log = setup_logger(self.__class__.__name__)
        self.time_utils = TimeUtils()
        self.timestamp = self.time_utils.timestamp()
        self.file_utils = FileUtils()
        self.spark_utils = spark_utils

    def run(self, scale_factor: float):
        self.log.info(f"Spark - Iniciando leitura das tabelas, scale factor {scale_factor}!")
        self.__ler_tabelas_tpch(scale_factor)

        for query in Queries:
            self.log.info(f"Spark - Iniciando benchmark, scale factor {scale_factor}, query {query.value}!")
            self.time_utils.inicio_contador_tempo()

            df = self.__run_spark_sql_query(query.value)
            df.foreachPartition(lambda _: None) # forçando execução query

            self.time_utils.fim_contador_tempo()
            tempo_processamento = self.time_utils.tempo_processamento_segundos()

            self.file_utils.gravar_arquivo(
                path_name = f'/data/results/spark/{self.timestamp}/sf{scale_factor}/',
                query = query.value,
                scale_factor=scale_factor,
                time=tempo_processamento
            )
            self.log.info(
                f"Spark - Finalizando benchmark, "
                f"scale factor {scale_factor}, "
                f"query {query.value}, "
                f"tempo: {tempo_processamento}!"
            )

    def __ler_tabelas_tpch(self, scale_factor: float):
        for table in TablesTPCH:
            path = f'/data/tpch_parquet/sf{scale_factor}/{table.value}'
            df = self.spark_utils.read_parquet_file(path)
            df.createOrReplaceTempView(table.value)

    def __run_spark_sql_query(self, query):
        path_sql = f'queries/{query}.sql'
        query = self.file_utils.ler_arquivo(path_sql)
        df = self.spark_utils.get_spark_session().sql(query)
        return df


if __name__ == "__main__":
    spark_utils = SparkUtils()
    pyspark_benchmark = PysparkBenckmarck(spark_utils)
    pyspark_benchmark.run(scale_factor=ScaleFactorTPCH.DEZ_MB.value) # Warm Up
    for scale in ScaleFactorTPCH:
        pyspark_benchmark.run(scale_factor=scale.value)
    spark_utils.limpa_cache_spark()
    spark_utils.stop_spark_session()
