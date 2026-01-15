from pyspark.sql import SparkSession
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from enums.enum_tpch_tables import TablesTPCH


class PysparkBenckmarck:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.spark = SparkSession.builder.appName("tpch").getOrCreate()
        self.file_utils = FileUtils()

    def run_benchmark(self):
        self.time_utils.inicio_contador_tempo()
        self.__ler_tabelas_tpch()
        self.__run_spark_sql_querie('groupby')
        self.__run_spark_sql_querie('groupby_filter')
        self.__run_spark_sql_querie('groupby_sortby_limit')
        self.__run_spark_sql_querie('join')
        self.__run_spark_sql_querie('join_three_tables')
        self.__run_spark_sql_querie('scan_filter')
        self.__run_spark_sql_querie('window_function')
        self.time_utils.fim_contador_tempo()
        tempo = self.time_utils.tempo_processamento_segundos()
        print(f'Tempo de processamento em segundos: {tempo}')

    def __ler_tabelas_tpch(self):
        for table in TablesTPCH:
            path = f'data/{table.value}.parquet'
            df = self.__read_table(path)
            df.createOrReplaceTempView(table.value)

    def __read_table(self, path: str):
        df = self.spark.read.parquet(path)
        return df

    def __run_spark_sql_querie(self, querie):
        path_sql = f'queries/{querie}.sql'
        querie = self.file_utils.ler_arquivo(path_sql)
        self.spark.sql(querie)


if __name__ == "__main__":
    pyspark_benchmark = PysparkBenckmarck()
    pyspark_benchmark.run_benchmark()
