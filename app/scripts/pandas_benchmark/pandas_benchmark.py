import pandas as pd
from pandasql import sqldf
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from enums.enum_queries import Queries


class PandasBenchmark:

    def __init__(self):
        self.time_utils = TimeUtils()
        self.timestamp = self.time_utils.timestamp()
        self.file_utils = FileUtils()
        self.tables = {}

    def run(self, scale_factor: float):
        self.__load_tables(scale_factor)

        for query in Queries:
            self.time_utils.inicio_contador_tempo()
            df = self.__run_sql_query(query.value)
            _ = df.values  # forçando execução da query
            self.time_utils.fim_contador_tempo()

            self.file_utils.gravar_arquivo(
                path_name = f'/data/results/pandas/{self.timestamp}/sf{scale_factor}/',
                query = query.value,
                scale_factor = scale_factor,
                time = self.time_utils.tempo_processamento_segundos()
            )

    def __load_tables(self, scale_factor: float):
        self.tables = {}
        for table in TablesTPCH:
            path = f"/data/tpch_parquet/sf{scale_factor}/{table.value}"
            self.tables[table.value] = pd.read_parquet(
                path,
                engine="pyarrow"
            )

    def __run_sql_query(self, query: str) -> str:
        path = f"queries/{query}.sql"
        query_sql = self.file_utils.ler_arquivo(path)
        df = sqldf(query_sql, self.tables)
        return df


if __name__ == "__main__":
    benchmark = PandasBenchmark()
    for scale in ScaleFactorTPCH:
        benchmark.run(scale.value)
