import pandas as pd
import duckdb
import gc
from utils.time_utils import TimeUtils
from utils.file_utils import FileUtils
from utils.logger_utils import setup_logger
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH
from enums.enum_queries import Queries


class PandasBenchmark:
    def __init__(self):
        self.log = setup_logger(self.__class__.__name__)
        self.time_utils = TimeUtils()
        self.timestamp = self.time_utils.timestamp()
        self.file_utils = FileUtils()
        self.con = duckdb.connect(database=":memory:")
        self.tables = {}

    def run(self, scale_factor: float):
        self.log.info(f"Pandas - Iniciando leitura das tabelas, scale factor {scale_factor}!")
        self.__load_tables(scale_factor)

        for query in Queries:
            self.log.info(f"Pandas - Iniciando benchmark, scale factor {scale_factor}, query {query.value}!")
            self.time_utils.inicio_contador_tempo()

            df = self.__run_sql_query(query.value)
            for _ in df.itertuples(index=False):
                pass # força execução da query

            self.time_utils.fim_contador_tempo()
            tempo_processamento = self.time_utils.tempo_processamento_segundos()
            self.file_utils.gravar_arquivo(
                path_name=f'/data/results/pandas/{self.timestamp}/sf{scale_factor}/',
                query=query.value,
                scale_factor=scale_factor,
                time=tempo_processamento
            )
            self.log.info(
                f"Pandas - Finalizando benchmark, "
                f"scale factor {scale_factor}, "
                f"query {query.value}, "
                f"tempo: {tempo_processamento}!"
            )
            self.__limpar_cache(df)

    def __load_tables(self, scale_factor: float):
        self.tables = {}
        for table in TablesTPCH:
            path = f"/data/tpch_parquet/sf{scale_factor}/{table.value}"
            df = pd.read_parquet(path, engine="pyarrow")
            self.tables[table.value] = df
            self.con.register(table.value, df)

    def __run_sql_query(self, query: str):
        path = f"queries/{query}.sql"
        query_sql = self.file_utils.ler_arquivo(path)
        return self.con.execute(query_sql).df()

    def __limpar_cache(self, df):
        del df
        gc.collect()

    def fechar_conexao(self):
        self.con.close()

if __name__ == "__main__":
    pandas_benchmark = PandasBenchmark()
    pandas_benchmark.run(scale_factor=ScaleFactorTPCH.DEZ_MB.value) # Warm-up
    for scale in ScaleFactorTPCH:
        pandas_benchmark.run(scale_factor=scale.value)
    pandas_benchmark.fechar_conexao()
