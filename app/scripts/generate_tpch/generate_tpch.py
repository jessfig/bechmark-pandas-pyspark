import duckdb
import os
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH


class TPCHGenerator:
    def __init__(self):
        self.connection = duckdb.connect()
        self.output_dir = '/data/tpch/'

    def gera_arquivos_parquet_tpch(self, scale_factor: float):
        os.makedirs(self.output_dir, exist_ok=True)
        self.connection.execute(f"""CALL dbgen(sf={scale_factor});""")
        for table in TablesTPCH:
            self.connection.execute(
                f"""
                    COPY {table.value}
                    TO '{self.output_dir}{table.value}_sf{scale_factor}.parquet'
                    (FORMAT 'parquet');
                """
            )
        print(f'Arquivos gerados com sucesso - Scale Factor {scale_factor}!')


if __name__ == "__main__":
    generator = TPCHGenerator()
    for scale in ScaleFactorTPCH:
        generator.gera_arquivos_parquet_tpch(scale_factor=scale.value)
