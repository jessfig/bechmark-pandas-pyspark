import duckdb
import os
from enums.enum_tpch_tables import TablesTPCH
from enums.enum_tpch_scale_factor import ScaleFactorTPCH


class TPCHGenerator:
    def __init__(self):
        self.connection = duckdb.connect()

    def gera_arquivos_parquet_tpch(self, output_dir:str, scale_factor:float):
        os.makedirs(output_dir, exist_ok=True)
        self.connection.execute(f"""CALL dbgen(sf={scale_factor});""")
        for table in TablesTPCH:
            self.connection.execute(
                f"""
                    COPY {table.value}
                    TO '{output_dir}/{table.value}.parquet'
                    (FORMAT 'parquet');
                """
            )
        print('Arquivos gerados com sucesso!')


if __name__ == "__main__":
    generator = TPCHGenerator()
    generator.gera_arquivos_parquet_tpch(
        output_dir="/data/tpch",
        scale_factor=ScaleFactorTPCH.UM_GIGA.value
    )
