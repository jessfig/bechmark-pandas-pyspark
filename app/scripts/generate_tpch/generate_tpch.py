import duckdb
import os
from enum_tpch_tables import TablesTPCH


class TPCHGenerator:
    def __init__(self, output_dir:str, scale_factor:int):
        self.output_dir = output_dir
        self.scale_factor = scale_factor

    def grava_arquivos_parquet_tabelas_tpch(self):
        self.__verifica_se_diretorio_existe()
        con = duckdb.connect()
        con.execute(f"""CALL dbgen(sf={self.scale_factor});""")
        for table in TablesTPCH:
            con.execute(
                f"""
                    COPY {table.value}
                    TO '{self.output_dir}/{table.value}.parquet'
                    (FORMAT 'parquet');
                """
            )

    def __verifica_se_diretorio_existe(self):
        os.makedirs(self.output_dir, exist_ok=True)


if __name__ == "__main__":
    generator = TPCHGenerator(output_dir="/data/tpch", scale_factor=1)
    generator.grava_arquivos_parquet_tabelas_tpch()
