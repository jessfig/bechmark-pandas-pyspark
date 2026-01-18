import os
import subprocess
from utils.time_utils import TimeUtils
from utils.logger_utils import setup_logger
from enums.enum_tpch_scale_factor import ScaleFactorTPCH


class TPCHGenerator:
    def __init__(self):
        self.log = setup_logger(self.__class__.__name__)
        self.time_utils = TimeUtils()

    def gera_arquivos_parquet_tpch(self, scale_factor: float):
        self.log.info(f'TPCH - Iniciando a geração das tabelas, scale factor: {scale_factor}!')
        self.time_utils.inicio_contador_tempo()
        output_dir = self.__obter_nome_diretorio_gravacao(scale_factor)
        cmd = [
            "/tpch/tpch-dbgen/dbgen",
            "-s", str(scale_factor),
            "-b", "/tpch/tpch-dbgen/dists.dss",
            "-f"
        ]
        subprocess.run(
            cmd,
            cwd=output_dir,
            check=True
        )
        self.time_utils.fim_contador_tempo()
        self.log.info(
            f'TPCH - Finalizando a geração das tabelas, '
            f'scale factor: {scale_factor}, '
            f'tempo: {self.time_utils.tempo_processamento_segundos()}!'
        )

    def __obter_nome_diretorio_gravacao(self, scale_factor: float):
        output_dir = f"/data/tpch/sf{scale_factor}"
        os.makedirs(output_dir, exist_ok=True)
        return output_dir


if __name__ == "__main__":
    generator = TPCHGenerator()
    for scale in ScaleFactorTPCH:
            generator.gera_arquivos_parquet_tpch(scale_factor=scale.value)
