import json
import os


class FileUtils:
    def ler_arquivo(self, caminho_arquivo: str):
        with open(caminho_arquivo) as f:
            content = f.read()
            return content

    def gravar_arquivo(self, nome_teste: str, scale_factor: float, tempo: int):
        output_dir = "/data/results"
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/{nome_teste}_sf{scale_factor}.json", "w") as f:
            json.dump({
                "engine": "tpchgen",
                "sf": scale_factor,
                "time": tempo
            }, f)
