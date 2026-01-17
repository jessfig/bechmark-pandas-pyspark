import json
import os


class FileUtils:
    def ler_arquivo(self, caminho_arquivo: str):
        with open(caminho_arquivo) as f:
            content = f.read()
            return content

    def gravar_arquivo(self, path_name: str, query: str, scale_factor: float, time: float):
        os.makedirs(path_name, exist_ok=True)
        with open(f"{path_name}/{query}.json", "w") as f:
            json.dump({
                "engine": "tpchgen",
                "query": query,
                "sf": scale_factor,
                "time": time
            }, f)
