class FileUtils:
    def ler_arquivo(self, caminho_arquivo):
        with open(caminho_arquivo) as f:
            content = f.read()
            return content
