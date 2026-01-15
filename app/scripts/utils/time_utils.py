import time


class TimeUtils:
    def inicio_contador_tempo(self):
        self.__start = time.perf_counter()

    def fim_contador_tempo(self):
        self.__end = time.perf_counter()

    def tempo_processamento_segundos(self):
        elapsed_seconds = self.__end - self.__start
        return elapsed_seconds
